"""
Crop estimate for the dashboard.

Three views of the US upland crop, by state, combined into one estimate:
  1. Conditions + drought → yield and harvested share (pooled across states, history from 2001)
  2. FSA reported acreage → planted and failed acres (scaled to NASS from past releases)
  3. Classing pace → final bales from season-to-date bales classed and each state's normal pace
The state estimates are blended with weights that follow each view's historical error at that
point in the season, then summed to the US.
"""
import math
import numpy as np
import pandas as pd

import load

MIN_YEARS = 6


def _ols(X, y, w):
    W = np.sqrt(w)[:, None]
    beta, *_ = np.linalg.lstsq(X * W, y * W[:, 0], rcond=None)
    return beta


def _week_features(cond, dr, years, wk):
    """Per state-year features using only information available by ISO week `wk`."""
    c = cond[(cond.woy >= 26) & (cond.woy <= wk) & cond.year.isin(years)]
    f = c.groupby(["year", "state"]).agg(ci=("ci", "mean"), pvp=("pvp", "mean"), n=("ci", "size"))
    last = cond[(cond.woy <= wk) & (cond.woy >= wk - 2) & cond.year.isin(years)].sort_values("woy").groupby(["year", "state"]).ci.last()
    f["ci_now"] = last
    d = dr[(dr.woy >= 22) & (dr.woy <= min(wk, 44)) & dr.year.isin(years)].groupby(["year", "state"]).d2.mean()
    f["d2"] = d
    return f[f.n >= 3]


def condition_model(cond, dr, ann, cur, wk):
    yld = load.pick(ann, "upland_yield", final_only=True)
    pl = load.pick(ann, "upland_planted", final_only=True)
    hv = load.pick(ann, "upland_harvested", final_only=True)
    years = sorted(set(cond.year))
    F = _week_features(cond, dr, years, wk)
    F["d2"] = F["d2"].fillna(0.0)
    df = F.join(yld.rename("yield")).join(pl.rename("planted")).join(hv.rename("harv")).reset_index()
    df = df[df.state != "US"]
    # trend yield: average of the state's previous five finals (no look-ahead)
    yl = yld.reset_index().rename(columns={"value": "y"})
    tr = {}
    for s, g in yl[yl.state != "US"].groupby("state"):
        g = g.set_index("year").y.sort_index()
        for y in range(g.index.min() + 3, cur + 1):
            prev = g[(g.index < y) & (g.index >= y - 5)]
            if len(prev) >= 3:
                tr[(y, s)] = prev.mean()
    df["trend"] = [tr.get((y, s)) for y, s in zip(df.year, df.state)]
    df["hr"] = df.harv / df.planted
    df["ci_now"] = df.ci_now.fillna(df.ci)
    hist = df[(df.year < cur) & df["yield"].notna() & df.trend.notna() & df.planted.notna()].copy()
    if hist.year.nunique() < MIN_YEARS:
        return None
    hist["ly"] = np.log(hist["yield"] / hist.trend)
    hist = hist.dropna(subset=["ci", "ci_now", "d2", "pvp", "ly"])

    def fit(h):
        m = h.groupby("state")[["ci", "ci_now", "d2", "pvp", "hr"]].transform("mean")
        X = np.c_[np.ones(len(h)), h.ci_now - m.ci_now, h.pvp - m.pvp, h.d2 - m.d2]
        b_y = _ols(X, h.ly.values, h.planted.values)
        ok = h.hr.notna().values & m.hr.notna().values
        Xa = np.c_[np.ones(ok.sum()), (h.d2 - m.d2)[ok], (h.pvp - m.pvp)[ok]]
        b_a = _ols(Xa, (h.hr - m.hr)[ok].values, h.planted.values[ok])
        return b_y, b_a, h.groupby("state")[["ci", "ci_now", "d2", "pvp", "hr"]].mean()

    def predict(row, b_y, b_a, means):
        if row.state not in means.index:
            return None, None
        m = means.loc[row.state]
        ly = b_y @ np.array([1, row.ci_now - m.ci_now, row.pvp - m.pvp, row.d2 - m.d2])
        hr = m.hr + b_a @ np.array([1, row.d2 - m.d2, row.pvp - m.pvp])
        return float(np.exp(ly)), float(min(max(hr, 0.25), 1.0))

    # leave-one-year-out error (production-weighted), used for blending weights
    errs_y, errs_h = [], []
    for y in sorted(hist.year.unique()):
        tr_ = hist[hist.year != y]
        te = hist[hist.year == y]
        b_y, b_a, mm = fit(tr_)
        for _, row in te.iterrows():
            yr, hr = predict(row, b_y, b_a, mm)
            if yr is None:
                continue
            errs_y.append((math.log(yr) - row.ly, row.planted))
            if not pd.isna(row.hr):
                errs_h.append((hr - row.hr, row.planted))
    def wsd(e):
        a = np.array(e)
        return float(np.sqrt(np.average(a[:, 0] ** 2, weights=a[:, 1]))) if len(a) else None
    sd_y, sd_h = wsd(errs_y), wsd(errs_h)
    b_y, b_a, mm = fit(hist)
    now = df[df.year == cur]
    out = {}
    for _, row in now.iterrows():
        if pd.isna(row.trend):
            continue
        yr, hr = predict(row, b_y, b_a, mm)
        if yr is None:
            continue
        out[row.state] = {"yield": row.trend * yr, "yield_trend": row.trend, "yield_ratio": yr, "hr_drought": hr,
                          "ci": row.ci, "ci_now": row.ci_now, "d2": row.d2, "hr_avg": mm.loc[row.state].hr}
    return {"states": out, "sd_yield": sd_y, "sd_hr": sd_h, "years": [int(hist.year.min()), int(hist.year.max())]}


def fsa_model(ann, cur):
    """FSA reported upland acres → planted (scaled to NASS) and harvested share.

    FSA 'planted' excludes failed acres, so total planted = planted + failed. The scale factor to NASS
    planted and the failed-share → NASS abandonment relation are estimated from past years at the same
    release month."""
    import os
    p = os.path.join(load.D, "fsa", "fsa_cotton_state.csv")
    if not os.path.exists(p):
        return None
    f = pd.read_csv(p)
    f = f[f.type.astype(str).str.upper().str.contains("UPLAND")].copy()
    f["st"] = f.state.astype(str).str.upper().map(load.NAME_STATE)
    f["rm"] = pd.to_datetime(f.release).dt.month
    g = f.groupby(["crop_year", "release", "rm", "st"])[["planted", "failed"]].sum().reset_index()
    irr = f[f.irr == "N"].groupby(["crop_year", "release", "st"])[["planted", "failed"]].sum()
    g["tot"] = g.planted + g.failed
    g["fs"] = g.failed / g.tot
    pl = load.pick(ann, "upland_planted", final_only=True)
    hv = load.pick(ann, "upland_harvested", final_only=True)
    g["npl"] = [pl.get((y, s)) if y < cur else None for y, s in zip(g.crop_year, g.st)]
    g["nhv"] = [hv.get((y, s)) if y < cur else None for y, s in zip(g.crop_year, g.st)]
    g["ratio"] = g.npl / g.tot
    g["ab"] = 1 - g.nhv / g.npl
    hist = g.dropna(subset=["npl", "nhv"])
    med = hist.groupby("st").ratio.median()
    hist = hist[(hist.ratio / hist.st.map(med)).between(0.7, 1.4)]        # drop the odd duplicated release
    now = g[g.crop_year == cur]
    if not len(now):
        return None
    rel = now.release.max()
    rm = int(pd.to_datetime(rel).month)
    now = now[now.release == rel]
    hx = hist[hist.rm == rm] if (hist.rm == rm).sum() >= 30 else hist
    b = np.polyfit(hx.fs, hx.ab, 1, w=np.sqrt(hx.npl))
    resid = (hx.ab - np.polyval(b, hx.fs)).groupby(hx.st).mean()
    # leave-one-year-out error of the harvested-share estimate at this release month
    e = []
    for y in sorted(hx.crop_year.unique()):
        tr, te = hx[hx.crop_year != y], hx[hx.crop_year == y]
        bb = np.polyfit(tr.fs, tr.ab, 1, w=np.sqrt(tr.npl))
        rr = (tr.ab - np.polyval(bb, tr.fs)).groupby(tr.st).mean()
        e += [((np.polyval(bb, r.fs) + 0.5 * rr.get(r.st, 0)) - r.ab, r.npl) for _, r in te.iterrows()]
    e = np.array(e)
    sd = float(np.sqrt(np.average(e[:, 0] ** 2, weights=e[:, 1]))) if len(e) else 0.12
    rmed = hist[hist.rm == rm].groupby("st").ratio.median() if (hist.rm == rm).any() else med
    out = {}
    for _, r in now.iterrows():
        k = rmed.get(r.st, med.get(r.st, 1.02))
        ab = float(np.clip(np.polyval(b, r.fs) + 0.5 * resid.get(r.st, 0), 0, 0.95))
        dry = irr.loc[(cur, rel, r.st)] if (cur, rel, r.st) in irr.index else None
        out[r.st] = {"fsa_planted": r.planted, "fsa_failed": r.failed, "fsa_total": r.tot, "fail_share": r.fs,
                     "planted_adj": r.tot * k, "scale": k, "hr_fsa": 1 - ab,
                     "dry_fail_share": float(dry.failed / (dry.planted + dry.failed)) if dry is not None and (dry.planted + dry.failed) > 0 else None}
    return {"states": out, "release": rel, "sd_hr": sd}


def classing_model(cs, ann, cur):
    """Season-to-date upland bales classed by gin state → projected final bales and 480-lb production."""
    if cs is None or not len(cs):
        return None
    u = cs[(cs.commodity == "Upland") & (cs.period == "season") & cs.st.notna() & (cs.st != "US")].copy()
    if not len(u):
        return None
    u["swk"] = ((u.week_end - pd.to_datetime(u.crop_year.astype(str) + "-07-31")).dt.days // 7).astype(int)
    fin = u.groupby(["crop_year", "st"]).bales.max()
    last_wk = int(u[u.crop_year == cur].swk.max()) if (u.crop_year == cur).any() else None
    if last_wk is None:
        return None
    prod = load.pick(ann, "upland_prod", final_only=True)
    out, curve = {}, {}
    for s, g in u.groupby("st"):
        piv = g.pivot_table(index="swk", columns="crop_year", values="bales", aggfunc="max").sort_index().ffill()
        past = [y for y in piv.columns if y < cur and (y, s) in fin.index and fin[(y, s)] > 0]
        fr = []
        for y in past:
            if last_wk in piv.index and not pd.isna(piv.loc[last_wk, y]):
                fr.append(piv.loc[last_wk, y] / fin[(y, s)])
        cum = piv[cur].dropna().iloc[-1] if cur in piv and piv[cur].notna().any() else 0.0
        ly = piv.loc[last_wk, cur - 1] if (cur - 1) in piv and last_wk in piv.index else None
        k = [prod.get((y, s)) / fin[(y, s)] for y in past[-5:] if prod.get((y, s)) and fin[(y, s)] > 1000]
        f_med = float(np.median(fr)) if len(fr) >= 4 else None
        sd = float(np.std(np.log(np.clip(np.array(fr) / f_med, 1e-3, None)))) if f_med and f_med > 0 else None
        e = {"cum": cum, "ly_same_week": ly, "ly_final": fin.get((cur - 1, s)), "frac_normal": f_med, "sd_log": sd,
             "k480": float(np.median(k)) if k else None}
        if f_med and f_med >= 0.02 and cum > 0:
            e["final_bales"] = cum / f_med
            if e["k480"]:
                e["prod"] = e["final_bales"] * e["k480"]
        out[s] = e
        # average pace curve (% of final by season week) for the chart
        cv = {}
        for y in past[-5:]:
            cv[y] = (piv[y] / fin[(y, s)] * 100).dropna()
        if cv:
            curve[s] = pd.DataFrame(cv).mean(axis=1).round(1)
    return {"states": out, "week": last_wk, "week_end": u[u.crop_year == cur].week_end.max(), "curve": curve}


def blend(c, g):
    """Inverse-variance blend of the condition estimate and the classing-pace estimate (log space)."""
    if c is None and g is None:
        return None, None
    if g is None or g.get("prod") is None or not g.get("sd_log"):
        return (c["prod"], 0.0) if c else (None, None)
    if c is None or c.get("prod") is None:
        return g["prod"], 1.0
    vc, vg = c["sd_log"] ** 2, max(g["sd_log"], 0.01) ** 2
    w = (1 / vg) / (1 / vg + 1 / vc)
    return math.exp(w * math.log(g["prod"]) + (1 - w) * math.log(c["prod"])), w


def run(cond, prog, dr, cur):
    ann = load.annual()
    res = {}
    wk = int(cond[cond.year == cur].woy.max())
    cm = condition_model(cond, dr, ann, cur, min(wk, 42)) if len(ann) else None
    fm = fsa_model(ann, cur) if len(ann) else None
    cs = load.classing_summary()
    gm = classing_model(cs, ann, cur) if len(ann) else None
    planted = load.pick(ann, "upland_planted", year=cur) if len(ann) else pd.Series(dtype=float)
    usda_p = load.pick(ann, "upland_prod", year=cur) if len(ann) else pd.Series(dtype=float)
    usda_y = load.pick(ann, "upland_yield", year=cur) if len(ann) else pd.Series(dtype=float)
    usda_h = load.pick(ann, "upland_harvested", year=cur) if len(ann) else pd.Series(dtype=float)
    rows = []
    states = sorted(set(list((cm or {}).get("states", {}).keys()) + list((gm or {}).get("states", {}).keys())))
    for s in states:
        c = (cm or {}).get("states", {}).get(s)
        g = (gm or {}).get("states", {}).get(s)
        nass_pl = planted.get((cur, s))
        fs = (fm or {}).get("states", {}).get(s)
        pl = fs["planted_adj"] if fs else nass_pl
        up, uy = usda_p.get((cur, s)), usda_y.get((cur, s))
        row = {"st": s, "name": load.STATE_NAME.get(s, s), "planted": pl, "planted_nass": nass_pl, "usda_prod": up,
               "usda_yield": uy, "usda_harv": usda_h.get((cur, s)) or (up * 480 / uy if up and uy else None)}
        if fs:
            row.update({"fsa_total": fs["fsa_total"], "fsa_failed": fs["fsa_failed"], "fail_share": fs["fail_share"],
                        "hr_fsa": fs["hr_fsa"]})
        ce = None
        if c and pl:
            hr = c["hr_drought"]
            row["hr_drought"] = hr
            if fs:                                   # blend the drought- and FSA-based harvested shares
                v1, v2 = (cm["sd_hr"] or 0.1) ** 2, (fm["sd_hr"] or 0.12) ** 2
                hr = (hr / v1 + fs["hr_fsa"] / v2) / (1 / v1 + 1 / v2)
            harv = pl * hr
            prod = harv * c["yield"] / 480
            sd = math.sqrt((cm["sd_yield"] or 0.1) ** 2 + ((cm["sd_hr"] or 0.05) / max(hr, 0.3)) ** 2)
            ce = {"prod": prod, "sd_log": sd}
            row.update({"harv_ratio": hr, "harv": harv, "yield_cond": c["yield"], "yield_trend": c["yield_trend"],
                        "prod_cond": prod, "ci": c["ci"], "d2": c["d2"]})
        if g:
            row.update({"classed": g["cum"], "classed_ly": g.get("ly_same_week"), "frac_normal": g.get("frac_normal"),
                        "prod_gin": g.get("prod")})
        p, w = blend(ce, g)
        row.update({"prod": p, "w_gin": w})
        if p and row.get("harv"):
            row["yield_implied"] = p * 480 / row["harv"]
        rows.append(row)
    rows.sort(key=lambda x: -(x.get("prod") or 0))
    tot = lambda k: sum(x.get(k) or 0 for x in rows)
    us = {"prod": tot("prod"), "prod_cond": tot("prod_cond"), "prod_gin_states": tot("prod_gin"), "planted": tot("planted"),
          "harv": tot("harv"), "usda_prod": usda_p.get((cur, "US")), "usda_yield": usda_y.get((cur, "US")),
          "usda_harv": usda_h.get((cur, "US")), "usda_planted": planted.get((cur, "US"))}
    if us["harv"]:
        us["yield"] = us["prod"] * 480 / us["harv"]
    us["planted_nass"] = tot("planted_nass")
    us["fsa_failed"] = tot("fsa_failed")
    us["fsa_total"] = tot("fsa_total")
    res["estimate"] = {"states": rows, "us": us, "fsa_release": (fm or {}).get("release"),
                       "model": {"years": (cm or {}).get("years"), "sd_yield": (cm or {}).get("sd_yield")}}
    res["ginnings"] = ginnings_table(cur)
    if gm:
        res["classing_pace"] = {"week": gm["week"], "week_end": gm["week_end"],
                                "curve": {s: [[int(k), float(v)] for k, v in c.items()] for s, c in gm["curve"].items()}}
    return res


def _interp_at(dates, vals, t):
    """Cumulative value at date t by linear interpolation (0 before the first report)."""
    s = sorted(zip(dates, vals))
    if not s:
        return None
    xs = np.array([(d - s[0][0]).days for d, _ in s], float)
    x = (t - s[0][0]).days
    if x < 0:
        return 0.0
    if x > xs[-1]:
        return float(s[-1][1])
    return float(np.interp(x, xs, [v for _, v in s]))


def ginnings_table(cur):
    import os
    p = os.path.join(load.D, "ginning", "ginnings.csv")
    if not os.path.exists(p):
        return None
    g = pd.read_csv(p)
    g = g[(g.item == "all_rb") & g.ref_date.notna()].copy()
    g["ref_date"] = pd.to_datetime(g.ref_date)
    now = g[g.year == cur]
    if not len(now):
        return None
    last = now[now.state == "US"].sort_values("ref_date").iloc[-1] if (now.state == "US").any() else now.sort_values("ref_date").iloc[-1]
    t, label = last.ref_date, last.period.title()
    rows = []
    for s, x in now[now.ref_date == t].groupby("state"):
        v = float(x.value.iloc[0])
        def at(y):
            h = g[(g.year == y) & (g.state == s)]
            if not len(h):
                return None, None
            fin = h[h.period.str.upper().isin(["MAY"])].value.max() if (h.period.str.upper() == "MAY").any() else h.value.max()
            shift = pd.DateOffset(years=cur - y)
            ds = [pd.Timestamp(cur, 8, 1)] + list(h.ref_date + shift)          # nothing ginned before August
            return _interp_at(ds, [0.0] + list(h.value), t), fin
        ly, ly_fin = at(cur - 1)
        exact = bool(((g.year == cur - 1) & (g.state == s) & (g.period == x.period.iloc[0])).any())
        per = x.period.iloc[0]
        norm = []
        for y in range(cur - 6, cur):
            h = g[(g.year == y) & (g.state == s)]
            if (h.period == per).any() and len(h):
                fin = h[h.period.str.upper() == "MAY"].value.max() if (h.period.str.upper() == "MAY").any() else h.value.max()
                if fin:
                    norm.append(float(h[h.period == per].value.iloc[0]) / fin)
        rows.append({"st": s, "name": load.STATE_NAME.get(s, s), "cur": v, "ly": ly if exact else None,
                     "pct_ly_final": v / ly_fin * 100 if ly_fin else None, "pct_normal": float(np.mean(norm)) * 100 if norm else None})
    rows.sort(key=lambda r: (r["st"] != "US", -r["cur"]))
    return {"period_label": f"{label} ({t:%b %d, %Y})", "table": rows}
