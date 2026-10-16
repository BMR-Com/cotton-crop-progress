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
                          "ci": row.ci, "ci_now": row.ci_now, "d2": row.d2, "pvp": row.pvp, "hr_avg": mm.loc[row.state].hr}
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
        e += [((np.polyval(bb, r.fs) + 0.8 * rr.get(r.st, 0)) - r.ab, r.npl) for _, r in te.iterrows()]
    e = np.array(e)
    sd = float(np.sqrt(np.average(e[:, 0] ** 2, weights=e[:, 1]))) if len(e) else 0.12
    rmed = hist[hist.rm == rm].groupby("st").ratio.median() if (hist.rm == rm).any() else med
    out = {}
    for _, r in now.iterrows():
        k = rmed.get(r.st, med.get(r.st, 1.02))
        ab = float(np.clip(np.polyval(b, r.fs) + 0.8 * resid.get(r.st, 0), 0, 0.95))
        dry = irr.loc[(cur, rel, r.st)] if (cur, rel, r.st) in irr.index else None
        out[r.st] = {"fsa_planted": r.planted, "fsa_failed": r.failed, "fsa_total": r.tot, "fail_share": r.fs,
                     "planted_adj": r.tot * k, "scale": k, "hr_fsa": 1 - ab,
                     "dry_fail_share": float(dry.failed / (dry.planted + dry.failed)) if dry is not None and (dry.planted + dry.failed) > 0 else None}
    return {"states": out, "release": rel, "sd_hr": sd}


def abandonment_model(cond, dr, ann, fm, cur, wk):
    """Projected FINAL abandonment by state. Failed acres reported to FSA so far understate the final
    figure (more is abandoned/zeroed later), so the projection maps what is known at this point of the
    season — FSA failed share at this release month, D2–D4 drought and poor/very-poor ratings through
    the latest week (drought capped at end-October) — to the final NASS abandonment, using every past
    season at the same release month. State averages are removed so each state is judged vs its norm."""
    import os
    p = os.path.join(load.D, "fsa", "fsa_cotton_state.csv")
    if fm is None or not os.path.exists(p):
        return None
    f = pd.read_csv(p)
    f = f[f.type.astype(str).str.upper().str.contains("UPLAND")].copy()
    f["st"] = f.state.astype(str).str.upper().map(load.NAME_STATE)
    f["rm"] = pd.to_datetime(f.release).dt.month
    rm = int(pd.to_datetime(fm["release"]).month)
    g = f[f.rm == rm].groupby(["crop_year", "st"])[["planted", "failed"]].sum().reset_index()
    g["fs"] = g.failed / (g.planted + g.failed)
    F = _week_features(cond, dr, sorted(set(cond.year)), min(wk, 44)).reset_index().rename(columns={"year": "crop_year", "state": "st"})
    g = g.merge(F[["crop_year", "st", "pvp", "d2"]], on=["crop_year", "st"], how="left")
    g["d2"] = g.d2.fillna(0)
    pl = load.pick(ann, "upland_planted", final_only=True)
    hv = load.pick(ann, "upland_harvested", final_only=True)
    g["npl"] = [pl.get((y, s)) if y < cur else None for y, s in zip(g.crop_year, g.st)]
    g["ab"] = [1 - hv.get((y, s)) / pl.get((y, s)) if y < cur and hv.get((y, s)) and pl.get((y, s)) else None for y, s in zip(g.crop_year, g.st)]
    feats = ["fs", "d2", "pvp"]
    hist = g[(g.crop_year < cur)].dropna(subset=feats + ["ab", "npl"])
    hist = hist[hist.npl > 20000]
    if hist.crop_year.nunique() < 6:
        return None

    def fit(tr):
        m = tr.groupby("st")[feats + ["ab"]].mean()
        X = np.c_[np.ones(len(tr)), tr[feats].values - m.loc[tr.st, feats].values]
        b = _ols(X, (tr.ab.values - m.loc[tr.st, "ab"].values), tr.npl.values)
        return b, m

    def pred(row, b, m, tr):
        base = m.loc[row.st] if row.st in m.index else tr[feats + ["ab"]].mean()
        x = np.r_[1, np.array([row[c] for c in feats], float) - base[feats].values.astype(float)]
        return float(np.clip(base.ab + x @ b, 0, 0.95))

    err = []
    for y in sorted(hist.crop_year.unique()):
        tr, te = hist[hist.crop_year != y], hist[hist.crop_year == y]
        b, m = fit(tr)
        err += [(pred(r, b, m, tr) - r.ab, r.npl) for _, r in te.iterrows()]
    e = np.array(err)
    sd = float(np.sqrt(np.average(e[:, 0] ** 2, weights=e[:, 1])))
    b, m = fit(hist)
    out = {}
    for _, r in g[g.crop_year == cur].dropna(subset=["fs"]).iterrows():
        rr = r.copy()
        rr["pvp"] = rr.pvp if not pd.isna(rr.pvp) else hist[hist.st == r.st].pvp.mean()
        out[r.st] = {"ab_final": pred(rr, b, m, hist), "fs_now": r.fs, "ab_avg": float(m.loc[r.st, "ab"]) if r.st in m.index else None}
    # Texas history at this release month for the page: failed share then vs final abandonment
    tx = hist[hist.st == "TX"].sort_values("crop_year")
    hist_tx = [{"year": int(r.crop_year), "fs": r.fs, "d2": r.d2, "pvp": r.pvp, "ab": r.ab} for _, r in tx.iterrows()]
    return {"states": out, "sd": sd, "release_month": rm, "tx_history": hist_tx}


def classing_model(cs, ann, cur, prog=None):
    """Season-to-date upland bales classed by gin state → projected final bales and 480-lb production.

    How much of a crop has been classed by a given week depends on how early the harvest is, so the
    pace is measured against harvest progress (share of the crop classed when the state's harvest
    was at today's %), with the calendar-week pace as a fallback. Past seasons give the normal share
    and its spread; bales are converted to 480-lb production with each state's recent ratio."""
    if cs is None or not len(cs):
        return None
    u = cs[(cs.commodity == "Upland") & (cs.period == "season") & cs.st.notna() & (cs.st != "US")].copy()
    u = u.dropna(subset=["bales"])
    if not len(u):
        return None
    u["swk"] = ((u.week_end - pd.to_datetime(u.crop_year.astype(str) + "-07-31")).dt.days // 7).astype(int)
    fin = u.groupby(["crop_year", "st"]).bales.max()
    last_wk = int(u[u.crop_year == cur].swk.max()) if (u.crop_year == cur).any() else None
    if last_wk is None:
        return None
    prod = load.pick(ann, "upland_prod", final_only=True)
    hv = prog.dropna(subset=["HARVESTED"]) if prog is not None and "HARVESTED" in prog else None

    def harvest_at(s, y, dates):
        if hv is None:
            return None
        h = hv[(hv.state == s) & (hv.year == y)].sort_values("week_ending")
        if len(h) < 3:
            return None
        x0 = h.week_ending.min() - pd.Timedelta(days=14)
        xs = np.r_[0, (h.week_ending - x0).dt.days.values]
        ys = np.r_[0, h.HARVESTED.values]
        return np.interp((pd.to_datetime(dates) - x0).days.values if hasattr(dates, "__len__") else [(dates - x0).days], xs, ys)

    out, curve = {}, {}
    for s, g in u.groupby("st"):
        piv = g.pivot_table(index="swk", columns="crop_year", values="bales", aggfunc="max").sort_index().ffill()
        past = [y for y in piv.columns if y < cur and (y, s) in fin.index and fin[(y, s)] > 0]
        cum = piv[cur].dropna().iloc[-1] if cur in piv and piv[cur].notna().any() else 0.0
        ly = piv.loc[last_wk, cur - 1] if (cur - 1) in piv and last_wk in piv.index else None
        # calendar pace
        fr_t = [piv.loc[last_wk, y] / fin[(y, s)] for y in past if last_wk in piv.index and not pd.isna(piv.loc[last_wk, y])]
        # harvest-progress pace
        now_date = g[g.crop_year == cur].week_end.max()
        h_now = harvest_at(s, cur, [now_date])
        fr_h = []
        if h_now is not None and h_now[0] >= 5:
            for y in past:
                gy = g[g.crop_year == y].sort_values("week_end")
                hy = harvest_at(s, y, gy.week_end.values)
                if hy is None:
                    continue
                f = gy.bales.values / fin[(y, s)]
                hy = np.maximum.accumulate(hy)
                if hy.max() >= h_now[0]:
                    fr_h.append(float(np.interp(h_now[0], hy, f)))
        def stat(fr):
            fr = [x for x in fr if x and x > 0]
            if len(fr) < 4:
                return None, None
            m = float(np.median(fr))
            return m, float(np.std(np.log(np.array(fr) / m)))
        ft, sdt = stat(fr_t)
        fh, sdh = stat(fr_h)
        use = (fh, sdh, "harvest") if fh and (sdt is None or sdh <= sdt) else (ft, sdt, "calendar")
        k = [prod.get((y, s)) / fin[(y, s)] for y in past[-5:] if prod.get((y, s)) and fin[(y, s)] > 1000]
        e = {"cum": cum, "ly_same_week": ly, "ly_final": fin.get((cur - 1, s)), "frac_normal": use[0], "sd_log": use[1],
             "basis": use[2], "harvest_now": float(h_now[0]) if h_now is not None else None, "k480": float(np.median(k)) if k else None}
        if use[0] and use[0] >= 0.02 and cum > 0:
            e["final_bales"] = cum / use[0]
            if e["k480"]:
                e["prod"] = e["final_bales"] * e["k480"]
        out[s] = e
        cv = {y: (piv[y] / fin[(y, s)] * 100).dropna() for y in past[-5:]}
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
    am = abandonment_model(cond, dr, ann, fm, cur, wk) if fm else None
    cs = load.classing_summary()
    gm = classing_model(cs, ann, cur, prog) if len(ann) else None
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
            a = (am or {}).get("states", {}).get(s)
            if a:                                    # projected final abandonment: FSA failed so far + drought + ratings
                hr = 1 - a["ab_final"]
                row.update({"ab_final": a["ab_final"], "ab_avg": a["ab_avg"]})
            harv = pl * hr
            prod = harv * c["yield"] / 480
            sd_h = (am or {}).get("sd") or cm["sd_hr"] or 0.05
            sd = math.sqrt((cm["sd_yield"] or 0.1) ** 2 + (sd_h / max(hr, 0.3)) ** 2)
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
    if am:
        res["abandonment"] = {"tx_history": am["tx_history"], "release_month": am["release_month"]}
        txc = (cm or {}).get("states", {}).get("TX") or {}
        us["tx_d2"], us["tx_pvp"] = txc.get("d2"), txc.get("pvp")
    res["estimate"] = {"states": rows, "us": us, "fsa_release": (fm or {}).get("release"),
                       "model": {"years": (cm or {}).get("years"), "sd_yield": (cm or {}).get("sd_yield")}}
    res["ginnings"] = ginnings_table(cur)
    for key, fn in (("classing", classing_section), ("quality", quality_section)):
        try:
            res[key] = fn(cs, cur)
        except Exception as e:                  # keep the rest of the page if a report changes shape
            import traceback; traceback.print_exc()
            print(key, "section failed:", e)
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


# ───────────────────────── classing pace & fibre quality ─────────────────────────
def _swk(df, cy_col="crop_year"):
    return ((df.week_end - pd.to_datetime(df[cy_col].astype(str) + "-07-31")).dt.days // 7).astype(int)


def classing_section(cs, cur):
    """Tables and series for the classing-pace section (upland, by gin state)."""
    if cs is None or not len(cs):
        return None
    u = cs[(cs.commodity == "Upland") & cs.st.notna()].copy()
    if not len(u):
        return None
    u["swk"] = _swk(u)
    sea = u[u.period == "season"]
    wkl = u[u.period == "week"]
    if not (sea.crop_year == cur).any():
        return None
    lw = int(sea[sea.crop_year == cur].swk.max())
    week_end = sea[sea.crop_year == cur].week_end.max()
    # state rows + a US total (sum of states when the report has no national row)
    def agg_us(df):
        g = df[df.st != "US"].groupby(["crop_year", "swk"])
        t = g.bales.sum().to_frame()
        for c in ("tenderable_pct", "eligible_pct"):
            t[c] = g.apply(lambda x: np.average(x[c].fillna(0), weights=x.bales.fillna(0)) if x.bales.fillna(0).sum() > 0 and x[c].notna().any() else np.nan)
        t["gins"] = g.gins.sum()
        t = t.reset_index(); t["st"] = "US"
        return t
    if not (sea.st == "US").any():
        sea = pd.concat([sea, agg_us(sea)], ignore_index=True)
    if not (wkl.st == "US").any() and len(wkl):
        wkl = pd.concat([wkl, agg_us(wkl)], ignore_index=True)
    fin = sea.groupby(["crop_year", "st"]).bales.max()
    series, weekly, table = {}, {}, []
    for s, g in sea.groupby("st"):
        piv = g[(g.swk >= 0) & (g.swk <= 48)].pivot_table(index="swk", columns="crop_year", values="bales", aggfunc="max").sort_index()
        d = {}
        if cur in piv:
            d["cur"] = [[int(k), float(v)] for k, v in piv[cur].dropna().items()]
        if cur - 1 in piv:
            d["ly"] = [[int(k), float(v)] for k, v in piv[cur - 1].dropna().items() if k <= 48]
        hist = [y for y in range(cur - 5, cur) if y in piv]
        if hist:
            a = piv[hist].ffill().mean(axis=1).dropna()
            d["avg5"] = [[int(k), round(float(v))] for k, v in a.items() if k <= 48]
        series[s] = d
        w = wkl[(wkl.st == s) & (wkl.swk >= 0) & (wkl.swk <= 48)].pivot_table(index="swk", columns="crop_year", values="bales", aggfunc="max").sort_index() if len(wkl) else pd.DataFrame()
        dw = {}
        if cur in w:
            dw["cur"] = [[int(k), float(v)] for k, v in w[cur].dropna().items()]
        if cur - 1 in w:
            dw["ly"] = [[int(k), float(v)] for k, v in w[cur - 1].dropna().items() if k <= 48]
        hist = [y for y in range(cur - 5, cur) if y in w]
        if hist:
            a = w[hist].mean(axis=1).dropna()
            dw["avg5"] = [[int(k), round(float(v))] for k, v in a.items() if k <= 48]
        weekly[s] = dw
        now = g[(g.crop_year == cur) & (g.swk == lw)]
        if not len(now):
            continue
        now = now.iloc[-1]
        ly_cut = week_end - pd.DateOffset(years=1) + pd.Timedelta(days=3)       # same calendar week last year
        ly = g[(g.crop_year == cur - 1) & (g.week_end <= ly_cut)].sort_values("week_end")
        ly = ly.iloc[-1] if len(ly) else None
        fr = [float(piv.loc[:lw, y].dropna().iloc[-1] / fin[(y, s)]) for y in range(cur - 5, cur)
              if y in piv and piv.loc[:lw, y].notna().any() and fin.get((y, s))]
        wk_now = wkl[(wkl.st == s) & (wkl.crop_year == cur) & (wkl.swk == lw)].bales.sum() if len(wkl) else None
        if pd.isna(now.bales) and (ly is None or pd.isna(ly.bales)):
            continue
        table.append({"st": s, "name": load.STATE_NAME.get(s, s), "bales": now.bales, "bales_ly": ly.bales if ly is not None else None,
                      "week": wk_now, "frac_normal": float(np.mean(fr)) if fr else None,
                      "tender": now.tenderable_pct, "tender_ly": ly.tenderable_pct if ly is not None else None,
                      "elig": now.eligible_pct, "elig_ly": ly.eligible_pct if ly is not None else None, "gins": now.gins})
    table.sort(key=lambda r: (r["st"] != "US", -(r["bales"] or 0)))
    states = [r["st"] for r in table]
    us = next((r for r in table if r["st"] == "US"), None)
    lead = ""
    if us:
        ch = (us["bales"] / us["bales_ly"] - 1) * 100 if us.get("bales_ly") else None
        lead = (f"{us['bales'] / 1e6:.2f}m upland bales classed this season through {week_end:%b %d}"
                + (f", {ch:+.0f}% vs the same week last year" if ch is not None else "")
                + (f"; normally {us['frac_normal'] * 100:.0f}% of the crop is classed by now." if us.get("frac_normal") else "."))
    return {"week": lw, "week_end": week_end, "states": states, "series": series, "weekly": weekly, "table": table, "lead": lead}


COLOR_GROUP = {"11 & 21": "21 & better", "31": "31", "41": "41", "51": "51", "12 & 22": "Light spotted", "32": "Light spotted",
               "42": "Light spotted", "52": "Light spotted", "13 & 23": "Spotted", "33": "Spotted", "43": "Spotted",
               "53": "Spotted", "24 - 54": "Tinged"}
COLOR_ORDER = ["21 & better", "31", "41", "51", "Light spotted", "Spotted", "Tinged"]


def _state_metrics(a, c, s_row):
    """Season-to-date quality metrics for one state at one week (a: attributes rows, c: color rows)."""
    def avg(attr, bucket):
        x = a[(a.attribute == attr) & (a.bucket == bucket)].season_pct
        return float(x.iloc[0]) if len(x) and pd.notna(x.iloc[0]) else None
    def share(attr, buckets):
        x = a[(a.attribute == attr) & a.bucket.isin(buckets)].season_pct
        return float(x.sum()) if len(x) else None
    def strength_share(lo=None, hi=None):
        x = a[(a.attribute == "Strength")].copy()
        x["v"] = pd.to_numeric(x.bucket.str.extract(r"(\d+)")[0], errors="coerce")
        x = x[x.bucket.str.match(r"^\d") & x.v.notna()]
        if lo is not None:
            x = x[x.v >= lo]
        if hi is not None:
            x = x[x.v <= hi]
        return float(x.season_pct.sum()) if len(x) else None
    def staple_share(lo=None, hi=None):
        x = a[(a.attribute == "Staple") & a.bucket.str.match(r"^\d+$")].copy()
        x["v"] = x.bucket.astype(int)
        if lo is not None:
            x = x[x.v >= lo]
        if hi is not None:
            x = x[x.v <= hi]
        return float(x.season_pct.sum()) if len(x) else None
    tot = c[c.leaf.astype(str).str.startswith("Total")]
    grp = tot.assign(g=tot.color.map(COLOR_GROUP)).groupby("g").season_pct.sum() if len(tot) else pd.Series(dtype=float)
    m = {
        "Tenderable, %": (s_row.get("tenderable_pct"), 1, 1),
        "CCC loan-eligible, %": (s_row.get("eligible_pct"), 1, 1),
        "Color 31 & better, %": ((grp.get("21 & better", 0) + grp.get("31", 0)) if len(grp) else None, 1, 1),
        "Spotted / light spotted / tinged, %": ((grp.get("Light spotted", 0) + grp.get("Spotted", 0) + grp.get("Tinged", 0)) if len(grp) else None, 1, -1),
        "Average leaf grade": (avg("Leaf Grade", "Average For All Leaf"), 2, -1),
        "Leaf 5 & higher, %": (share("Leaf Grade", ["5", "6", "7"]), 1, -1),
        "Average staple (32nds)": (avg("Staple", "Average Staple"), 2, 1),
        "Staple 33 & shorter, %": (staple_share(hi=33), 1, -1),
        "Staple 36 & longer, %": (staple_share(lo=36), 1, 1),
        "Average mike": (avg("MIKE", "Average For All MIKE"), 2, 0),
        "Mike 3.4 & below (discount), %": (share("MIKE Range", ["2.4 & Below", "2.5 - 2.6", "2.7 - 2.9", "3.0 - 3.2", "3.3 - 3.4"]), 1, -1),
        "Mike 5.0 & above (discount), %": (share("MIKE Range", ["5.0 - 5.2", "5.3 & Above"]), 1, -1),
        "Average strength (g/tex)": (avg("Strength", "Average For All Strengths"), 1, 1),
        "Strength 26 & below, %": (strength_share(hi=26), 1, -1),
        "Average uniformity": (avg("Uniformity", "Average For All Uniformity"), 1, 1),
        "Extraneous matter, %": (share("Extraneous Matter", list(a[a.attribute == "Extraneous Matter"].bucket.unique())), 1, -1),
    }
    m = {k: ((None if v[0] is None or (isinstance(v[0], float) and math.isnan(v[0])) else v[0]),) + v[1:] for k, v in m.items()}
    colors = [[k, float(grp.get(k, 0))] for k in COLOR_ORDER] if len(grp) else []
    return m, colors


def quality_section(cs, cur):
    a_cur = load.classing_detail("attributes", [cur])
    c_cur = load.classing_detail("colorleaf", [cur])
    if not len(a_cur):
        return None
    a_ly = load.classing_detail("attributes", [cur - 1])
    c_ly = load.classing_detail("colorleaf", [cur - 1])
    wk = a_cur.week_end.max()
    ly_wk = a_ly[a_ly.week_end <= wk - pd.DateOffset(years=1) + pd.Timedelta(days=3)].week_end.max() if len(a_ly) else None
    summ = cs[(cs.commodity == "Upland") & (cs.period == "season")] if cs is not None and len(cs) else pd.DataFrame()

    def srow(st, d):
        x = summ[(summ.st == st) & (summ.week_end == d)] if len(summ) else []
        return x.iloc[0].to_dict() if len(x) else {}

    metrics, colors, weights = {}, {}, {}
    per_state = {}
    for st in sorted(set(a_cur.st.dropna())):
        mc, cc = _state_metrics(a_cur[(a_cur.st == st) & (a_cur.week_end == wk)], c_cur[(c_cur.st == st) & (c_cur.week_end == wk)] if len(c_cur) else c_cur, srow(st, wk))
        ml, cl = ({}, [])
        if ly_wk is not None and pd.notna(ly_wk):
            ml, cl = _state_metrics(a_ly[(a_ly.st == st) & (a_ly.week_end == ly_wk)], c_ly[(c_ly.st == st) & (c_ly.week_end == ly_wk)] if len(c_ly) else c_ly, srow(st, ly_wk))
        per_state[st] = (mc, ml, cc, cl, srow(st, wk).get("bales") or 0, srow(st, ly_wk).get("bales") if ly_wk is not None else 0)

    def pack(mc, ml):
        return [{"label": k, "cur": v[0], "ly": (ml.get(k) or (None,))[0], "dp": v[1], "dir": v[2],
                 "unit": "%" if k.endswith("%") else ""} for k, v in mc.items()]

    # US = bales-weighted average of the gin states
    def wavg(idx, label):
        num = den = 0.0
        for st, t in per_state.items():
            v = t[idx].get(label, (None,))[0]
            w = t[4] if idx == 0 else (t[5] or 0)
            if v is not None and not (isinstance(v, float) and math.isnan(v)) and w and not math.isnan(w):
                num += v * w; den += w
        return num / den if den else None
    any_m = next(iter(per_state.values()))[0] if per_state else {}
    us_c = {k: (wavg(0, k), v[1], v[2]) for k, v in any_m.items()}
    us_l = {k: (wavg(1, k), v[1], v[2]) for k, v in any_m.items()}
    for k, col in (("Tenderable, %", "tenderable_pct"), ("CCC loan-eligible, %", "eligible_pct")):   # national rows from the report
        a1, b1 = srow("US", wk).get(col), srow("US", ly_wk).get(col) if ly_wk is not None else None
        if a1 is not None and not math.isnan(a1):
            us_c[k] = (a1,) + us_c[k][1:]
        if b1 is not None and not math.isnan(b1):
            us_l[k] = (b1,) + us_l[k][1:]
    metrics["US"] = pack(us_c, us_l)
    def wcol(i, wi):
        acc, den = {}, 0.0
        for t in per_state.values():
            w = t[wi] or 0
            if not t[i] or not w or math.isnan(w):
                continue
            den += w
            for k, v in t[i]:
                acc[k] = acc.get(k, 0) + v * w
        return [[k, acc.get(k, 0) / den] for k in COLOR_ORDER] if den else []
    colors["US"] = {"cur": wcol(2, 4), "ly": wcol(3, 5)}
    order = sorted(per_state, key=lambda s: -per_state[s][4])
    for st in order:
        mc, ml, cc, cl, _, _ = per_state[st]
        metrics[st] = pack(mc, ml)
        colors[st] = {"cur": cc, "ly": cl}
    # compact by-state table
    cols = ["Bales classed", "Tenderable %", "Δ vs LY", "31 & better %", "Δ vs LY", "Avg staple", "Δ vs LY", "Avg mike", "Avg strength", "Avg leaf"]
    def cell(v, dp=1, delta=False, good=1):
        if v is None:
            return {"t": None}
        if delta:
            return {"t": f"{v:+.{dp}f}", "c": "up" if v * good > 0 else "down" if v * good < 0 else ""}
        return {"t": f"{v:,.{dp}f}"}
    rows = []
    for st in ["US"] + order:
        m = {r["label"]: r for r in metrics[st]}
        g = lambda k: m.get(k, {}).get("cur")
        dl = lambda k: (m[k]["cur"] - m[k]["ly"]) if k in m and m[k]["cur"] is not None and m[k]["ly"] is not None else None
        b = sum(t[4] for t in per_state.values()) if st == "US" else per_state[st][4]
        rows.append([load.STATE_NAME.get(st, st), cell(b, 0), cell(g("Tenderable, %")), cell(dl("Tenderable, %"), 1, True),
                     cell(g("Color 31 & better, %")), cell(dl("Color 31 & better, %"), 1, True), cell(g("Average staple (32nds)"), 2),
                     cell(dl("Average staple (32nds)"), 2, True), cell(g("Average mike"), 2), cell(g("Average strength (g/tex)"), 1),
                     cell(g("Average leaf grade"), 2)])
    us = {r["label"]: r for r in metrics["US"]}
    def txt(k, unit="%", dp=1):
        r = us.get(k)
        if not r or r["cur"] is None:
            return ""
        return f"{r['cur']:.{dp}f}{unit}" + (f" (last year {r['ly']:.{dp}f}{unit})" if r.get("ly") is not None else "")
    lead = (f"Season to date through {wk:%b %d}, upland, bales-weighted across gin states: tenderable {txt('Tenderable, %')}, "
            f"color 31 & better {txt('Color 31 & better, %')}, average staple {txt('Average staple (32nds)', '', 2)}.")
    return {"week_end": wk, "states": ["US"] + order, "metrics": metrics, "color": colors,
            "by_state": {"cols": cols, "rows": rows}, "lead": lead}
