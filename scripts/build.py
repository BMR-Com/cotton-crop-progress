#!/usr/bin/env python3
"""
Build docs/data/crop.json for the dashboard: crop conditions/progress, drought, ginning & classing
pace, fibre quality and the crop estimate (conditions + drought, ginning pace, FSA acreage).
"""
import json, math, os, sys
from datetime import date, datetime, timezone
import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import load                                     # noqa: E402
import model                                    # noqa: E402

OUT = os.path.join(load.ROOT, "docs", "data", "crop.json")
MAIN = ["TX", "GA", "AR", "MS", "AL", "NC", "OK", "MO", "TN", "SC", "LA", "KS", "AZ", "CA", "NM", "FL", "VA"]


def r(x, n=1):
    if x is None or (isinstance(x, float) and (math.isnan(x) or math.isinf(x))):
        return None
    return round(float(x), n)


def clean(o):
    if isinstance(o, dict):
        return {str(k): clean(v) for k, v in o.items()}
    if isinstance(o, (list, tuple)):
        return [clean(v) for v in o]
    if isinstance(o, (np.floating, float)):
        return None if (math.isnan(o) or math.isinf(o)) else float(o)
    if isinstance(o, (np.bool_,)):
        return bool(o)
    if isinstance(o, (np.integer,)):
        return int(o)
    if isinstance(o, (pd.Timestamp, datetime, date)):
        return o.strftime("%Y-%m-%d")
    return o


def seasonal(df, col, states, cur, years_back=5, key="woy"):
    """{state: {cur: [[woy, v]], ly: [...], avg5: [...]}} for one weekly column."""
    out = {}
    for s in states:
        x = df[df.state == s]
        if not len(x):
            continue
        piv = x.pivot_table(index=key, columns="year", values=col, aggfunc="last")
        d = {}
        for lab, y in (("cur", cur), ("ly", cur - 1)):
            if y in piv:
                v = piv[y].dropna()
                d[lab] = [[int(k), r(val)] for k, val in v.items()]
        hist = [y for y in range(cur - years_back, cur) if y in piv]
        if hist:
            m = piv[hist].mean(axis=1, skipna=True).dropna()
            d["avg5"] = [[int(k), r(val)] for k, val in m.items()]
        out[s] = d
    return out


def last(series_dict, key="cur"):
    a = (series_dict or {}).get(key) or []
    return a[-1] if a else None


def at_week(series_dict, key, wk, tol=2):
    """Value at ISO week `wk`, or the nearest earlier week within `tol` weeks."""
    best = None
    for w, v in (series_dict or {}).get(key) or []:
        if wk - tol <= w <= wk:
            best = v
    return best


def m(x):
    return f"{x / 1e6:.2f}m" if x else "–"


def brief(res):
    cur = res["crop_year"]
    E = res.get("estimate") or {}
    us = E.get("us") or {}
    pts, kpis = [], []
    d = (us["prod"] / us["usda_prod"] - 1) * 100 if us.get("prod") and us.get("usda_prod") else None
    HC = res.get("headline")
    if HC:
        B, Dm, Rm = HC["balance"], HC["drought"], HC["ratings"]
        dd = HC["crop"] - HC["wasde"]
        head = (f"We estimate the {cur}/{(cur + 1) % 100:02d} US crop at {HC['crop']:.2f}m bales, {abs(dd):.2f}m "
                f"{'above' if dd > 0 else 'below'} the {HC['report']} WASDE ({HC['wasde']:.2f}m): the average of our drought model "
                f"({Dm['crop']:.2f}m) and ratings model ({Rm['crop']:.2f}m). Ending stocks {B['end_stocks']:.2f}m vs WASDE {B['wasde_end']:.2f}m." if Rm else
                f"We estimate the {cur}/{(cur + 1) % 100:02d} US crop at {HC['crop']:.2f}m bales vs WASDE {HC['wasde']:.2f}m.")
        pts.append(f"Drought model: Texas abandonment {Dm['tx_ab'] * 100:.1f}% (±{Dm['tx_ab_sd'] * 100:.0f} pts) from the highest-R² drought model "
                   f"({Dm['model']}, R² {Dm['r2'] * 100:.0f}%, cotton-area drought through {Dm['week']}), against {HC['tx_ab_usda'] * 100:.1f}% implied by USDA, "
                   f"at USDA's {Dm['tx_yield']:.0f} lb yield: Texas {Dm['tx_prod']:.2f}m bales, US crop {Dm['crop']:.2f}m (±1σ {Dm['lo']:.2f}–{Dm['hi']:.2f}m).")
        if Rm:
            pts.append(f"Ratings model: Texas condition index {Rm['ci']:.2f} (latest two weeks) points to {Rm['lbpa']:.0f} lb per planted acre "
                       f"(USDA {HC['tx_prod_usda'] * 1e6 * 480 / HC['tx_planted']:.0f}; fit R² {Rm['r2']:.2f} over {Rm['years'][0]}–{Rm['years'][1]}): "
                       f"Texas {Rm['tx_prod']:.2f}m bales, US crop {Rm['crop']:.2f}m (±1σ {Rm['lo']:.2f}–{Rm['hi']:.2f}m).")
        pts.append(f"Final: average of the two, Texas {HC['tx_prod']:.2f}m (USDA {HC['tx_prod_usda']:.2f}m), US crop {HC['crop']:.2f}m (±1σ {HC['lo']:.2f}–{HC['hi']:.2f}m); "
                   "every other state at USDA's latest.")
        pts.append(f"Balance sheet (WASDE {HC['my']}, our crop): beginning stocks {B['beg_stocks']:.2f}m + crop {B['prod']:.2f}m + imports {B['imports']:.2f}m "
                   f"− mill use {B['domestic']:.2f}m − exports {B['exports']:.2f}m = ending stocks {B['end_stocks']:.2f}m "
                   f"(stocks/use {B['su'] * 100:.1f}% vs WASDE {B['wasde_su'] * 100:.1f}%); ±1σ {B['end_lo']:.2f}–{B['end_hi']:.2f}m. "
                   "These stocks drive the price and spread fair values on the model tabs.")
        kpis.append({"label": "US crop — final (avg)", "value": f"{HC['crop']:.2f}m", "delta": f"{dd:+.2f}m", "good": dd >= 0,
                     "sub": f"WASDE {HC['wasde']:.2f}m · all cotton · ±1σ {HC['lo']:.2f}–{HC['hi']:.2f}"})
        kpis.append({"label": "Drought model", "value": f"{Dm['crop']:.2f}m", "delta": f"{Dm['crop'] - HC['wasde']:+.2f}m", "good": Dm['crop'] >= HC['wasde'],
                     "sub": f"Texas abandonment {Dm['tx_ab'] * 100:.0f}% (USDA {HC['tx_ab_usda'] * 100:.0f}%)"})
        if Rm:
            kpis.append({"label": "Ratings model", "value": f"{Rm['crop']:.2f}m", "delta": f"{Rm['crop'] - HC['wasde']:+.2f}m", "good": Rm['crop'] >= HC['wasde'],
                         "sub": f"Texas {Rm['lbpa']:.0f} lb/planted acre · condition {Rm['ci']:.2f}"})
        kpis.append({"label": "Ending stocks", "value": f"{B['end_stocks']:.2f}m", "delta": f"{B['end_stocks'] - B['wasde_end']:+.2f}m",
                     "good": B['end_stocks'] <= B['wasde_end'], "sub": f"WASDE {B['wasde_end']:.2f}m · S/U {B['su'] * 100:.1f}%"})
        if d is not None:
            pts.append(f"Crop-progress cross-check (ratings, drought, FSA failed acres, classing): US upland {m(us.get('prod'))} vs USDA {m(us.get('usda_prod'))} "
                       f"({d:+.1f}%). It is higher than the drought model because it projects Texas abandonment lower; it would replace the headline only if it proves the better predictor.")
    else:
        head = (f"We estimate the {cur} US upland crop at {m(us.get('prod'))} bales, "
                f"{abs(d):.1f}% {'above' if d > 0 else 'below'} USDA's {m(us.get('usda_prod'))}." if d is not None else "Crop estimate pending data.")
        kpis.append({"label": "Upland crop — our estimate", "value": m(us.get("prod")), "delta": f"{d:+.1f}%" if d is not None else None,
                     "good": (d or 0) >= 0, "sub": f"USDA {m(us.get('usda_prod'))} 480-lb bales"})
    if us.get("harv") and us.get("usda_harv") and not HC:
        dh = (us["harv"] / us["usda_harv"] - 1) * 100
        kpis.append({"label": "Harvested acres", "value": f"{us['harv'] / 1e6:.2f}m", "delta": f"{dh:+.1f}%", "good": dh >= 0,
                     "sub": f"USDA {us['usda_harv'] / 1e6:.2f}m · planted {us['planted'] / 1e6:.2f}m"})
    if us.get("yield") and us.get("usda_yield") and not HC:
        dy = us["yield"] - us["usda_yield"]
        kpis.append({"label": "Yield, lb/harvested acre", "value": f"{us['yield']:.0f}", "delta": f"{dy:+.0f}", "good": dy >= 0,
                     "sub": f"USDA {us['usda_yield']:.0f}"})
    C = res["conditions"]
    ge, ly = last(C["ge"].get("US")), None
    if ge:
        wk = ge[0]
        ly = at_week(C["ge"]["US"], "ly", wk); av = at_week(C["ge"]["US"], "avg5", wk)
        pv = last(C["pvp"].get("US"))
        kpis.append({"label": "US good/excellent", "value": f"{ge[1]:.0f}%", "delta": f"{ge[1] - ly:+.0f} pts" if ly is not None else None,
                     "good": ly is None or ge[1] >= ly, "sub": f"{cur - 1}: {ly:.0f}% · 5-yr {av:.0f}%" if ly is not None and av is not None else ""})
        pts.append(f"Crop ratings: {ge[1]:.0f}% good/excellent and {pv[1]:.0f}% poor/very poor nationally"
                   + (f", against {ly:.0f}% good/excellent a year ago and a {av:.0f}% five-year average." if ly is not None and av is not None else "."))
    tx = next((r for r in E.get("states", []) if r["st"] == "TX"), None)
    txd = last(res["drought"].get("TX"))
    if tx and tx.get("harv_ratio") is not None:
        uh = tx.get("usda_harv")
        t = ("Texas abandonment: " + (f"FSA failed acres are {tx['fail_share'] * 100:.1f}% of reported plantings so far, but failed acres "
             f"keep rising after October — on this season's drought and ratings we project final abandonment of {tx['ab_final'] * 100:.0f}% "
             f"(state average {tx['ab_avg'] * 100:.0f}%)" if tx.get("ab_final") is not None and tx.get("fail_share") is not None
             else f"{(1 - tx['harv_ratio']) * 100:.0f}% projected")
             + (f", vs {(1 - uh / tx['planted_nass']) * 100:.0f}% implied by USDA" if uh and tx.get("planted_nass") else "")
             + f". Yield {tx.get('yield_implied') or tx.get('yield_cond'):.0f} lb vs USDA {tx.get('usda_yield') or 0:.0f}"
             + (f"; {txd[1]:.0f}% of Texas in D2–D4 drought." if txd else "."))
        pts.append(t)
    diffs = [r for r in E.get("states", []) if r.get("prod") and r.get("usda_prod")]
    diffs.sort(key=lambda r: -abs(r["prod"] - r["usda_prod"]))
    if diffs:
        pts.append("Largest gaps to USDA: " + "; ".join(
            f"{r['name']} {m(r['prod'])} vs {m(r['usda_prod'])}" for r in diffs[:3]) + ".")
    H = res["progress"].get("HARVESTED", {}).get("US")
    h = last(H)
    if h:
        a5 = at_week(H, "avg5", h[0]); l1 = at_week(H, "ly", h[0])
        pts.append(f"Harvest {h[1]:.0f}% complete" + (f" vs {l1:.0f}% last year and {a5:.0f}% on average." if a5 is not None and l1 is not None else "."))
        kpis.append({"label": "Harvested", "value": f"{h[1]:.0f}%", "delta": f"{h[1] - a5:+.0f} pts" if a5 is not None else None,
                     "good": a5 is None or h[1] >= a5, "sub": f"5-yr avg {a5:.0f}%" if a5 is not None else ""})
    G = res.get("ginnings")
    if G:
        u = next((r for r in G["table"] if r["st"] == "US"), None)
        if u and u.get("pct_normal"):
            pts.append(f"NASS ginnings to {G['period_label']}: {u['cur'] / 1e6:.2f}m running bales, "
                       f"{u['pct_ly_final']:.1f}% of last season's total (normal {u['pct_normal']:.1f}% by this date).")
    GP = res.get("gin_projection")
    if GP and GP["us"].get("proj"):
        u = GP["us"]
        dg = (u["proj"] / u["usda"] - 1) * 100 if u.get("usda") else None
        tx = next((x for x in GP["states"] if x["st"] == "TX"), None)
        band = f" (±1σ {m(u['lo'])}–{m(u['hi'])})" if u.get("lo") else ""
        pts.append(f"Projection from ginnings: {m(u['proj'])} bales{band}, {dg:+.1f}% vs USDA, with {u['from_gin'] / u['proj'] * 100:.0f}% of it resting on ginning so far"
                   + (f"; Texas ginning pace alone points to {m(tx['gin_only'])} ({tx['gin_only'] * 480 / tx['harv']:.0f} lb on harvested acres after projected abandonment)"
                      if tx and tx.get("gin_only") and tx.get("harv") else "") + ".")
        kpis.insert(3, {"label": "Ginning projection (upland check)", "value": m(u["proj"]), "delta": f"{dg:+.1f}%" if dg is not None else None,
                        "good": (dg or 0) >= 0, "sub": (f"±{u['sd_log'] * 100:.1f}% (1σ) · " if u.get("sd_log") else "") + f"{u['from_gin'] / u['proj'] * 100:.0f}% ginning-based so far"})
    K = res.get("classing")
    if K:
        u = next((r for r in K["table"] if r["st"] == "US"), None)
        if u and u.get("bales"):
            ch = (u["bales"] / u["bales_ly"] - 1) * 100 if u.get("bales_ly") else None
            pts.append(K["lead"])
            kpis.append({"label": "Upland bales classed", "value": f"{u['bales'] / 1e6:.2f}m", "delta": f"{ch:+.0f}%" if ch is not None else None,
                         "good": (ch or 0) >= 0, "sub": f"through {K['week_end']:%b %d} · vs same week {cur - 1}"})
    Q = res.get("quality")
    if Q:
        qm = {r["label"]: r for r in Q["metrics"]["US"]}
        t = qm.get("Tenderable, %")
        if t and t["cur"] is not None:
            dt = t["cur"] - t["ly"] if t.get("ly") is not None else None
            kpis.append({"label": "Tenderable (season)", "value": f"{t['cur']:.1f}%", "delta": f"{dt:+.1f} pts" if dt is not None else None,
                         "good": (dt or 0) >= 0, "sub": f"{cur - 1} same week {t['ly']:.1f}%" if t.get("ly") is not None else ""})
        def d(k, dp=1, unit="%"):
            r = qm.get(k)
            if not r or r["cur"] is None:
                return "–"
            pu = " pts" if unit == "%" else ""
            return f"{r['cur']:.{dp}f}{unit}" + (f" ({r['cur'] - r['ly']:+.{dp}f}{pu} vs last year)" if r.get("ly") is not None else "")
        pts.append(f"Quality so far: tenderable {d('Tenderable, %')}; color 31 & better {d('Color 31 & better, %')}; "
                   f"staple {d('Average staple (32nds)', 2, '')}; mike 5.0+ discount {d('Mike 5.0 & above (discount), %')}; "
                   f"strength {d('Average strength (g/tex)', 1, ' g/tex')}.")
    # BMR-Com crop projection (stage-adjusted ginnings + ratings + drought, Texas abandonment, classing by office);
    # produced separately and published here as output only
    pp = os.path.join(os.path.dirname(OUT), "crop_projection.json")
    if os.path.exists(pp):
        try:
            P = json.load(open(pp))
            u, t, us = P["us_upland"], P["texas"], P.get("usda", {})
            rng = lambda x, a, b: f"{x[a]:.1f}–{x[b]:.1f}m"
            pts.insert(0, f"BMR-Com crop projection ({P['asof']}): US upland {u['central']:.2f}m bales "
                          f"(95% {rng(u, 'lo95', 'hi95')}; 99% {rng(u, 'lo99', 'hi99')}) vs USDA {us.get('us_upland', float('nan')):.2f}m; "
                          f"all cotton {P['us_all']['central']:.2f}m vs WASDE {us.get('us_all', float('nan')):.2f}m. "
                          f"Texas {t['central']:.2f}m (95% {rng(t, 'lo95', 'hi95')}) vs USDA {us.get('tx', float('nan')):.2f}m, "
                          f"with final Texas abandonment projected at {t['abandonment']['central'] * 100:.0f}% "
                          f"(95% {t['abandonment']['lo95'] * 100:.0f}–{t['abandonment']['hi95'] * 100:.0f}%) against {us.get('tx_abandonment', 0) * 100:.0f}% in USDA's numbers.")
            C = P.get("conditional")
            if C:
                an = C.get("analog") or {}
                ha, yd, tc, uc = C.get("tx_harvested_acres"), C.get("tx_yield"), C["texas"], C["us_upland"]
                pts.insert(1, (f"Conditional view (Texas built as harvested acres × yield): abandonment {C['tx_abandonment']['central'] * 100:.0f}% of "
                               f"{C.get('tx_planted_acres', 0):.1f}m planted leaves {ha['central']:.2f}m harvested acres (95% {ha['lo95']:.2f}–{ha['hi95']:.2f}m); "
                               f"at USDA's {yd['central']} lb (95% {yd['lo95']}–{yd['hi95']}) Texas is {tc['central']:.2f}m (95% {tc['lo95']:.2f}–{tc['hi95']:.2f}m). "
                               f"US upland {uc['central']:.2f}m (95% {uc['lo95']:.2f}–{uc['hi95']:.2f}m), all cotton {C['us_all']['central']:.2f}m." if ha else
                               f"Conditional view: Texas {tc['central']:.2f}m, US upland {uc['central']:.2f}m.")
                              + (f" Past seasons with similar Texas ratings ended at {an['min'] * 100:.0f}–{an['max'] * 100:.0f}% abandonment." if an else ""))
            if P.get("texas_regions"):
                pts.insert(2 if C else 1, P["texas_regions"])
            for line in P.get("notes", []):
                if C and line.startswith("Seasons with Texas poor"):      # already in the conditional line
                    continue
                pts.insert(2, line)
            kpis.insert(0, {"label": "BMR-Com US upland projection", "value": f"{u['central']:.2f}m",
                            "delta": f"{(u['central'] / us['us_upland'] - 1) * 100:+.1f}% vs USDA" if us.get("us_upland") else None,
                            "good": u["central"] >= us.get("us_upland", 0), "sub": f"95% {rng(u, 'lo95', 'hi95')}"})
            res["projection"] = P
            wa = us.get("us_all")
            head = (f"We project the {cur}/{(cur + 1) % 100:02d} US crop at {P['us_all']['central']:.2f}m bales (all cotton; upland "
                    f"{u['central']:.2f}m), {abs(P['us_all']['central'] - wa):.2f}m {'below' if P['us_all']['central'] < wa else 'above'} the "
                    f"{us.get('report', '')} WASDE ({wa:.2f}m). Texas drives the gap: {t['central']:.2f}m vs USDA {us['tx']:.2f}m, "
                    f"on {t['abandonment']['central'] * 100:.0f}% projected abandonment.") if wa else head
            for i, x in enumerate(pts):
                if x.startswith(("Drought model:", "Ratings model:", "Final: average")):
                    pts[i] = "Cross-check — " + (x.replace("Final: average", "Average", 1) if x.startswith("Final") else x)
        except Exception as e:
            print("crop projection skipped:", e)
    for extra in res.get("brief_extra", []):
        pts.append(extra)
    for k in res.get("kpis_extra", []):
        kpis.append(k)
    res["brief"] = {"date": str(res["asof"]["conditions"])[:10], "headline": head, "points": pts}
    res["kpis"] = kpis


def wasde_us(cur):
    """Latest WASDE US cotton production / ending stocks for the current marketing year (wasde-analytics)."""
    import requests
    loc = os.environ.get("WASDE_LOCAL")
    try:
        d = json.load(open(loc)) if loc else requests.get("https://raw.githubusercontent.com/BMR-Com/wasde-analytics/main/data/Cotton.json", timeout=60).json()
    except Exception as e:
        print("wasde unavailable", e)
        return None
    my = f"{cur}/{(cur + 1) % 100:02d}"
    out = {"my": my}
    for k, lab in (("prod", "Production"), ("end_stocks", "Ending Stocks"), ("exports", "Exports"), ("domestic", "Domestic Use"), ("planted", "Planted"),
                   ("harvested", "Harvested"), ("yield", "Yield per Harvested Acre"), ("beg_stocks", "Beginning Stocks"), ("imports", "Imports"),
                   ("unaccounted", "Unaccounted"), ("price", "Avg. Farm Price")):
        v = d["data"].get(f"United States||{lab}||{my}")
        if v:
            out[k] = v[-1]["value"]
            out["report"] = v[-1]["reportDate"]
    return out


DROUGHT_MODEL_URL = "https://raw.githubusercontent.com/BMR-Com/Cotton-Texas-Drought-analysis/main/docs/model.json"


def ratings_texas(cond, cur):
    """Texas crop from crop ratings: pounds per PLANTED acre (abandonment and yield together) regressed on the
    Texas condition index of the latest two weeks, using every past season at the same week (2001 on).
    Leave-one-year-out error is the model's own uncertainty."""
    ann = load.annual()
    pl = load.pick(ann, "upland_planted", final_only=True)
    pr = load.pick(ann, "upland_prod", final_only=True)
    t = cond[cond.state == "TX"]
    wk = int(t[t.year == cur].woy.max())

    def ci_now(y):
        g = t[(t.year == y) & (t.woy <= wk)].sort_values("week_ending").tail(2)
        return float(g.ci.mean()) if len(g) else None
    rows = [(y, ci_now(y), pr[(y, "TX")] * 480 / pl[(y, "TX")]) for y in sorted(set(t.year))
            if y < cur and (y, "TX") in pr.index and (y, "TX") in pl.index]
    rows = [r for r in rows if r[1] is not None]
    if len(rows) < 12:
        return None
    x, yv = np.array([r[1] for r in rows]), np.array([r[2] for r in rows])
    errs = []
    for k in range(len(rows)):
        m = np.arange(len(rows)) != k
        b = np.polyfit(x[m], yv[m], 1)
        errs.append(math.log(max(np.polyval(b, x[k]), 1) / yv[k]))
    b = np.polyfit(x, yv, 1)
    now = ci_now(cur)
    r2 = 1 - ((yv - np.polyval(b, x)) ** 2).sum() / ((yv - yv.mean()) ** 2).sum()
    return {"ci": now, "week": wk, "lbpa": float(np.polyval(b, now)), "slope": float(b[0]), "intercept": float(b[1]), "r2": float(r2),
            "sd_log": float(np.sqrt(np.mean(np.square(errs)))), "years": [rows[0][0], rows[-1][0]], "n": len(rows)}


def headline_crop(W, cur, cond):
    """The page's crop number, all cotton, against WASDE. Two Texas models, every other state at USDA's latest:
       1. drought: Texas abandonment from the highest-R² drought model (Texas drought analysis) × USDA's Texas yield
       2. ratings: Texas pounds per planted acre from the latest Texas condition index
    The final number is the average of the two, carried through the WASDE balance sheet (WASDE beginning
    stocks, imports, mill use, exports and unaccounted held) to ending stocks."""
    import requests
    loc = os.environ.get("DROUGHT_MODEL_LOCAL")
    try:
        j = json.load(open(loc)) if loc else requests.get(DROUGHT_MODEL_URL, timeout=60).json()
    except Exception as e:
        print("drought model unavailable", e)
        return None
    c, b = j.get("crop") or {}, j.get("tx_best") or {}
    if not c.get("all_prod") or not W or W.get("prod") is None:
        return None
    base = W["prod"] - c["tx_prod_usda"] / 1e6                      # WASDE crop without USDA's Texas
    tx_d = c["tx_prod"] / 1e6
    d_lo = (c["tx_planted"] * (1 - min(c["tx_ab"] + c["tx_ab_sd"], 0.95)) * c["tx_yield"] / 480) / 1e6
    d_hi = (c["tx_planted"] * (1 - max(c["tx_ab"] - c["tx_ab_sd"], 0)) * c["tx_yield"] / 480) / 1e6
    drought = {"crop": base + tx_d, "lo": base + d_lo, "hi": base + d_hi, "tx_prod": tx_d, "tx_ab": c["tx_ab"], "tx_ab_sd": c["tx_ab_sd"],
               "tx_ab_usda": c["tx_ab_usda"], "tx_yield": c["tx_yield"], "model": f"{b.get('variable')} {str(b.get('model', '')).lower()}",
               "r2": b.get("r2"), "week": j.get("latest_week")}
    rt = ratings_texas(cond, cur)
    ratings = None
    if rt:
        tx_r = c["tx_planted"] * rt["lbpa"] / 480 / 1e6
        ratings = dict(rt, crop=base + tx_r, tx_prod=tx_r, lo=base + tx_r * math.exp(-rt["sd_log"]), hi=base + tx_r * math.exp(rt["sd_log"]))
    tx_f = (tx_d + ratings["tx_prod"]) / 2 if ratings else tx_d
    SD_AVG = 0.166            # leave-one-year-out error of the averaged Texas crop at mid-October, 2002–2025
    crop, lo, hi = base + tx_f, base + tx_f * math.exp(-SD_AVG), base + tx_f * math.exp(SD_AVG)
    use = (W.get("domestic") or 0) + (W.get("exports") or 0)

    def end(p):
        return (W.get("beg_stocks") or 0) + p + (W.get("imports") or 0) - use - (W.get("unaccounted") or 0)
    bal = {"beg_stocks": W.get("beg_stocks"), "imports": W.get("imports"), "domestic": W.get("domestic"), "exports": W.get("exports"),
           "unaccounted": W.get("unaccounted"), "use": use, "prod": crop, "end_stocks": end(crop), "su": end(crop) / use if use else None,
           "end_lo": end(lo), "end_hi": end(hi), "wasde_prod": W["prod"], "wasde_end": W.get("end_stocks"),
           "wasde_su": (W.get("end_stocks") or 0) / use if use else None}
    return {"crop": crop, "lo": lo, "hi": hi, "wasde": W["prod"], "report": W.get("report"), "my": W.get("my"),
            "tx_prod": tx_f, "tx_prod_usda": c["tx_prod_usda"] / 1e6, "tx_planted": c["tx_planted"], "tx_ab_usda": c["tx_ab_usda"],
            "drought": drought, "ratings": ratings, "balance": bal, "sd_tx": SD_AVG}


def main():
    cond, prog = load.nass_weekly()
    cur = int(cond.year.max())
    states = ["US"] + [s for s in MAIN if s in set(cond.state)]
    dr = load.drought([s for s in states if s != "US"])

    res = {"generated": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"), "crop_year": cur}
    res["asof"] = {"conditions": cond.week_ending.max(), "drought": dr.week.max()}

    # ── conditions / progress / drought seasonal series ──
    res["conditions"] = {k: seasonal(cond, k, states, cur) for k in ("ge", "pvp", "ci")}
    stages = {}
    for st in load.STAGES:
        if st in prog:
            stages[st] = seasonal(prog.dropna(subset=[st]), st, states, cur)
    res["progress"] = stages
    res["drought"] = seasonal(dr, "d2", [s for s in states if s != "US"], cur)
    # US and Texas: share of COTTON area in D2–D4 (USDA/NDMC Ag in Drought) instead of whole-state area
    ap = os.path.join(load.D, "drought", "cotton_area_drought.csv")
    if os.path.exists(ap):
        ad = pd.read_csv(ap)
        ad["week"] = pd.to_datetime(ad.week)
        ad["year"] = ad.week.dt.year
        ad["woy"] = ad.week.dt.isocalendar().week.astype(int)
        ad = ad.rename(columns={"location": "state"})
        res["drought"].update(seasonal(ad, "d2", ["US", "TX"], cur))
        res["drought_cotton_area"] = ["US", "TX"]
        res["asof"]["drought"] = max(res["asof"]["drought"], ad.week.max())
    res["states"] = [{"st": s, "name": load.STATE_NAME.get(s, s)} for s in states]

    # ── models (crop estimate, ginning/classing pace, quality) ──
    est = model.run(cond, prog, dr, cur)
    res.update(est)
    res["wasde"] = wasde_us(cur)
    res["headline"] = headline_crop(res["wasde"], cur, cond)
    brief(res)
    os.makedirs(os.path.dirname(OUT), exist_ok=True)
    try:                                         # drought → abandonment regressions for the Production model tab
        dreg = model.drought_regressions(dr, load.annual(), cur)
        json.dump(clean(dreg), open(os.path.join(os.path.dirname(OUT), "drought_reg.json"), "w"), separators=(",", ":"))
    except Exception as e:
        import traceback; traceback.print_exc()
    json.dump(clean(res), open(OUT, "w"), separators=(",", ":"))
    print("wrote", OUT, os.path.getsize(OUT) // 1024, "KB")


if __name__ == "__main__":
    main()
