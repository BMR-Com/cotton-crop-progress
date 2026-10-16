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
    head = (f"We estimate the {cur} US upland crop at {m(us.get('prod'))} bales, "
            f"{abs(d):.1f}% {'above' if d > 0 else 'below'} USDA's {m(us.get('usda_prod'))}." if d is not None else "Crop estimate pending data.")
    kpis.append({"label": "Upland crop — our estimate", "value": m(us.get("prod")), "delta": f"{d:+.1f}%" if d is not None else None,
                 "good": (d or 0) >= 0, "sub": f"USDA {m(us.get('usda_prod'))} 480-lb bales"})
    if us.get("harv") and us.get("usda_harv"):
        dh = (us["harv"] / us["usda_harv"] - 1) * 100
        kpis.append({"label": "Harvested acres", "value": f"{us['harv'] / 1e6:.2f}m", "delta": f"{dh:+.1f}%", "good": dh >= 0,
                     "sub": f"USDA {us['usda_harv'] / 1e6:.2f}m · planted {us['planted'] / 1e6:.2f}m"})
    if us.get("yield") and us.get("usda_yield"):
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
        t = (f"Texas: {tx['harv_ratio'] * 100:.0f}% of planted acres expected to be harvested"
             + (f" (FSA failed acres {tx['fail_share'] * 100:.1f}% of reported plantings)" if tx.get("fail_share") is not None else "")
             + f"; yield {tx.get('yield_implied') or tx.get('yield_cond'):.0f} lb vs USDA {tx.get('usda_yield') or 0:.0f}"
             + (f"; {txd[1]:.0f}% of the state in D2–D4 drought." if txd else "."))
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
    for extra in res.get("brief_extra", []):
        pts.append(extra)
    for k in res.get("kpis_extra", []):
        kpis.append(k)
    res["brief"] = {"date": str(res["asof"]["conditions"])[:10], "headline": head, "points": pts}
    res["kpis"] = kpis


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
    res["states"] = [{"st": s, "name": load.STATE_NAME.get(s, s)} for s in states]

    # ── models (crop estimate, ginning/classing pace, quality) ──
    est = model.run(cond, prog, dr, cur)
    res.update(est)
    brief(res)
    os.makedirs(os.path.dirname(OUT), exist_ok=True)
    json.dump(clean(res), open(OUT, "w"), separators=(",", ":"))
    print("wrote", OUT, os.path.getsize(OUT) // 1024, "KB")


if __name__ == "__main__":
    main()
