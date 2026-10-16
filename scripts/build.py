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
        kpis.insert(1, {"label": "Projection from ginnings", "value": m(u["proj"]), "delta": f"{dg:+.1f}%" if dg is not None else None,
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
                   ("harvested", "Harvested"), ("yield", "Yield per Harvested Acre")):
        v = d["data"].get(f"United States||{lab}||{my}")
        if v:
            out[k] = v[-1]["value"]
            out["report"] = v[-1]["reportDate"]
    return out


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
