import os, sys
import numpy as np
import pandas as pd
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import gin_data as gd

OUT = gd.CACHE
TODAY = pd.Timestamp.today().normalize()

cond, prog, ann, prod, pl, hv, dr = gd.panel()
CUR = int(prog.year.max())
YEARS = list(range(int(os.environ.get("FIRST_YEAR", "2008")), CUR + 1))
gin = gd.ginnings()
gin = gin[gin.year.isin(YEARS)]
states = sorted(set(gin.state))

# trend production per planted acre (lb), prior five finals only
ypa = (prod * 480 / pl).dropna()
trend = {}
for s in states:
    for y in YEARS:
        v = [ypa.get((yy, s)) for yy in range(y - 5, y) if (yy, s) in ypa.index]
        v = [x for x in v if x and np.isfinite(x)]
        if len(v) >= 3:
            trend[(y, s)] = float(np.mean(v))


def cond_at(y, s, t):
    c = cond[(cond.year == y) & (cond.state == s) & (cond.week_ending <= t)].sort_values("week_ending")
    if not len(c):
        return {}
    cs = c[c.woy >= 26]
    last = c.iloc[-1]
    return {"ci_now": last.ci, "pvp_now": last.pvp, "ge_now": last.ge,
            "ci_avg": cs.ci.mean() if len(cs) else np.nan, "pvp_avg": cs.pvp.mean() if len(cs) else np.nan,
            "ci_min": cs.ci.min() if len(cs) else np.nan}


def drought_at(y, s, t):
    d = dr[(dr.state == s) & (dr.week >= pd.Timestamp(y, 6, 1)) & (dr.week <= min(pd.Timestamp(t), pd.Timestamp(y, 10, 31)))]
    if not len(d):
        return {}
    return {"d2": d.d2.mean(), "d1": d.d1.mean(), "d3": d.d3.mean(),
            "d2_late": d[d.week >= pd.Timestamp(y, 8, 1)].d2.mean() if (d.week >= pd.Timestamp(y, 8, 1)).any() else d.d2.mean()}


def feats(y, s, t):
    t = pd.Timestamp(t)
    f = {"H": gd.curve_at(prog, "HARVESTED", y, s, t),
         "H_l14": gd.curve_at(prog, "HARVESTED", y, s, t - pd.Timedelta(days=14)),
         "BO": gd.curve_at(prog, "BOLLS OPENING", y, s, t),
         "doy": (t - pd.Timestamp(y, 8, 1)).days}
    f.update(cond_at(y, s, t))
    f.update(drought_at(y, s, t))
    return f


# ── rows at ginning reference dates (share model) ──
rows = []
for r in gin.itertuples():
    if r.state not in states:
        continue
    f = feats(r.year, r.state, r.ref_date)
    f.update({"year": r.year, "state": r.state, "period": r.period, "ref_date": r.ref_date, "G": r.G,
              "P": prod.get((r.year, r.state)) if r.year < CUR else np.nan,
              "planted": pl.get((r.year, r.state)), "trend_ypa": trend.get((r.year, r.state))})
    rows.append(f)
R = pd.DataFrame(rows)
R.to_pickle(os.path.join(OUT, "ref_rows.pkl"))
print("ref rows", R.shape)

# ── weekly grid (what is known each Monday) ──
fc = gd.usda_forecasts(ann)
W = []
for y in YEARS:
    d0 = pd.Timestamp(y, 9, 1)
    d0 = d0 + pd.Timedelta(days=(7 - d0.weekday()) % 7)          # first Monday on/after Sep 1
    for k in range(0, 20):
        D = d0 + pd.Timedelta(days=7 * k)
        if y == CUR and D > TODAY + pd.Timedelta(days=1):
            break
        gy = gin[(gin.year == y) & (gin.ref_date + pd.Timedelta(days=gd.RELEASE_LAG) <= D)]
        ref = gy.ref_date.max() if len(gy) else pd.NaT
        for s in states:
            if (y, s) not in trend:
                continue
            f = feats(y, s, D - pd.Timedelta(days=1))
            g = gy[gy.state == s].sort_values("ref_date") if len(gy) else gy
            u = fc[(fc.year == y) & (fc.state == s) & (fc.rel <= D)].sort_values("rel")
            f.update({"year": y, "state": s, "k": k, "D": D, "ref_date": g.ref_date.iloc[-1] if len(g) else pd.NaT,
                      "G": float(g.G.iloc[-1]) if len(g) else 0.0,
                      "P": prod.get((y, s)) if y < CUR else np.nan,
                      "usda": float(u.value.iloc[-1]) if len(u) else np.nan,
                      "planted": pl.get((y, s)), "trend_ypa": trend[(y, s)]})
            W.append(f)
W = pd.DataFrame(W)
W.to_pickle(os.path.join(OUT, "week_rows.pkl"))
print("week rows", W.shape)
print(W[(W.year == CUR) & (W.state == "TX")][["D", "ref_date", "G", "H", "BO", "ci_now", "d2", "usda"]].tail(3))
