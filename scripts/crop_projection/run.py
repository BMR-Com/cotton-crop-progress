"""Weekly crop projection: features → leave-one-year-out backtest (for the ranges) → current-season projection
→ Texas abandonment blend → docs/data/crop_projection.json (numbers only).

Run from the repo root:  python scripts/crop_projection/run.py
Intermediate files go to .cache/crop_projection (not committed)."""
import os, sys, json, math, subprocess
from datetime import date
os.environ.setdefault("FIRST_YEAR", "2008")
os.environ.setdefault("GIN_CONVERT", "1")
HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
for step in ("build_features.py", "pheno.py"):
    subprocess.run([sys.executable, os.path.join(HERE, step)], check=True, env=os.environ.copy())

import numpy as np
import pandas as pd
from scipy import stats
import gin_data as gd
import model2
from model2 import (Wf, R, HIST, CUR, prep, attach_shares, fit_week, pred_week, run_week_backtest)

model2.DROUGHT_CUTOFF = True
OUT = os.path.join(gd.REPO, "docs", "data", "crop_projection.json")
A1 = ["lH_a", "lH14_a"]
RAT = "ratings+bloom"
GIN_FROM_K = 4                               # before October: ratings + drought only
load = gd.load
ann = load.annual()


def tmult(p, n):
    return stats.t.ppf(0.5 + p / 2, n - 1) * math.sqrt(1 + 1 / n)


# ── 1. backtest at every week (errors give the ranges) ──
V = {"nogin": {"rat": RAT}, "gin": {"rat": RAT, "gin": "model", "share_feats": A1}}
P = run_week_backtest(V, lam_share=5.0, lam_week=5.0)
bt = P["gin"].sort_index().copy()
ng = P["nogin"].sort_index()
early = bt.k < GIN_FROM_K
bt.loc[early, "yhat"] = ng.loc[bt.index[early], "yhat"].values

# ── 2. current-season projection by state at the latest week ──
trW = prep(HIST, Wf[Wf.year.isin(HIST)])
cur = prep(HIST, Wf[Wf.year == CUR])
kc = int(cur.k.max())
a = attach_shares(trW, HIST, A1, 5.0, True)
b = attach_shares(cur, HIST, A1, 5.0, False)
use_gin = kc >= GIN_FROM_K
m = fit_week(a[a.k == kc].dropna(subset=["y"]), RAT, use_gin, 5.0)
now = b[b.k == kc].copy()
now["yhat"] = pred_week(m, now, RAT, use_gin)
now["proj"] = now.P0 * np.exp(now.yhat)
asof_D = now.D.iloc[0]

# backtest errors at this week: US and by state (log)
e = bt[(bt.k == kc)].dropna(subset=["y"]).copy()
e["Ph"] = e.P0 * np.exp(e.yhat)
us_e = np.log(e.groupby("year").Ph.sum() / e.groupby("year").P.sum())
sd_us, n_us = float(np.sqrt(np.mean(us_e ** 2))), len(us_e)
st_e = (e.yhat - e.y).groupby(e.state)
sd_st = st_e.apply(lambda x: float(np.sqrt(np.mean(x ** 2)))).to_dict()
n_st = st_e.size().to_dict()

# ── 3. Texas: final abandonment from drought + ratings history, × USDA's in-season yield; blended with the model ──
cond, prog = load.nass_weekly()
dr = pd.read_csv(os.path.join(load.D, "drought", "cotton_area_drought.csv"))
dr["week"] = pd.to_datetime(dr.week)
pl = load.pick(ann, "upland_planted", final_only=True)
hv = load.pick(ann, "upland_harvested", final_only=True)
prod = load.pick(ann, "upland_prod", final_only=True)


def tx_feats(y, t):
    d = dr[(dr.location == "TX") & (dr.week >= pd.Timestamp(y, 5, 1)) & (dr.week <= min(t, pd.Timestamp(y, 10, 31)))]
    c = cond[(cond.year == y) & (cond.state == "TX") & (cond.week_ending <= t)].sort_values("week_ending")
    return {"d2": d.d2.mean(), "pvp_now": c.pvp.iloc[-1] if len(c) else np.nan}


def usda_latest(y, item, st, before=None):
    x = ann[(ann.year == y) & (ann.state == st) & (ann.item == item) & ann.period.str.contains("FORECAST|ACREAGE")].copy()
    if before is not None and "load_time" in x:
        x = x[pd.to_datetime(x.load_time) <= before]
    if not len(x):
        return np.nan
    return float(x.sort_values("load_time").value.iloc[-1]) if "load_time" in x else float(x.value.iloc[-1])


md = (asof_D.month, asof_D.day)
T = []
for y in range(2001, CUR + 1):
    t = pd.Timestamp(y, *md) - pd.Timedelta(days=1)
    f = tx_feats(y, t)
    planted = pl.get((y, "TX")) if y < CUR else usda_latest(y, "upland_planted", "TX")
    T.append({"year": y, **f, "planted": planted,
              "ab": 1 - hv.get((y, "TX"), np.nan) / pl.get((y, "TX")) if y < CUR and (y, "TX") in pl.index else np.nan,
              "P": prod.get((y, "TX"), np.nan) if y < CUR else np.nan,
              "uy": usda_latest(y, "upland_yield", "TX", before=pd.Timestamp(y, *md))})
T = pd.DataFrame(T).set_index("year")
h = T[T.index < CUR].dropna(subset=["ab", "d2", "pvp_now"])
X = lambda D: np.c_[np.ones(len(D)), D.d2, D.pvp_now]
ab_loo = pd.Series({y: float((X(h.loc[[y]]) @ np.linalg.lstsq(X(h.drop(y)), h.drop(y).ab, rcond=None)[0])[0]) for y in h.index})
bA = np.linalg.lstsq(X(h), h.ab, rcond=None)[0]
res = h.ab - X(h) @ bA
s2 = float(res @ res / (len(h) - 3))
x0 = np.r_[1, T.loc[CUR, ["d2", "pvp_now"]].values.astype(float)]
ab_now = float(np.clip(x0 @ bA, 0.02, 0.9))
se_ab = math.sqrt(s2 * (1 + x0 @ np.linalg.inv(X(h).T @ X(h)) @ x0))
ab_lo, ab_hi = max(0.0, ab_now - tmult(0.95, len(h)) / math.sqrt(1 + 1 / len(h)) * se_ab), min(0.95, ab_now + tmult(0.95, len(h)) / math.sqrt(1 + 1 / len(h)) * se_ab)
r2_ab = 1 - np.sum((h.ab - ab_loo) ** 2) / np.sum((h.ab - h.ab.mean()) ** 2)

tx_ab_model = T.planted * (1 - pd.concat([ab_loo, pd.Series({CUR: ab_now})])) * T.uy / 480
g_tx = e[e.state == "TX"].set_index("year")
tx_gin_hist = g_tx.P0 * np.exp(g_tx.yhat)
both = pd.DataFrame({"ab": tx_ab_model, "gin": tx_gin_hist, "P": T.P}).dropna()
use_blend = len(both) >= 6
if use_blend:
    eb = np.log(np.sqrt(both.ab * both.gin) / both.P)
    sd_tx, n_tx = float(np.sqrt(np.mean(eb ** 2))), len(eb)
    eg = np.log(both.gin / both.P)
    sd_tx_gin = float(np.sqrt(np.mean(eg ** 2)))
    if sd_tx_gin < sd_tx:                     # keep the blend only while it beats the model alone
        use_blend = False
tx_gin_now = float(now.loc[now.state == "TX", "proj"].iloc[0])
tx_ab_now = float(tx_ab_model.get(CUR, np.nan))
if use_blend and np.isfinite(tx_ab_now):
    tx_now = math.sqrt(tx_gin_now * tx_ab_now)
    sd_st["TX"], n_st["TX"] = sd_tx, n_tx
else:
    tx_now = tx_gin_now
now.loc[now.state == "TX", "proj"] = tx_now

# ── 4. assemble ──
def band(c, sd, n):
    o = {"central": round(c / 1e6, 3)}
    for p, k in ((0.68, "68"), (0.95, "95"), (0.99, "99")):
        w = tmult(p, n) * sd
        o["lo" + k], o["hi" + k] = round(c * math.exp(-w) / 1e6, 3), round(c * math.exp(w) / 1e6, 3)
    return o


us_up = float(now.proj.sum())
pima = usda_latest(CUR, "pima_prod", "US")
pima = 0.0 if not np.isfinite(pima) else pima
U = band(us_up, sd_us, n_us)
A = {k: round(v + pima / 1e6, 3) for k, v in U.items()}
states = []
for _, r in now.sort_values("proj", ascending=False).iterrows():
    s = r.state
    o = {"st": s, "name": load.STATE_NAME.get(s, s), **band(r.proj, sd_st.get(s, sd_us), n_st.get(s, n_us)),
         "usda": round(usda_latest(CUR, "upland_prod", s) / 1e6, 3) if np.isfinite(usda_latest(CUR, "upland_prod", s)) else None}
    states.append(o)
txs = next(o for o in states if o["st"] == "TX")

# Texas classing by office, latest report this season
regions = None
op = os.path.join(load.D, "classing", "office.csv")
if os.path.exists(op):
    o = pd.read_csv(op)
    o["d"] = pd.to_datetime(o.report_date)
    o["cy"] = np.where(o.d.dt.month >= 8, o.d.dt.year, o.d.dt.year - 1)
    oc = o[o.cy == CUR]
    if len(oc):
        last = oc[oc.d == oc.d.max()].set_index("office").season
        w = {k: last.get(k, 0) or 0 for k in ("Lubbock", "Lamesa", "Abilene")}
        cc = last.get("Corpus Christi", 0) or 0
        regions = (f"Classing by office to {oc.d.max():%b %d}: South Texas (Corpus Christi) {cc / 1e3:,.0f}k bales classed; "
                   f"West Texas (Lubbock {w['Lubbock'] / 1e3:,.0f}k, Lamesa {w['Lamesa'] / 1e3:,.0f}k, Abilene {w['Abilene'] / 1e3:,.0f}k) "
                   f"{sum(w.values()) / 1e3:,.0f}k. " + ("West Texas, where abandonment decides the crop, has barely started; it is projected from drought and ratings until its classing builds."
                                                         if sum(w.values()) < 0.25 * max(cc, 1) else "West Texas classing is now well under way."))

# FSA failed share, latest release
fsa_note = None
fp = os.path.join(load.D, "fsa", "fsa_cotton_state.csv")
if os.path.exists(fp):
    f = pd.read_csv(fp)
    f = f[f.type.str.contains("UPLAND") & ~f.type.str.contains("SEED") & (f.state.str.upper() == "TEXAS") & (f.crop_year == CUR)]
    if len(f):
        lr = f[f.release == f.release.max()]
        fs = lr.failed.sum() / (lr.planted.sum() + lr.failed.sum())
        fsa_note = (f"Texas FSA failed acres ({pd.Timestamp(lr.release.iloc[0]):%B} release) are {fs * 100:.1f}% of planted. Failed acres are reported to FSA "
                    f"with a lag and the December–January releases settle them, so the abandonment projection uses this season's drought and "
                    f"ratings readings against what similar readings meant for final abandonment in past seasons.")

tx_pl = usda_latest(CUR, "upland_planted", "TX")
tx_hv = usda_latest(CUR, "upland_harvested", "TX")
gin_last = R[R.year == CUR].ref_date.max()
out = {
    "asof": str(date.today()),
    "inputs_through": {"ginnings": f"{gin_last:%Y-%m-%d}" if pd.notna(gin_last) else None,
                       "crop_progress": f"{cond.week_ending.max():%Y-%m-%d}",
                       "drought": f"{dr.week.max():%Y-%m-%d}",
                       "classing_by_office": f"{oc.d.max():%Y-%m-%d}" if regions else None},
    "unit": "million 480-lb bales",
    "us_upland": U, "us_all": A, "pima_usda": round(pima / 1e6, 3),
    "texas": {**{k: v for k, v in txs.items() if k not in ("st", "name", "usda")},
              "abandonment": {"central": round(ab_now, 3), "lo95": round(ab_lo, 3), "hi95": round(ab_hi, 3)}},
    "states": states,
    "usda": {"report": pd.Timestamp(ann[(ann.year == CUR) & (ann.item == "upland_prod") & (ann.state == "US")].load_time.max()).strftime("%B %Y"),
             "us_upland": round(usda_latest(CUR, "upland_prod", "US") / 1e6, 3),
             "us_all": round(usda_latest(CUR, "all_prod", "US") / 1e6, 3),
             "tx": round(usda_latest(CUR, "upland_prod", "TX") / 1e6, 3),
             "tx_abandonment": round(1 - tx_hv / tx_pl, 3) if np.isfinite(tx_hv) and np.isfinite(tx_pl) else None},
    "texas_regions": regions,
    "notes": [fsa_note] if fsa_note else [],
    "variables": ["NASS crop progress by state: squaring, setting bolls, bolls opening, harvested",
                  "NASS crop condition ratings by state",
                  "US Drought Monitor, share of cotton area in D2–D4, weighted by crop stage, through October",
                  "NASS Cotton Ginnings by state (running bales, converted to 480-lb bales of the state where grown)",
                  "USDA AMS bales classed by classing office (Texas split: Lubbock, Lamesa, Abilene vs Corpus Christi)",
                  "USDA FSA planted and failed acres", "NASS planted acres",
                  "Target: NASS final upland production (May annual report)"],
}
json.dump(out, open(OUT, "w"), indent=1)
# private diagnostics (not published): written to the cache only
json.dump({"week": kc, "monday": str(asof_D.date()), "sd_us": sd_us, "n_us": n_us, "sd_state": sd_st, "tx_gin": tx_gin_now, "tx_ab_model": tx_ab_now,
           "tx_blend_used": use_blend, "ab_r2_loo": r2_ab, "us_errors": us_e.round(4).to_dict()},
          open(os.path.join(gd.CACHE, "diagnostics.json"), "w"), indent=1, default=float)
print(f"US upland {U['central']:.2f}m (95% {U['lo95']:.2f}-{U['hi95']:.2f}), all {A['central']:.2f}m, Texas {txs['central']:.2f}m, abandonment {ab_now:.1%}")
