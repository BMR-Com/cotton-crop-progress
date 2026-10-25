"""Assemble the state-year panel for the ginnings → final crop backtest."""
import os, sys
import numpy as np
import pandas as pd

REPO = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
CACHE = os.environ.get("CROP_CACHE", os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), ".cache", "crop_projection"))
os.makedirs(CACHE, exist_ok=True)
sys.path.insert(0, os.path.join(REPO, "scripts"))
import load  # noqa

PIMA = {"AZ", "CA", "NM", "TX"}
GIN_PERIODS = ["FIRST OF SEP", "MID SEP", "FIRST OF OCT", "MID OCT", "FIRST OF NOV", "MID NOV",
               "FIRST OF DEC", "MID DEC", "FIRST OF JAN", "MID JAN"]
RELEASE_LAG = 8          # days from ginning reference date to NASS release


def ginnings():
    g = pd.read_csv(os.path.join(load.D, "ginning", "ginnings.csv"))
    ep = os.path.join(load.D, "ginning", "ginnings_esmis.csv")
    if os.path.exists(ep):                      # NASS report archive fills crop years QuickStats lacks
        e = pd.read_csv(ep)
        e = e[~((e.item == "pima_rb") & (e.value < 10))]          # footnote markers, not bales
        have = set(g.year.unique())
        g = pd.concat([g, e[~e.year.isin(have)].drop(columns=["release"], errors="ignore")], ignore_index=True)
    g = g[g.period.isin(GIN_PERIODS) & g.ref_date.notna()].copy()
    g["ref_date"] = pd.to_datetime(g.ref_date)
    w = g.pivot_table(index=["year", "state", "period", "ref_date"], columns="item", values="value", aggfunc="max").reset_index()
    up = w.get("upland_rb")
    alt = w["all_rb"] - w["pima_rb"].fillna(0) if "pima_rb" in w else w["all_rb"]
    w["G"] = up.fillna(alt) if up is not None else alt
    w = w[["year", "state", "period", "ref_date", "G"]]
    # a state missing from a report that the US row covers had nothing ginned yet
    rep = w[w.state == "US"][["year", "period", "ref_date"]]
    sts = sorted(set(w.state) - {"US"})
    full = rep.assign(k=1).merge(pd.DataFrame({"state": sts, "k": 1}), on="k").drop(columns="k")
    w = full.merge(w[w.state != "US"], on=["year", "state", "period", "ref_date"], how="left")
    # missing before anything was ginned → 0; missing after → not reported (drop, previous report stands)
    w = w.sort_values(["year", "state", "ref_date"])
    started = w.groupby(["year", "state"]).G.transform(lambda s: (s.fillna(0) > 0).cummax())
    w.loc[w.G.isna() & ~started, "G"] = 0.0
    return w.dropna(subset=["G"])


def curve_at(df, col, year, state, t):
    """Progress % at date t, linear between weekly reports; 0 before the stage starts, last value after."""
    h = df[(df.year == year) & (df.state == state)].dropna(subset=[col]).sort_values("week_ending")
    if not len(h):
        return np.nan
    xs = (h.week_ending - pd.Timestamp(year, 1, 1)).dt.days.values.astype(float)
    ys = h[col].values.astype(float)
    if ys[0] > 0:                                    # stage started ~1 week before first report
        xs, ys = np.r_[xs[0] - 7, xs], np.r_[0.0, ys]
    x = (pd.Timestamp(t) - pd.Timestamp(year, 1, 1)).days
    if x > xs[-1]:
        # NASS stops reporting a stage when nearly done; carry forward toward 100 at ~3 pts/day
        return float(min(100.0, ys[-1] + max(0.0, x - xs[-1]) * 3.0 * (ys[-1] >= 85)))
    return float(np.interp(x, xs, ys, left=0.0))


def panel():
    cond, prog = load.nass_weekly()
    ann = load.annual()
    prod = load.pick(ann, "upland_prod", final_only=True)
    pl = load.pick(ann, "upland_planted", final_only=True)
    hv = load.pick(ann, "upland_harvested", final_only=True)
    a = pd.read_csv(os.path.join(load.D, "drought", "cotton_area_drought.csv"))
    a["week"] = pd.to_datetime(a.week)
    a = a.rename(columns={"location": "state"})
    a["year"] = a.week.dt.year
    return cond, prog, ann, prod, pl, hv, a


def usda_forecasts(ann):
    """In-season NASS state forecasts with their release dates (Aug..Dec, Jan)."""
    x = ann[(ann.item == "upland_prod") & ann.period.str.contains("FORECAST")].copy()
    x["mon"] = x.period.str.extract(r"- (\w{3}) FORECAST")[0]
    mm = {"AUG": 8, "SEP": 9, "OCT": 10, "NOV": 11, "DEC": 12, "JAN": 1}
    x["m"] = x.mon.map(mm)
    x["rel"] = [pd.Timestamp(y + (m == 1), m, 12) for y, m in zip(x.year, x.m)]   # crop report ~ 9th–12th
    return x[["year", "state", "rel", "value"]]
