"""Stage-weighted drought exposure from NASS stage progress.

For each drought week, the share of a state's crop in each phase:
  squaring  = SQUARING − SETTING BOLLS        (fruiting sites set — quantity)
  bloom/set = SETTING BOLLS − BOLLS OPENING   (flowering → boll fill — the most yield-critical window)
  open      = BOLLS OPENING − HARVESTED       (crop made — quality/harvest window)
Exposure = drought (D2+ % of cotton area) averaged over weeks, weighted by the phase share, using only
weeks up to the as-of date and never past October 31."""
import os, sys
import numpy as np
import pandas as pd
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import gin_data as gd

cond, prog, ann, prod, pl, hv, dr = gd.panel()
STG = {"SQUARING": "sq", "SETTING BOLLS": "sb", "BOLLS OPENING": "bo", "HARVESTED": "hv"}
NEIGH = {"FL": "GA"}


def curves(y, s):
    p = prog[(prog.year == y) & (prog.state == NEIGH.get(s, s))].sort_values("week_ending")
    out = {}
    for col, k in STG.items():
        h = p.dropna(subset=[col]) if col in p else p.iloc[0:0]
        if not len(h):
            out[k] = None
            continue
        xs = (h.week_ending - pd.Timestamp(y, 1, 1)).dt.days.values.astype(float)
        ys = h[col].values.astype(float)
        if ys[0] > 0:
            xs, ys = np.r_[xs[0] - 7, xs], np.r_[0.0, ys]
        if ys[-1] >= 80:                                   # reporting stops near completion
            xs, ys = np.r_[xs, xs[-1] + 14], np.r_[ys, 100.0]
        out[k] = (xs, ys)
    return out


def at(c, x):
    if c is None:
        return np.zeros_like(x)
    return np.interp(x, c[0], c[1], left=0.0, right=c[1][-1])


def exposures(y, s, dates):
    c = curves(y, s)
    d = dr[(dr.state == s) & (dr.week >= pd.Timestamp(y, 5, 15)) & (dr.week <= pd.Timestamp(y, 10, 31))].sort_values("week")
    res = []
    if not len(d) or c["sb"] is None:
        return pd.DataFrame([{} for _ in dates], index=range(len(dates)))
    x = (d.week - pd.Timestamp(y, 1, 1)).dt.days.values.astype(float)
    sq, sb, bo, hvv = (at(c[k], x) / 100 for k in ("sq", "sb", "bo", "hv"))
    ph = {"sq": np.clip(sq - sb, 0, 1), "bl": np.clip(sb - bo, 0, 1), "op": np.clip(bo - hvv, 0, 1)}
    for t in dates:
        m = d.week.values <= np.datetime64(pd.Timestamp(t))
        r = {}
        for k, f in ph.items():
            w = f[m]
            for lvl in ("d1", "d2"):
                v = d[lvl].values[m]
                r[f"{lvl}_{k}"] = float(np.sum(w * v) / np.sum(w)) if np.sum(w) > 0.05 else np.nan
            r[f"wk_{k}"] = float(np.sum(w))                    # crop-weeks spent in phase so far
        res.append(r)
    return pd.DataFrame(res)


if __name__ == "__main__":
    here = gd.CACHE
    for fn, key, shift in (("ref_rows.pkl", "ref_date", 0), ("week_rows.pkl", "D", 1)):
        T = pd.read_pickle(os.path.join(here, fn))
        parts = []
        for (y, s), g in T.groupby(["year", "state"]):
            e = exposures(y, s, list(g[key] - pd.Timedelta(days=shift)))
            e.index = g.index
            parts.append(e)
        E = pd.concat(parts)
        T = T.drop(columns=[c for c in E.columns if c in T.columns]).join(E)
        T.to_pickle(os.path.join(here, fn))
        print(fn, E.notna().mean().round(2).to_dict())
    W = pd.read_pickle(os.path.join(here, "week_rows.pkl"))
    print(W[(W.state == "TX") & (W.k == 8)][["year", "d2", "d2_sq", "d2_bl", "d2_op", "P", "planted", "trend_ypa"]].round(1).to_string())
