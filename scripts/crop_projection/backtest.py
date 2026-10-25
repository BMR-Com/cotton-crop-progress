"""Leave-one-year-out backtest: ginnings → final upland crop, with harvest stage, ratings and drought."""
import os, sys, json
import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import gin_data as gd
HERE = gd.CACHE
R = pd.read_pickle(os.path.join(HERE, "ref_rows.pkl"))
W = pd.read_pickle(os.path.join(HERE, "week_rows.pkl"))
CUR = int(W.year.max())
HIST = list(range(int(os.environ.get("FIRST_YEAR", "2008")), CUR))


def logit(p):
    p = np.clip(np.asarray(p, float) / 100.0, 0.01, 0.99)
    return np.log(p / (1 - p))


def wls(X, y, w, ridge=1e-6):
    sw = np.sqrt(w)[:, None]
    A = X * sw
    b = np.linalg.solve(A.T @ A + ridge * np.eye(X.shape[1]), A.T @ (y * sw[:, 0]))
    return b


def r2(y, yh, w=None):
    y, yh = np.asarray(y), np.asarray(yh)
    w = np.ones_like(y) if w is None else np.asarray(w)
    m = np.average(y, weights=w)
    return 1 - np.sum(w * (y - yh) ** 2) / np.sum(w * (y - m) ** 2)


# ───────── prep ─────────
NEIGH = {"FL": "GA"}                  # states without NASS progress/condition reports borrow a neighbour's
FILL = ["H", "H_l14", "BO", "ci_now", "pvp_now", "ge_now", "ci_avg", "pvp_avg", "ci_min"]


def neigh_fill(D, key):
    D = D.copy()
    for s, nb in NEIGH.items():
        src = D[D.state == nb].set_index(["year", key])[FILL]
        idx = D.state == s
        tgt = D.loc[idx].set_index(["year", key])
        D.loc[idx, FILL] = tgt[FILL].fillna(src.reindex(tgt.index)).values
    return D


R, W = neigh_fill(R, "ref_date"), neigh_fill(W, "D")
for D in (R, W):
    D["P0"] = D.planted * D.trend_ypa / 480.0
    D["lH"], D["lH14"], D["lBO"] = logit(D.H), logit(D.H_l14), logit(D.BO)
    for c in ("d2", "d1", "d3", "d2_late"):
        D[c] = D[c].fillna(0.0)
    D["ci_now"] = D.ci_now.fillna(D.ci_avg)

R = R.dropna(subset=["P0"])
R["share"] = R.G / R.P
R["Pfin"] = R.P


def state_means(tr, cols):
    return tr.groupby("state")[cols].mean()


# ───────── share models (ginned to date / final) ─────────
SHARE_FEATS = {
    "S2 harvest stage": ["lH", "lH14", "lBO", "doy"],
    "S3 + ratings": ["lH", "lH14", "lBO", "doy", "ci_dev", "pvp_dev"],
    "S4 + ratings + drought": ["lH", "lH14", "lBO", "doy", "ci_dev", "pvp_dev", "d2"],
}


def add_dev(D, means):
    D = D.copy()
    m = means.reindex(D.state)
    D["ci_dev"] = D.ci_now.values - m.ci_now.values
    D["ciavg_dev"] = D.ci_avg.values - m.ci_avg.values
    D["pvp_dev"] = D.pvp_now.values - m.pvp_now.values
    D["d2_dev"] = D.d2.values - m.d2.values
    for c in ("ci_dev", "ciavg_dev", "pvp_dev", "d2_dev"):
        D[c] = D[c].fillna(0.0)
    return D


def fit_share(tr, feats, state_fe=True):
    t = tr[(tr.G > 0) & (tr.share > 0.003) & (tr.share < 1.0)]
    y = logit(t.share * 100)
    sts = sorted(t.state.unique())
    X = [np.ones(len(t))] + [t[f].values for f in feats]
    if state_fe:
        X += [(t.state == s).values.astype(float) for s in sts[1:]]
    X = np.c_[tuple(X)] if False else np.column_stack(X)
    b = wls(X, y, t.P0.values, ridge=1e-3)
    resid = y - X @ b
    return {"b": b, "feats": feats, "sts": sts, "fe": state_fe, "sd": float(np.sqrt(np.average(resid ** 2, weights=t.P0)))}


def pred_share(m, D):
    X = [np.ones(len(D))] + [D[f].values for f in m["feats"]]
    if m["fe"]:
        X += [(D.state == s).values.astype(float) for s in m["sts"][1:]]
    z = np.column_stack(X) @ m["b"]
    return 1 / (1 + np.exp(-z))


def share_calendar(tr, D):
    """Repo baseline: mean share at the same report (period) in other years, by state."""
    m = tr.groupby(["state", "period"]).share.mean()
    return np.array([m.get((s, p), np.nan) for s, p in zip(D.state, D.period)])


def share_harvest_interp(tr, D):
    """Share ginned at the same harvested % in other years (median across years), by state."""
    out = []
    for s, h in zip(D.state, D.H):
        v = []
        for y, g in tr[tr.state == s].groupby("year"):
            g = g.sort_values("ref_date")
            hh = np.maximum.accumulate(g.H.values)
            if np.isnan(h) or hh.max() < h or len(g) < 3:
                continue
            v.append(np.interp(h, hh, g.share.values))
        out.append(np.median(v) if len(v) >= 3 else np.nan)
    return np.array(out)


def eval_share():
    """Pure ginning projection error by report, LOYO."""
    res = {}
    preds = {}
    for y in HIST:
        tr, te = R[(R.year != y) & R.year.isin(HIST)], R[R.year == y]
        mm = state_means(tr, ["ci_now", "ci_avg", "pvp_now", "d2"])
        tr, te = add_dev(tr, mm), add_dev(te, mm)
        p = {"S0 calendar (current)": share_calendar(tr, te), "S1 harvest-% interp": share_harvest_interp(tr, te)}
        for k, f in SHARE_FEATS.items():
            p[k] = pred_share(fit_share(tr, f), te)
        for k, v in p.items():
            preds.setdefault(k, []).append(pd.Series(v, index=te.index))
    out = R[R.year.isin(HIST)].copy()
    rows = []
    for k, v in preds.items():
        out[k] = pd.concat(v)
    for per in ["FIRST OF SEP", "MID SEP", "FIRST OF OCT", "MID OCT", "FIRST OF NOV", "MID NOV", "FIRST OF DEC", "MID DEC", "FIRST OF JAN"]:
        o = out[(out.period == per) & (out.G > 0)]
        row = {"period": per, "n": len(o), "true_share_US": o.G.sum() / o.P.sum()}
        for k in preds:
            s = o[k].clip(lower=0.02)
            e = np.log(o.G / s / o.P)
            ok = e.notna() & np.isfinite(e)
            row[k] = float(np.sqrt(np.average(e[ok] ** 2, weights=o.P0[ok]))) if ok.sum() else np.nan
            row[k + " cov"] = ok.mean()
        rows.append(row)
    return pd.DataFrame(rows), out


if __name__ == "__main__":
    pd.set_option("display.width", 220)
    tab, _ = eval_share()
    print("Pure ginning projection — state-level RMSE of log(projected/final), production-weighted, LOYO 2014–2025")
    print(tab.round(3).to_string(index=False))
