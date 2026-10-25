"""Weekly backtest of the full ginnings model.

Stage 1 — share ginned: logit(ginned/final) per state from harvest progress (now and 2 weeks earlier),
bolls open, date and crop condition (+ drought), state slopes shrunk toward the pooled slopes.
Stage 2 — weekly combiner: log(final / trend crop) from the ginning-implied crop (weighted by how much
of the crop is ginned) plus ratings and drought. Every year is predicted from the other years only
(nested for stage 1 inside stage 2)."""
import os, sys
import numpy as np
import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
from backtest import R, W, logit, wls, r2, add_dev, state_means, share_calendar, HIST, CUR  # noqa

R = R.copy()
W = W.copy()


def conv_factors():
    """Running bales ginned in a state → that state's NASS production (480-lb, where grown).
    Trailing 3-year mean of final production / final upland running bales: covers bale weight and
    cotton ginned across state lines (e.g. Arkansas gins handling Missouri/Mississippi cotton)."""
    import gin_data as gd
    g = pd.read_csv(os.path.join(gd.load.D, "ginning", "ginnings.csv"))
    u = g[g.item == "upland_rb"].groupby(["year", "state"]).value.max()
    a = g[g.item == "all_rb"].groupby(["year", "state"]).value.max()
    pm = g[g.item == "pima_rb"].groupby(["year", "state"]).value.max()
    fin = u.combine_first(a.sub(pm, fill_value=0))
    prod = gd.load.pick(gd.load.annual(), "upland_prod", final_only=True)
    r = (prod / fin).dropna()
    r = r[(r > 0.6) & (r < 1.6)]
    k = {}
    for (y, s) in set(zip(W.year, W.state)) | set(zip(R.year, R.state)):
        v = [r.get((yy, s)) for yy in range(y - 3, y) if (yy, s) in r.index]
        k[(y, s)] = float(np.mean(v)) if v else float(np.mean([r.get((yy, "US"), 1.03) for yy in range(y - 3, y)] or [1.03]))
    return k


CONVERT = os.environ.get("GIN_CONVERT", "0") == "1"
if CONVERT:
    _k = conv_factors()
    for D_ in (R, W):
        D_["G"] = D_.G.values * np.array([_k[(y, s)] for y, s in zip(D_.year, D_.state)])
    print("ginnings converted to state 480-lb production equivalent")
R["share"] = R.G / R.P


SLOPE_STATES = None

# ───────── hierarchical ridge: pooled slopes + state deviations ─────────
def hfit(df, ycol, feats, wcol="P0", lam=5.0, lam0=1e-3, state_slopes=True):
    X = np.column_stack([np.ones(len(df))] + [df[f].values for f in feats])
    y, w = df[ycol].values, df[wcol].values
    b0 = wls(X, y, w, ridge=lam0)
    bs = {}
    if state_slopes:
        for s, idx in df.groupby("state").indices.items():
            if SLOPE_STATES is not None and s not in SLOPE_STATES:
                continue
            Xs, ys, ws = X[idx], y[idx], w[idx]
            sw = np.sqrt(ws / ws.mean())[:, None]
            A = Xs * sw
            k = X.shape[1]
            P = lam * np.eye(k)
            bs[s] = np.linalg.solve(A.T @ A + P, A.T @ (ys * sw[:, 0]) + P @ b0)
    return {"b0": b0, "bs": bs, "feats": feats}


def hpred(m, df):
    X = np.column_stack([np.ones(len(df))] + [df[f].values for f in m["feats"]])
    out = np.empty(len(df))
    for i, s in enumerate(df.state.values):
        out[i] = X[i] @ m["bs"].get(s, m["b0"])
    return out


SHARE_SETS = {
    "harvest": ["lH", "lH14", "lBO", "doy"],
    "harvest+ratings": ["lH", "lH14", "lBO", "doy", "ci_dev", "pvp_dev"],
    "harvest+ratings+drought": ["lH", "lH14", "lBO", "doy", "ci_dev", "pvp_dev", "d2_dev"],
}


def share_train(tr, feats, lam):
    t = tr[(tr.G > 0) & (tr.share > 0.003) & (tr.share < 1.0)].copy()
    t["ls"] = logit(t.share * 100)
    return hfit(t, "ls", feats, lam=lam)


def share_predict(m, df):
    return 1 / (1 + np.exp(-hpred(m, df)))


def prep(tr_years, df):
    mm = state_means(R[R.year.isin(tr_years)], ["ci_now", "ci_avg", "pvp_now", "d2"])
    return add_dev(df, mm)


# ───────── stage 1 alone: pure ginning projection error by report ─────────
def stage1_table(lams=(0.5, 2, 5, 20, 1e6)):
    rows = []
    res = {}
    for name, feats in SHARE_SETS.items():
        for lam in lams:
            pr = []
            for y in HIST:
                tr_y = [v for v in HIST if v != y]
                tr, te = prep(tr_y, R[R.year.isin(tr_y)]), prep(tr_y, R[R.year == y])
                te = te.assign(sh=share_predict(share_train(tr, feats, lam), te))
                pr.append(te)
            o = pd.concat(pr)
            res[(name, lam)] = o
    for per in ["FIRST OF OCT", "MID OCT", "FIRST OF NOV", "MID NOV", "FIRST OF DEC", "MID DEC", "FIRST OF JAN"]:
        row = {"period": per}
        base = R[(R.period == per) & R.year.isin(HIST) & (R.G > 0)]
        cal = []
        for y in HIST:
            tr_y = [v for v in HIST if v != y]
            te = base[base.year == y]
            cal.append(pd.Series(share_calendar(R[R.year.isin(tr_y)], te), index=te.index))
        cal = pd.concat(cal)
        e = np.log(base.G / cal / base.P)
        row["calendar(current)"] = np.sqrt(np.average(e ** 2, weights=base.P0))
        for (name, lam), o in res.items():
            o = o[(o.period == per) & (o.G > 0)]
            e = np.log(o.G / o.sh / o.P)
            row[f"{name}|λ={lam:g}"] = np.sqrt(np.average(e ** 2, weights=o.P0))
        rows.append(row)
    return pd.DataFrame(rows).set_index("period")


if __name__ == "__main__":
    pd.set_option("display.width", 250)
    t = stage1_table()
    print(t.T.round(3).to_string())


# ───────── stage 2: weekly evaluation ─────────
def ref_feats(df_w):
    """Attach stage-1 features measured at each week's latest ginning reference date."""
    cols = ["lH", "lH14", "lBO", "doy", "ci_now", "pvp_now", "ci_avg", "d2", "period"]
    m = R[["year", "state", "ref_date"] + cols].rename(columns={c: "r_" + c for c in cols})
    return df_w.merge(m, on=["year", "state", "ref_date"], how="left")


Wf = ref_feats(W)
Wf = Wf[Wf.year.isin(HIST + [CUR])].dropna(subset=["P0"]).reset_index(drop=True)
Wf["y"] = np.log(Wf.P / Wf.P0)


def as_ref(df):
    """View of week rows with ginning-date features under stage-1 names."""
    d = df.copy()
    for c in ["lH", "lH14", "lBO", "doy", "ci_now", "pvp_now", "ci_avg", "d2", "period"]:
        d[c] = d["r_" + c]
    return d


def gin_share_for(rows, train_years, feats, lam):
    tr = prep(train_years, R[R.year.isin(train_years)])
    m = share_train(tr, feats, lam)
    d = prep(train_years, as_ref(rows))
    sh = np.where(rows.G.values > 0, share_predict(m, d.fillna({"lH": -4.6, "lH14": -4.6, "lBO": -4.6})), 0.0)
    return sh


def cal_share_for(rows, train_years):
    tr = R[R.year.isin(train_years)]
    d = as_ref(rows)
    s = share_calendar(tr, d)
    return np.where(rows.G.values > 0, s, 0.0)


RAT = {"none": [], "ratings": ["ci_dev", "ciavg_dev", "pvp_dev"], "ratings+drought": ["ci_dev", "ciavg_dev", "pvp_dev", "d2_dev"]}


def design(d, rat, use_gin):
    d = d.copy()
    feats = list(RAT[rat])
    if use_gin:
        ok = (d.G > 0) & (d.sh > 0.005)
        z = np.where(ok, np.log(np.maximum(d.G, 1) / np.maximum(d.sh, 0.005) / d.P0), 0.0)
        s = np.where(ok, d.sh, 0.0)
        d["z1"], d["z2"], d["z3"] = z * s, z * s * s, z * np.sqrt(s)
        feats += ["z3", "z1", "z2"]
    return d, feats


def fit_week(tr, rat, use_gin, lam):
    d, feats = design(tr, rat, use_gin)
    if not feats:
        return {"b0": np.array([np.average(d.y, weights=d.P0)]), "bs": {}, "feats": []}
    return hfit(d, "y", feats, lam=lam)


def pred_week(m, te, rat, use_gin):
    d, _ = design(te, rat, use_gin)
    return hpred(m, d)


def attach_shares(rows, train_years, feats, lam, nested):
    """Stage-1 share for each row; training rows get it from a model that excludes their own year."""
    rows = rows.copy()
    if nested:
        parts = []
        for v in sorted(rows.year.unique()):
            ry = rows[rows.year == v]
            ty = [t for t in train_years if t != v]
            parts.append(pd.Series(gin_share_for(ry, ty, feats, lam), index=ry.index))
        rows["sh"] = pd.concat(parts)
    else:
        rows["sh"] = gin_share_for(rows, train_years, feats, lam)
    return rows


def run_week_backtest(variants, lam_share=2.0, lam_week=5.0):
    """variants: name -> dict(rat=..., gin='model'|'calendar'|None, share_feats=...)."""
    preds = {k: [] for k in variants}
    for y in HIST:
        ty = [v for v in HIST if v != y]
        trW = prep(ty, Wf[Wf.year.isin(ty)])
        teW = prep(ty, Wf[Wf.year == y])
        cache = {}
        for name, v in variants.items():
            key = (v.get("gin"), tuple(v.get("share_feats", [])))
            if key not in cache:
                if v.get("gin") == "model":
                    cache[key] = (attach_shares(trW, ty, v["share_feats"], lam_share, True),
                                  attach_shares(teW, ty, v["share_feats"], lam_share, False))
                elif v.get("gin") == "calendar":
                    a = trW.copy(); b = teW.copy()
                    a["sh"] = np.concatenate([cal_share_for(a[a.year == t], [u for u in ty if u != t]) for t in sorted(a.year.unique())]) \
                        if False else 0.0
                    parts = []
                    for t in sorted(a.year.unique()):
                        at = a[a.year == t]
                        parts.append(pd.Series(cal_share_for(at, [u for u in ty if u != t]), index=at.index))
                    a["sh"] = pd.concat(parts)
                    b["sh"] = cal_share_for(b, ty)
                    a["sh"], b["sh"] = a.sh.fillna(0.0), b.sh.fillna(0.0)
                    cache[key] = (a, b)
                else:
                    a, b = trW.copy(), teW.copy()
                    a["sh"], b["sh"] = 0.0, 0.0
                    cache[key] = (a, b)
            a, b = cache[key]
            out = pd.Series(np.nan, index=b.index)
            for k in sorted(b.k.unique()):
                trk = a[(a.k == k)].dropna(subset=["y"])
                tek = b[b.k == k]
                m = fit_week(trk, v["rat"], v.get("gin") is not None, lam_week)
                out[tek.index] = pred_week(m, tek, v["rat"], v.get("gin") is not None)
            preds[name].append(b.assign(yhat=out))
    return {k: pd.concat(v) for k, v in preds.items()}


def score(df, col="yhat"):
    """Per week: state-level R² of the crop surprise, US total R² and RMSE across years."""
    rows = []
    for k, g in df.groupby("k"):
        g = g.dropna(subset=["y", col])
        Ph = g.P0 * np.exp(g[col])
        us = g.assign(Ph=Ph).groupby("year")[["P", "Ph", "P0"]].sum()
        e = np.log(us.Ph / us.P)
        rows.append({"k": k, "date": pd.Timestamp(CUR, 9, 7) + pd.Timedelta(days=7 * k),
                     "R2_state": r2(g.y, g[col], g.P0),
                     "R2_state_level": r2(np.log(g.P), np.log(Ph), g.P0),
                     "R2_US": r2(np.log(us.P), np.log(us.Ph)),
                     "US_rmse_pct": float(np.sqrt(np.mean(e ** 2)) * 100),
                     "US_mae_pct": float(np.mean(np.abs(e)) * 100)})
    return pd.DataFrame(rows).set_index("k")


# ───────── stage 1b: calendar pace adjusted for harvest-progress / rating anomalies ─────────
ANOM_BASE = ["lH", "lH14", "lBO"]


def _period_means(tr, excl_year=None):
    t = tr if excl_year is None else tr[tr.year != excl_year]
    t = t[t.P.notna()]
    g = t.groupby(["state", "period"])
    m = g[["lH", "lH14", "lBO"]].mean()
    m["lsh"] = g.share.apply(lambda s: np.log(np.clip(s, 0.003, 1.0)).mean())
    return m


def _anom_rows(d, m):
    idx = pd.MultiIndex.from_arrays([d.state, d.period])
    mm = m.reindex(idx)
    d = d.copy()
    d["lsh_bar"] = mm.lsh.values
    for c in ANOM_BASE:
        d[c + "_a"] = d[c].values - mm[c].values
    return d


def anom_train(tr, feats, lam):
    parts = []
    for v in sorted(tr.year.unique()):
        parts.append(_anom_rows(tr[tr.year == v], _period_means(tr, excl_year=v)))
    t = pd.concat(parts)
    t = t[(t.G > 0) & (t.share > 0.003) & t.lsh_bar.notna()].copy()
    t["ly"] = np.log(np.clip(t.share, 0.003, 1.0)) - t.lsh_bar
    for f in feats:
        t[f] = t[f].fillna(0.0)
    m = hfit(t, "ly", feats, lam=lam)
    m["means"] = _period_means(tr)
    return m


def anom_predict(m, d):
    d = _anom_rows(d, m["means"])
    for f in m["feats"]:
        d[f] = d[f].fillna(0.0)
    return np.exp(np.clip(d.lsh_bar.values + hpred(m, d), np.log(0.003), 0.0))


def gin_share_for(rows, train_years, feats, lam):           # override: dispatch on feature set
    tr = prep(train_years, R[R.year.isin(train_years)])
    d = prep(train_years, as_ref(rows))
    if feats and feats[0].endswith("_a"):
        m = anom_train(tr, feats, lam)
        sh = anom_predict(m, d)
    else:
        m = share_train(tr, feats, lam)
        sh = share_predict(m, d.fillna({"lH": -4.6, "lH14": -4.6, "lBO": -4.6}))
    sh = np.where(np.isnan(sh), 0.0, sh)
    return np.where(rows.G.values > 0, sh, 0.0)


# ───────── inverse-variance combiner ─────────
BINS = np.array([0.0, 0.03, 0.06, 0.10, 0.15, 0.22, 0.30, 0.40, 0.55, 0.70, 0.85, 1.01])


def _shares(v, trW, teW, ty, lam_share):
    if v.get("gin") == "model":
        return (attach_shares(trW, ty, v["share_feats"], lam_share, True), attach_shares(teW, ty, v["share_feats"], lam_share, False))
    a, b = trW.copy(), teW.copy()
    if v.get("gin") == "calendar":
        parts = [pd.Series(cal_share_for(a[a.year == t], [u for u in ty if u != t]), index=a[a.year == t].index) for t in sorted(a.year.unique())]
        a["sh"] = pd.concat(parts).fillna(0.0)
        b["sh"] = np.nan_to_num(cal_share_for(b, ty))
    else:
        a["sh"], b["sh"] = 0.0, 0.0
    return a, b


def _gin_err_model(a):
    """σ and bias of the ginning-implied crop (log) by predicted share, from out-of-sample training rows."""
    g = a[(a.sh >= 0.02) & (a.G > 0)].dropna(subset=["y"])
    gh = np.log(g.G / g.sh / g.P0)
    e = (gh - g.y).clip(-2, 2)
    b = np.digitize(g.sh, BINS) - 1
    sd, bias, mid = [], [], []
    for i in range(len(BINS) - 1):
        m = b == i
        if m.sum() >= 8:
            w = g.P0[m]
            bias.append(np.average(e[m], weights=w))
            sd.append(np.sqrt(np.average((e[m] - bias[-1]) ** 2, weights=w)))
            mid.append(np.average(g.sh[m], weights=w))
    sd = np.minimum.accumulate(np.array(sd))            # more ginned → never less precise
    return np.array(mid), sd, np.array(bias)


def run_iv(variants, lam_share=5.0, lam_week=5.0, use_bias=True):
    preds = {k: [] for k in variants}
    for y in HIST:
        ty = [v for v in HIST if v != y]
        trW = prep(ty, Wf[Wf.year.isin(ty)])
        teW = prep(ty, Wf[Wf.year == y])
        for name, v in variants.items():
            a, b = _shares(v, trW, teW, ty, lam_share)
            mid, sdg, bias = _gin_err_model(a)
            out = pd.Series(np.nan, index=b.index)
            wgin = pd.Series(0.0, index=b.index)
            for k in sorted(b.k.unique()):
                ak = a[a.k == k].dropna(subset=["y"])
                bk = b[b.k == k]
                # ratings/drought model and its out-of-sample error at this week
                m = fit_week(ak, v["rat"], False, lam_week)
                yr = pred_week(m, bk, v["rat"], False)
                res = []
                for t in sorted(ak.year.unique()):
                    mt = fit_week(ak[ak.year != t], v["rat"], False, lam_week)
                    at = ak[ak.year == t]
                    res.append(pd.DataFrame({"e": at.y.values - pred_week(mt, at, v["rat"], False), "w": at.P0.values}))
                res = pd.concat(res)
                sdr = np.sqrt(np.average(res.e ** 2, weights=res.w))
                ok = (bk.sh.values >= 0.05) & (bk.G.values > 0) & (len(sdg) > 0)
                gh = np.where(ok, np.log(np.maximum(bk.G.values, 1) / np.maximum(bk.sh.values, 0.02) / bk.P0.values), 0.0)
                if len(sdg):
                    sg = np.interp(bk.sh.values, mid, sdg)
                    bg = np.interp(bk.sh.values, mid, bias) if use_bias else 0.0
                    gh = gh - bg
                else:
                    sg = np.full(len(bk), 9.0)
                wg = np.where(ok, (1 / sg ** 2) / (1 / sg ** 2 + 1 / sdr ** 2), 0.0)
                out[bk.index] = wg * gh + (1 - wg) * yr
                wgin[bk.index] = wg
            preds[name].append(b.assign(yhat=out, w_gin=wgin))
    return {k: pd.concat(v) for k, v in preds.items()}


# ───────── drought only through October (crop essentially made after that) ─────────
DROUGHT_CUTOFF = True
_prep_orig = prep


def prep(tr_years, df):            # noqa: F811
    d = _prep_orig(tr_years, df)
    if DROUGHT_CUTOFF and "D" in d.columns:
        late = (d.D.dt.month >= 11) | (d.D.dt.month <= 2)
        d.loc[late, "d2_dev"] = 0.0          # drought term switched off from November
    return d


# ───────── phenology-weighted drought terms ─────────
PH = ["d2_sq", "d2_bl", "d2_op", "d1_bl"]
RAT.update({"ratings+pheno": ["ci_dev", "ciavg_dev", "pvp_dev", "d2_sq_dev", "d2_bl_dev", "d2_op_dev"],
            "ratings+bloom": ["ci_dev", "ciavg_dev", "pvp_dev", "d2_bl_dev"],
            "ratings+sq+bloom": ["ci_dev", "ciavg_dev", "pvp_dev", "d2_sq_dev", "d2_bl_dev"]})
_prep2 = prep


def prep(tr_years, df):            # noqa: F811
    d = _prep2(tr_years, df)
    if PH[0] in d.columns:
        base = Wf[Wf.year.isin(tr_years)]
        mm = base.groupby("state")[PH].mean()
        m = mm.reindex(d.state)
        for c in PH:
            d[c + "_dev"] = (d[c].fillna(0).values - m[c].fillna(0).values)
        if DROUGHT_CUTOFF and "D" in d.columns:
            late = (d.D.dt.month >= 11) | (d.D.dt.month <= 2)
            for c in PH:
                d.loc[late, c + "_dev"] = 0.0
    return d


# ───────── USDA's latest in-season forecast as an input ─────────
RAT.update({"usda": ["usda_dev"], "usda+ratings": ["usda_dev", "ci_dev", "ciavg_dev", "pvp_dev"],
            "usda+ratings+bloom": ["usda_dev", "ci_dev", "ciavg_dev", "pvp_dev", "d2_bl_dev"]})
_prep3 = prep


def prep(tr_years, df):            # noqa: F811
    d = _prep3(tr_years, df)
    if "usda" in d.columns:
        d["usda_dev"] = np.log(d.usda.fillna(d.P0) / d.P0)
    return d
