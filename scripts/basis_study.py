#!/usr/bin/env python3
"""Research: does the US spot basis (AMS base-quality 41-4-34 spot, average of the designated markets,
minus the nearby ICE futures) predict the front calendar spread or flat price from October to May?

Inputs : data/spot/spot41_daily.csv (date, spot41), ICE contract closes (TradingView, cftc-cot-full research data)
Outputs: printed tables + results CSV in a scratch directory (findings go to the private research notes, not the site)
"""
import os, sys, numpy as np, pandas as pd
SPOT = sys.argv[1] if len(sys.argv) > 1 else 'data/spot/spot41_daily.csv'
CON = sys.argv[2] if len(sys.argv) > 2 else '../cftc-cot-full/research/index-roll/data/tv_contracts.csv.gz'
OUTD = sys.argv[3] if len(sys.argv) > 3 else '/tmp'
CYC = 'HKNZ'; MC = dict(H=3, K=5, N=7, Z=12)

def load_contracts():
    d = pd.read_csv(CON); d = d[d.symbol.str.match(r'^CT[HKNZ]\d{4}$')]; P = {}
    for sym, g in d.groupby('symbol'):
        idx = pd.to_datetime(g.date); idx = idx + pd.to_timedelta((idx.dt.dayofweek == 6).astype(int), unit='D')
        x = pd.Series(g.close.values, index=idx.values).sort_index(); x = x[~x.index.duplicated(keep='last')]
        med = x.rolling(11, center=True, min_periods=3).median(); P[(sym[2], int(sym[3:]))] = x[(x / med - 1).abs() <= 0.2]
    return P

def fnd(code, y, cal):
    first = cal[cal >= pd.Timestamp(y, MC[code], 1)]
    if len(first) and first[0].month == MC[code]:
        return cal[cal.get_loc(first[0]) - 5]
    return pd.Timestamp(y, MC[code], 1) - pd.offsets.BDay(5)

def nxt(k):
    i = CYC.index(k[0]) + 1
    return (CYC[i % 4], k[1] + i // 4)

def build():
    P = load_contracts(); cal = pd.DatetimeIndex(sorted(set().union(*[set(s.index) for s in P.values()])))
    F = {k: fnd(k[0], k[1], cal) for k in P}
    S = pd.read_csv(SPOT, parse_dates=['date']); S = S[S.date.dt.dayofweek < 5].set_index('date').spot41
    px = lambda k, t: (lambda s: s.iloc[-1] if len(s) and (t - s.index[-1]).days <= 4 else np.nan)(P[k][P[k].index <= t]) if k in P else np.nan
    fridays = [t for t in pd.date_range('2000-08-01', S.index.max(), freq='W-FRI')]
    rows = []
    for t in fridays:
        if t.month not in (10, 11, 12, 1, 2, 3, 4, 5): continue
        sp = S[S.index <= t]
        if not len(sp) or (t - sp.index[-1]).days > 4: continue
        # front = nearest Dec/Mar/May/Jul whose first notice is more than 8 weeks + 5 days away (so the 8-week target stays on the same pair)
        cands = sorted([k for k in P if F[k] > t + pd.Timedelta(days=61)], key=lambda k: F[k])
        near = sorted([k for k in P if F[k] > t + pd.Timedelta(days=5)], key=lambda k: F[k])
        if not cands or not near: continue
        f8, f0 = cands[0], near[0]
        r = dict(date=t, my=t.year if t.month >= 8 else t.year - 1, spot=sp.iloc[-1], near=f"{f0[0]}{f0[1]}", f_near=px(f0, t))
        r['basis'] = r['spot'] - r['f_near']                     # cents/lb vs nearest non-delivery contract
        f1 = nxt(f8); r['pair'] = f"{f8[0]}{f8[1] % 100:02d}/{f1[0]}{f1[1] % 100:02d}"
        for h in (4, 8):
            t2 = t + pd.Timedelta(weeks=h)
            a0, b0, a1, b1 = px(f8, t), px(f1, t), px(f8, t2), px(f1, t2)
            r[f'dspread_{h}'] = (a1 - b1) - (a0 - b0)               # + = front gained (bull spread)
            r[f'dpx_{h}'] = np.log(a1 / a0) * 100 if a0 and a1 else np.nan   # % change of that front contract
        r['spread_lvl'] = px(f8, t) - px(f1, t)
        rows.append(r)
    D = pd.DataFrame(rows).sort_values('date')
    D['wk'] = D.date.dt.isocalendar().week.astype(int)
    D['dbasis_4'] = D.basis - D.basis.shift(4).where((D.date - D.date.shift(4)).dt.days.between(27, 29))
    # seasonal norm for the same week, leave-one-year-out (no look-ahead into the same season)
    norm = []
    for i, r in D.iterrows():
        o = D[(D.my != r.my) & ((D.wk - r.wk).abs() <= 1)]
        norm.append(o.basis.median() if len(o) >= 10 else np.nan)
    D['basis_anom'] = D.basis - np.array(norm)
    # trailing 3-year anomaly (only past seasons) as a stricter no-look-ahead variant
    tr = []
    for i, r in D.iterrows():
        o = D[(D.my < r.my) & (D.my >= r.my - 5) & ((D.wk - r.wk).abs() <= 1)]
        tr.append(o.basis.median() if len(o) >= 6 else np.nan)
    D['basis_anom_past'] = D.basis - np.array(tr)
    return D

def report(D):
    out = []
    pd.set_option('display.width', 220)
    for pred in ('basis', 'basis_anom', 'basis_anom_past', 'dbasis_4'):
        for tgt in ('dspread_4', 'dspread_8', 'dpx_4', 'dpx_8'):
            h = int(tgt[-1]); sub = D.dropna(subset=[pred, tgt])
            # non-overlapping: every h-th Friday, averaged over the h possible phases
            rhos, ts, qd, hits = [], [], [], []
            for ph in range(h):
                s = sub.iloc[ph::h]
                if len(s) < 30: continue
                rhos.append(s[pred].rank().corr(s[tgt].rank()))
                q = pd.qcut(s[pred], 5, labels=False, duplicates='drop')
                top, bot = s[tgt][q == q.max()], s[tgt][q == 0]
                qd.append(top.mean() - bot.mean()); hits.append(((top > 0).mean(), (bot > 0).mean()))
                se = np.sqrt(top.var() / len(top) + bot.var() / len(bot)); ts.append((top.mean() - bot.mean()) / se)
            if not rhos: continue
            e = sub[sub.my < 2013]; l = sub[sub.my >= 2013]
            rho_e = e[pred].rank().corr(e[tgt].rank()); rho_l = l[pred].rank().corr(l[tgt].rank())
            out.append(dict(pred=pred, target=tgt, n=len(sub) // h, rho=np.mean(rhos), rho_2000_12=rho_e, rho_2013_26=rho_l,
                            top_minus_bottom=np.mean(qd), t_top_bottom=np.mean(ts),
                            hit_top=np.mean([x[0] for x in hits]), hit_bottom=np.mean([x[1] for x in hits])))
    R = pd.DataFrame(out); print(R.round(3).to_string(index=False)); return R

if __name__ == '__main__':
    D = build(); D.to_csv(os.path.join(OUTD, 'basis_weekly.csv'), index=False)
    print(D.groupby('my').size().to_string()); print(D[['basis', 'basis_anom', 'dbasis_4', 'dspread_4', 'dpx_4']].describe().round(2))
    R = report(D); R.to_csv(os.path.join(OUTD, 'basis_results.csv'), index=False)
