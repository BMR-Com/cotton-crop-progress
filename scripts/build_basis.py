#!/usr/bin/env python3
"""Basis & trade ideas page data → docs/data/basis.json

Spot: AMS daily spot quotation, base quality 41-4-34, average of the designated markets (data/spot/spot41_daily.csv)
Futures: docs/data/cotton_px.csv (month generics CTxxx1 / CTxxx2), Dec/Mar/May/Jul only
Positioning: docs/data/cot_cotton.csv (managed money, all crops)
Balance sheet: BMR-Com/wasde-analytics Cotton.json
Seasonal windows: BMR-Com/cftc-cot-full data/web/softs_seasonal.json
"""
import os, json, math, datetime as dt
import numpy as np, pandas as pd, requests

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SPOT = os.path.join(ROOT, 'data', 'spot', 'spot41_daily.csv')
MKTS = os.path.join(ROOT, 'data', 'spot', 'ams_spot_markets.csv')
PX = os.path.join(ROOT, 'docs', 'data', 'cotton_px.csv')
COT = os.path.join(ROOT, 'docs', 'data', 'cot_cotton.csv')
OUT = os.path.join(ROOT, 'docs', 'data', 'basis.json')
WASDE_URL = 'https://raw.githubusercontent.com/BMR-Com/wasde-analytics/main/data/Cotton.json'
SOFTS_URL = 'https://raw.githubusercontent.com/BMR-Com/cftc-cot-full/main/data/web/softs_seasonal.json'
CYC = 'HKNZ'; MC = dict(H=3, K=5, N=7, Z=12); GEN = dict(H='MAR', K='MAY', N='JUL', Z='DEC')
NAME = dict(H='Mar', K='May', N='Jul', Z='Dec'); MULT = 500
MON = ['Aug', 'Sep', 'Oct', 'Nov', 'Dec', 'Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul']

def bd(x): return pd.offsets.BDay(x)
def nth_bd(y, m, n):
    first = pd.Timestamp(y, m, 1); first = first if first.dayofweek < 5 else first + bd(1)
    return first + bd(n - 1)
def gsci_day3(k):
    m1 = pd.Timestamp(k[1], MC[k[0]], 1) - pd.DateOffset(months=1)
    return nth_bd(m1.year, m1.month, 7)
def fnd(code, y):
    first = pd.Timestamp(y, MC[code], 1); first = first if first.dayofweek < 5 else first + bd(1)
    return first - bd(5)
def lab(k): return f"{NAME[k[0]]} {k[1] % 100:02d}"
def nxt(k):
    i = CYC.index(k[0]) + 1
    return (CYC[i % 4], k[1] + i // 4)

class Px:
    """contract prices from Bloomberg-style month generics: CTMAR1 = next unexpired Mar, CTMAR2 = the one after"""
    def __init__(self):
        p = pd.read_csv(PX, low_memory=False); p['d'] = pd.to_datetime(p['Dates'], format='%m/%d/%y')
        self.p = p.set_index('d').sort_index()
    def col(self, k, d):
        y1 = d.year if d < fnd(k[0], d.year) else d.year + 1
        n = k[1] - y1 + 1
        return f"CT{GEN[k[0]]}{n} Comdty PX_LAST" if n in (1, 2) else None
    def get(self, k, d):
        d = pd.Timestamp(d); s = self.p[self.p.index <= d]
        if not len(s) or (d - s.index[-1]).days > 5: return np.nan
        c = self.col(k, s.index[-1])
        return float(s[c].iloc[-1]) if c and c in s and not pd.isna(s[c].iloc[-1]) else np.nan
    def front(self, d, days=5):
        d = pd.Timestamp(d); y = d.year
        cands = [(c, yy) for yy in (y, y + 1) for c in CYC if fnd(c, yy) > d + pd.Timedelta(days=days)]
        return min(cands, key=lambda k: fnd(*k))
    @property
    def last(self): return self.p.index.max()

def my_of(d): return d.year if d.month >= 8 else d.year - 1
def my_week(d): return int((d - pd.Timestamp(my_of(d), 8, 1)).days // 7)

def build():
    X = Px(); today = X.last
    S = pd.read_csv(SPOT, parse_dates=['date']); S = S[S.date.dt.dayofweek < 5].set_index('date').spot41.sort_index()
    # ---------------- daily basis series (vs nearest Dec/Mar/May/Jul not near first notice)
    # weekly (Friday) sample keeps it fast; daily for the last 400 days for the chart
    fr = pd.date_range(S.index.min(), S.index.max(), freq='W-FRI')
    rows = []
    for t in fr:
        sp = S[S.index <= t]
        if not len(sp) or (t - sp.index[-1]).days > 4: continue
        k = X.front(t); f = X.get(k, t)
        if pd.isna(f): continue
        rows.append(dict(date=t, my=my_of(t), wk=my_week(t), spot=sp.iloc[-1], fut=f, near=lab(k), basis=sp.iloc[-1] - f))
    W = pd.DataFrame(rows)
    # targets for history tables: next 8 weeks of the front (first notice > 61 days away) and its spread vs the next contract
    tg = []
    for r in W.itertuples():
        k = X.front(r.date, 61); n = nxt(k); t2 = r.date + pd.Timedelta(weeks=8)
        if t2 > today: tg.append((np.nan, np.nan)); continue
        a0, b0, a1, b1 = X.get(k, r.date), X.get(n, r.date), X.get(k, t2), X.get(n, t2)
        tg.append((math.log(a1 / a0) * 100 if a0 and a1 else np.nan, (a1 - b1) - (a0 - b0)))
    W['px8'], W['sp8'] = zip(*tg)
    norm = W.apply(lambda r: W[(W.my != r.my) & ((W.wk - r.wk).abs() <= 1)].basis.median(), axis=1)
    W['anom'] = W.basis - norm
    W['chg4'] = W.basis - W.basis.shift(4)
    hist = W[W.my < my_of(today)]
    q1, q2 = hist.anom.quantile([1 / 3, 2 / 3])
    cur = W.iloc[-1]
    # ---------------- seasonal chart (marketing-year weeks)
    cur_my = my_of(today); P = W.pivot_table(index='my', columns='wk', values='basis', aggfunc='last')
    past = P[P.index < cur_my]
    bands = {q: [[int(w), round(float(v), 2)] for w, v in past.quantile(q / 100).items() if not pd.isna(v)] for q in (10, 25, 50, 75, 90)}
    avg5 = [[int(w), round(float(v), 2)] for w, v in past.tail(5).mean().items() if not pd.isna(v)]
    rowpts = lambda y: [[int(w), round(float(v), 2)] for w, v in P.loc[y].items() if not pd.isna(v)] if y in P.index else []
    # ---------------- monthly heat map (median basis by marketing year x month, last 12 seasons)
    W['mon'] = W.date.dt.month
    H = W[W.my >= cur_my - 11].pivot_table(index='my', columns='mon', values='basis', aggfunc='median')
    heat = {int(y): {MON[[8, 9, 10, 11, 12, 1, 2, 3, 4, 5, 6, 7].index(m)]: round(float(v), 2) for m, v in r.items() if not pd.isna(v)} for y, r in H.iterrows()}
    # ---------------- history tables by basis state (Oct-May)
    om = hist[hist.date.dt.month.isin([10, 11, 12, 1, 2, 3, 4, 5])].dropna(subset=['px8'])
    def tbl(df):
        out = []
        for name, m in (('Weak', df.anom <= q1), ('Normal', (df.anom > q1) & (df.anom < q2)), ('Firm', df.anom >= q2)):
            g = df[m]
            out.append(dict(state=name, n=int(len(g)), fut_avg=round(g.px8.mean(), 2), fut_up=round((g.px8 > 0).mean() * 100),
                            spread_avg=round(g.sp8.mean(), 2), spread_up=round((g.sp8 > 0).mean() * 100), spread_usd=round(g.sp8.mean() * MULT)))
        return out
    jm = om[om.date.dt.month.isin([1, 2, 3, 4, 5])]
    state = 'Weak' if cur.anom <= q1 else ('Firm' if cur.anom >= q2 else 'Normal')
    # ---------------- daily chart: full basis history (weekly) + last 400 days daily
    daily = []
    for d, s in S[S.index >= today - pd.Timedelta(days=400)].items():
        k = X.front(d); f = X.get(k, d)
        if not pd.isna(f): daily.append([str(d.date()), round(s, 2), round(f, 2), round(s - f, 2), lab(k)])
    # ---------------- per-market (API, Aug 2025 onward)
    mk = pd.read_csv(MKTS, parse_dates=['date']) if os.path.exists(MKTS) else pd.DataFrame()
    markets = {}
    if len(mk):
        for m, g in mk.groupby('market'):
            g = g.sort_values('date'); last = g.iloc[-1]; prev = g[g.date <= last.date - pd.Timedelta(days=28)]
            markets[m] = dict(date=str(last.date.date()), price=round(float(last.price_41_4_34), 2), basis_pts=int(last.basis_41_4_34_pts),
                              basis_month=last.basis_month, chg4_pts=None if not len(prev) else int(last.basis_41_4_34_pts - prev.iloc[-1].basis_41_4_34_pts),
                              bales_4w=int(g[g.date > last.date - pd.Timedelta(days=28)].bales.sum()),
                              series=[[str(d.date()), int(b)] for d, b in zip(g.date, g.basis_41_4_34_pts) if not pd.isna(b)][-260:])
    ideas = trade_ideas(X, W, cur, state, q1, q2, om, jm, today)
    return dict(generated=dt.datetime.now(dt.timezone.utc).strftime('%Y-%m-%d %H:%M UTC'), as_of=str(today.date()),
                spot_as_of=str(S.index.max().date()),
                now=dict(spot=round(cur.spot, 2), fut=round(cur.fut, 2), near=cur.near, basis=round(cur.basis, 2), anom=round(cur.anom, 2),
                         norm=round(cur.basis - cur.anom, 2), chg4=None if pd.isna(cur.chg4) else round(cur.chg4, 2), state=state,
                         pct_anom=round(float((hist.anom < cur.anom).mean() * 100)), week=int(cur.wk), date=str(cur.date.date()),
                         same_week_range=[round(float(past[cur.wk].quantile(.1)), 2), round(float(past[cur.wk].quantile(.9)), 2)] if cur.wk in past else None),
                seasonal=dict(bands=bands, avg5=avg5, current=rowpts(cur_my), last_year=rowpts(cur_my - 1), my=cur_my, first_my=int(past.index.min()), n_years=int(len(past))),
                heat=heat, history=dict(oct_may=tbl(om), jan_may=tbl(jm), cut_weak=round(q1, 2), cut_firm=round(q2, 2)),
                weekly=[[str(r.date.date()), round(r.basis, 2)] for r in W.itertuples()], daily=daily, markets=markets, ideas=ideas)

# ============================================================== trade ideas
def spec_pct(today):
    c = pd.read_csv(COT, parse_dates=['date']); c = c[c.crop_type == 'all'].sort_values('date')
    c['x'] = c.managed_money_net / c.open_interest
    c['pct'] = c.x.rolling(156, min_periods=104).apply(lambda w: (w[:-1] < w[-1]).mean() * 100, raw=True)
    return c.set_index('date')[['x', 'pct']]

def rogers(k):
    M = MC[k[0]]; m2 = pd.Timestamp(k[1], M, 1) - pd.DateOffset(months=2)
    last = m2 + pd.offsets.MonthEnd(0); last = last if last.dayofweek < 5 else last - bd(1)
    day1 = last - bd(1); return day1, day1 - bd(1)            # Rogers day 1, close before it (cover)

def trade_ideas(X, W, cur, state, q1, q2, om, jm, today):
    ideas = []
    mo = today.month; in_season = mo in (10, 11, 12, 1, 2, 3, 4, 5)
    # 1) basis direction
    def row(df, s):
        g = df[(df.anom <= q1) if s == 'Weak' else (df.anom >= q2) if s == 'Firm' else ((df.anom > q1) & (df.anom < q2))]
        return g
    g = row(om, state); k = X.front(today, 61)
    if state == 'Normal' or not in_season:
        ideas.append(dict(id='basis_px', title='Basis signal — flat price', status='Neutral', side='No lean',
                          instrument=lab(k) + ' futures', window='next 8 weeks',
                          why=f"Basis {cur.basis:+.2f}¢ vs {cur.near} is close to normal for this week ({cur.anom:+.2f}¢ from the seasonal median).",
                          record=None))
    else:
        bear = state == 'Weak'; hit = (g.px8 < 0).mean() if bear else (g.px8 > 0).mean()
        ideas.append(dict(id='basis_px', title='Basis signal — flat price', status='Active', side='Bearish lean' if bear else 'Bullish lean',
                          instrument=lab(k) + ' futures', window=f"{today.date()} → {(today + pd.Timedelta(weeks=8)).date()}",
                          why=f"Spot 41-4-34 is {cur.basis:+.2f}¢ vs {cur.near}, {abs(cur.anom):.2f}¢ {'weaker' if bear else 'firmer'} than normal for this week. "
                              f"A {'weak' if bear else 'firm'} physical basis has tended to lead futures {'lower' if bear else 'higher'}.",
                          record=dict(hit=round(hit * 100), avg=f"{g.px8.mean():+.1f}% over 8 weeks", n=int(len(g)), label=f"{state} basis, Oct–May since 2000")))
    if mo in (1, 2, 3, 4, 5) and state != 'Normal':
        gj = row(jm, state); bear = state == 'Weak'; n = nxt(k); hit = (gj.sp8 < 0).mean() if bear else (gj.sp8 > 0).mean()
        ideas.append(dict(id='basis_sp', title='Basis signal — front spread', status='Active', side='Bear spread (sell front)' if bear else 'Bull spread (buy front)',
                          instrument=f"{lab(k)} / {lab(n)}", window=f"{today.date()} → {(today + pd.Timedelta(weeks=8)).date()}",
                          why='From January the old-crop spread follows the physical basis: a weak basis has tended to see the nearby lose to the deferred.' if bear else
                              'From January a firm physical basis has tended to see the nearby gain on the deferred.',
                          record=dict(hit=round(hit * 100), avg=f"{gj.sp8.mean():+.2f}¢ ({gj.sp8.mean() * MULT:+,.0f} $/spread) over 8 weeks", n=int(len(gj)), label=f"{state} basis, Jan–May since 2000")))
    # 2) pre-Rogers-roll spread short, conditional on managed-money length
    SP = spec_pct(today)
    def pct_at(t):
        s = SP[SP.index + pd.Timedelta(days=3) <= t]
        return (np.nan, np.nan) if not len(s) else tuple(s.iloc[-1])
    hist = []
    for y in range(2008, today.year + 1):
        for c in CYC:
            kk = (c, y); d1, cover = rogers(kk); entry = cover - bd(20)
            if cover > today: continue
            nn = nxt(kk)
            a0, b0, a1, b1 = X.get(kk, entry), X.get(nn, entry), X.get(kk, cover), X.get(nn, cover)
            if any(pd.isna(v) for v in (a0, b0, a1, b1)): continue
            hist.append(dict(c=lab(kk), entry=entry, pct=pct_at(entry)[1], pnl=-((a1 - b1) - (a0 - b0))))
    H = pd.DataFrame(hist)
    up = [z for z in [(c, y) for y in (today.year, today.year + 1) for c in CYC] if rogers(z)[1] >= today.normalize()]
    kk = min(up, key=lambda z: rogers(z)[0]); d1, cover = rogers(kk); entry = cover - bd(20); nn = nxt(kk)
    x_now, p_now = pct_at(today); hh = H[H.pct >= 80]; hh90 = H[H.pct >= 90]
    rec = dict(hit=round((hh.pnl > 0).mean() * 100), avg=f"{hh.pnl.mean():+.2f}¢ ({hh.pnl.mean() * MULT:+,.0f} $/spread)", n=int(len(hh)),
               label="spec length ≥80th percentile, 2008 onward", extra=f"≥90th: {(hh90.pnl > 0).mean() * 100:.0f}% of {len(hh90)}")
    base = dict(id='prerogers', title='Pre-roll spread short (index roll + spec length)', side='Bear spread (sell front)',
                instrument=f"{lab(kk)} / {lab(nn)}", window=f"{entry.date()} → {cover.date()}", record=rec)
    if today.normalize() >= entry:                       # window open: the decision was made at entry
        x_e, p_e = pct_at(entry)
        a0, b0, a1, b1 = X.get(kk, entry), X.get(nn, entry), X.get(kk, today), X.get(nn, today)
        mark = -((a1 - b1) - (a0 - b0))
        if p_e >= 80:
            st = 'Cover at today\'s close' if today.normalize() >= cover else 'Active'
            why = (f"Entered {entry.date()} with managed money at the {p_e:.0f}th percentile of its 3-year range; covers at the close before the Rogers roll out of {lab(kk)} starts ({d1.date()}). "
                   f"Spread {lab(kk)}/{lab(nn)} has moved {mark:+.2f}¢ in favour ({mark * MULT:+,.0f} $/spread).")
            base.update(status=st, why=why, mark=dict(pnl_c=round(mark, 2), pnl_usd=round(mark * MULT)))
        else:
            base.update(status='Not taken — spec length below trigger at entry',
                        why=f"Managed money was at the {p_e:.0f}th percentile at entry ({entry.date()}); the rule needs the 80th or higher.")
    else:
        base.update(status=('Qualifies now — entry on ' if p_now >= 80 else 'Watch — entry on ') + str(entry.date()),
                    why=f"Sell the {lab(kk)}/{lab(nn)} spread 20 trading days before the Rogers index roll out of {lab(kk)} (roll starts {d1.date()}) if managed-money net length is in the top 20% of its 3-year range. "
                        f"Managed money is now {x_now * 100:+.1f}% of open interest, {p_now:.0f}th percentile.")
    ideas.append(base)
    # 3) old-crop / new-crop spread from the May WASDE
    try:
        ideas.append(newcrop_idea(X, today))
    except Exception as e:
        print('new-crop idea skipped:', e)
    # 4) seasonal spread windows (from the Softs Seasonals build)
    try:
        J = requests.get(SOFTS_URL, timeout=60).json()
        for t in J.get('trades', []):
            if t.get('root') != 'CT': continue
            ideas.append(dict(id='seasonal', title='Seasonal spread window', status='Upcoming' if t['next_entry'] > str(today.date()) else 'Active',
                              side=t['side'], instrument=t['what'].split(' in ')[0].replace('Bear spread (sell front) ', '').replace('Bull spread (buy front) ', ''),
                              window=f"{t['next_entry']} → {t['next_exit']}",
                              why=f"{'Sell' if 'sell' in t['side'] else 'Buy'} the {t['what'].split(' ')[-3]} spread at the close on {t['next_entry']} and close it at the end of {t.get('month', '')}. It has worked in {t['hit']}% of years since 2000 and in {t.get('hit_last10', '–')}% of the last 10.",
                              record=dict(hit=t['hit'], avg=(f"{t['avg_chg']:+.2f}¢ ({t['usd']:+,.0f} $/spread)" if t.get('avg_chg') is not None else f"{t['avg_pct']:+.2f}%"),
                                          n=t['n'], label='years since 2000')))
    except Exception as e:
        print('seasonal ideas skipped:', e)
    return ideas

def newcrop_idea(X, today):
    d = requests.get(WASDE_URL, timeout=90).json()['data']
    def su(my):
        out = {}
        for a_es in ('Ending Stocks', 'Ending stocks'):
            es = {(r['forecastYear'], r['forecastMonth']): r['value'] for r in d.get(f'United States||{a_es}||{my}', [])}
            if es: break
        use = {}
        for a in ('Use, Total', 'Use, total'):
            for r in d.get(f'United States||{a}||{my}', []): use.setdefault((r['forecastYear'], r['forecastMonth']), r['value'])
        dom = {(r['forecastYear'], r['forecastMonth']): r['value'] for a in ('Domestic Use', 'Domestic use') for r in d.get(f'United States||{a}||{my}', [])}
        exp = {(r['forecastYear'], r['forecastMonth']): r['value'] for a in ('Exports', 'Exports, Total') for r in d.get(f'United States||{a}||{my}', [])}
        for k in set(dom) & set(exp): use.setdefault(k, dom[k] + exp[k])
        return {k: es[k] / use[k] for k in es if k in use and use[k]}
    Y = today.year if today.month >= 5 else today.year - 1
    my = f"{Y}/{str(Y + 1)[2:]}"; cur = su(my)
    if (Y, 5) not in cur: raise ValueError('no May WASDE yet')
    hist = []
    for yy in range(Y - 10, Y):
        s = su(f"{yy}/{str(yy + 1)[2:]}")
        if s: hist.append(s[max(s)])                      # latest estimate of each past season
    pct = lambda v: 100 * np.mean([h < v for h in hist])
    may, latest_k = cur[(Y, 5)], max(cur); latest = cur[latest_k]
    p_may, p_now = pct(may), pct(latest)
    short0, long0 = ('Z', Y), ('Z', Y + 1)
    entry = pd.Timestamp(Y, 5, 12)
    # mark to market with GSCI-roll timing (7th business day of the month before delivery)
    chain = [short0, ('H', Y + 1), ('K', Y + 1), ('N', Y + 1)]
    t = entry; pnl = 0.0; legs = []
    for i, k in enumerate(chain):
        roll = gsci_day3(k) if i < 3 else fnd('N', Y + 1) - bd(1)
        end = min(roll, X.last)
        if end <= t: continue
        a, b = X.get(k, t), X.get(k, end)
        pnl += -(b - a); legs.append(lab(k)); t = end
        if end >= X.last: break
    la, lb = X.get(long0, entry), X.get(long0, X.last)
    pnl += lb - la
    nxt_roll_k = [k for k in chain[:3] if gsci_day3(k) >= X.last]
    nr = None
    if nxt_roll_k:
        k = nxt_roll_k[0]; nr = dict(contract=lab(k), to=lab(nxt(k)), rogers=str(rogers(k)[1].date()), gsci=str(gsci_day3(k).date()))
    qualifies = p_may > 30
    stopped = p_now <= 30
    status = 'Not taken — tight balance sheet at entry' if not qualifies else ('Stop — balance sheet has tightened' if stopped else 'Active')
    return dict(id='newcrop', title='Old-crop / new-crop spread from the first balance sheet',
                status=status, side=f"Short {lab(short0)} (rolled) / long {lab(long0)}",
                instrument=f"{' → '.join(legs)} vs {lab(long0)}", window=f"{entry.date()} → {(fnd('N', Y + 1) - bd(1)).date()}",
                why=f"US {my} ending stocks/use at the May WASDE was {may * 100:.1f}% ({p_may:.0f}th percentile of the last 10 seasons); latest {latest * 100:.1f}% ({p_now:.0f}th, {latest_k[1]}/{latest_k[0]}). "
                    f"The rule skips seasons at or below the 30th percentile and exits if a later WASDE drops there.",
                mark=dict(pnl_c=round(pnl, 2), pnl_usd=round(pnl * MULT)), next_roll=nr,
                record=dict(hit=80, avg="+$1,903 per spread", n=10, label="May entries in non-tight seasons, 2010–2025"))

if __name__ == '__main__':
    out = build()
    os.makedirs(os.path.dirname(OUT), exist_ok=True)
    def clean(o):
        if isinstance(o, dict): return {k: clean(v) for k, v in o.items()}
        if isinstance(o, (list, tuple)): return [clean(v) for v in o]
        if isinstance(o, (np.floating, float)): return None if (math.isnan(o) or math.isinf(o)) else float(o)
        if isinstance(o, np.integer): return int(o)
        return o
    json.dump(clean(out), open(OUT, 'w'), separators=(',', ':'), default=str, allow_nan=False)
    print('basis.json', os.path.getsize(OUT) // 1024, 'KB; state', out['now']['state'], out['now']['basis'], 'ideas', len(out['ideas']))
