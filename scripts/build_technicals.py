#!/usr/bin/env python3
"""
docs/data/technicals.json — ICE cotton daily OHLCV from Yahoo Finance (continuous CT=F and the active
Dec/Mar/May/Jul contracts) plus a technical read for each: trend, momentum, volatility, levels, and a
TradingView-style rating from moving averages and oscillators, on daily and weekly bars.
"""
import json, math, os, sys
from datetime import date, datetime, timezone
import numpy as np
import pandas as pd

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUT = os.path.join(ROOT, "docs", "data", "technicals.json")
MONTHS = {"DEC": (12, "Z"), "MAR": (3, "H"), "MAY": (5, "K"), "JUL": (7, "N")}


def active_contracts(today, n=5):
    """Next n Dec/Mar/May/Jul contracts that have not expired (roll on the 8th of the delivery month)."""
    out, y = [], today.year
    while len(out) < n:
        for mon, (m, code) in sorted(MONTHS.items(), key=lambda kv: kv[1][0]):
            if (y, m, 8) > (today.year, today.month, today.day) and len(out) < n:
                out.append((f"{mon.title()} {y % 100:02d}", f"CT{code}{y % 100:02d}.NYB", date(y, m, 8)))
        y += 1
    return sorted(out, key=lambda x: x[2])


def fetch(ticker, period="max"):
    import yfinance as yf
    h = yf.download(ticker, period=period, interval="1d", progress=False, auto_adjust=False)
    if h is None or not len(h):
        return None
    if isinstance(h.columns, pd.MultiIndex):
        h.columns = h.columns.get_level_values(0)
    h = h[["Open", "High", "Low", "Close", "Volume"]].dropna(subset=["Close"])
    h = h[(h.Close > 0) & (h.High >= h.Low)].copy()
    for c in ("Open", "High", "Low"):                    # Yahoo sometimes sends 0 for an unknown open/high/low
        h[c] = h[c].where(h[c] > 0)
    h["Open"] = h.Open.fillna(h.Close.shift()).fillna(h.Close)
    h["High"] = h[["High", "Open", "Close"]].max(axis=1)
    h["Low"] = h[["Low", "Open", "Close"]].min(axis=1)
    h.index = pd.to_datetime(h.index).tz_localize(None)
    return h


# ───────── indicators ─────────
def sma(s, n): return s.rolling(n).mean()
def ema(s, n): return s.ewm(span=n, adjust=False).mean()
def rma(s, n): return s.ewm(alpha=1 / n, adjust=False).mean()


def rsi(c, n=14):
    d = c.diff()
    up, dn = rma(d.clip(lower=0), n), rma(-d.clip(upper=0), n)
    return 100 - 100 / (1 + up / dn.replace(0, np.nan))


def atr(h, n=14):
    tr = pd.concat([h.High - h.Low, (h.High - h.Close.shift()).abs(), (h.Low - h.Close.shift()).abs()], axis=1).max(axis=1)
    return rma(tr, n)


def adx(h, n=14):
    up, dn = h.High.diff(), -h.Low.diff()
    pdm = np.where((up > dn) & (up > 0), up, 0.0)
    mdm = np.where((dn > up) & (dn > 0), dn, 0.0)
    a = atr(h, n)
    pdi = 100 * rma(pd.Series(pdm, h.index), n) / a
    mdi = 100 * rma(pd.Series(mdm, h.index), n) / a
    dx = 100 * (pdi - mdi).abs() / (pdi + mdi)
    return rma(dx, n), pdi, mdi


def stoch(h, n=14, k=3, d=3):
    ll, hh = h.Low.rolling(n).min(), h.High.rolling(n).max()
    kk = sma(100 * (h.Close - ll) / (hh - ll), k)
    return kk, sma(kk, d)


def cci(h, n=20):
    tp = (h.High + h.Low + h.Close) / 3
    md = tp.rolling(n).apply(lambda x: np.mean(np.abs(x - x.mean())), raw=True)
    return (tp - sma(tp, n)) / (0.015 * md)


def ichimoku(h):
    conv = (h.High.rolling(9).max() + h.Low.rolling(9).min()) / 2
    base = (h.High.rolling(26).max() + h.Low.rolling(26).min()) / 2
    a = (conv + base) / 2
    b = (h.High.rolling(52).max() + h.Low.rolling(52).min()) / 2
    return conv, base, a, b


def psar(h, step=0.02, mx=0.2):
    hi, lo = h.High.values, h.Low.values
    out = np.full(len(h), np.nan)
    if len(h) < 3:
        return pd.Series(out, h.index)
    bull, af, ep, sar = True, step, hi[0], lo[0]
    for i in range(1, len(h)):
        sar = sar + af * (ep - sar)
        if bull:
            sar = min(sar, lo[i - 1], lo[i - 2] if i > 1 else lo[i - 1])
            if lo[i] < sar:
                bull, sar, ep, af = False, ep, lo[i], step
            elif hi[i] > ep:
                ep, af = hi[i], min(af + step, mx)
        else:
            sar = max(sar, hi[i - 1], hi[i - 2] if i > 1 else hi[i - 1])
            if hi[i] > sar:
                bull, sar, ep, af = True, ep, hi[i], step
            elif lo[i] < ep:
                ep, af = lo[i], min(af + step, mx)
        out[i] = sar
    return pd.Series(out, h.index)


def hma(s, n=9):
    return sma(2 * s.rolling(n // 2).apply(_wma, raw=True) - s.rolling(n).apply(_wma, raw=True), 1).rolling(int(math.sqrt(n))).apply(_wma, raw=True)


def _wma(x):
    w = np.arange(1, len(x) + 1)
    return (x * w).sum() / w.sum()


def weekly(h):
    w = h.resample("W-FRI").agg({"Open": "first", "High": "max", "Low": "min", "Close": "last", "Volume": "sum"}).dropna(subset=["Close"])
    return w


def rating(h):
    """TradingView-style summary: each MA and oscillator votes buy / sell / neutral."""
    c = h.Close
    last = c.iloc[-1]
    ma_votes = {}
    for n in (10, 20, 30, 50, 100, 200):
        for kind, f in (("SMA", sma), ("EMA", ema)):
            v = f(c, n).iloc[-1]
            if not np.isnan(v):
                ma_votes[f"{kind} {n}"] = 1 if last > v else -1 if last < v else 0
    conv, base, a, b = ichimoku(h)
    if not np.isnan(base.iloc[-1]):
        ma_votes["Ichimoku base line"] = 1 if last > base.iloc[-1] else -1
    hm = hma(c, 9)
    if len(hm.dropna()):
        ma_votes["Hull MA 9"] = 1 if last > hm.iloc[-1] else -1
    osc = {}
    r = rsi(c)
    osc["RSI (14)"] = (r.iloc[-1], 1 if r.iloc[-1] < 30 and r.iloc[-1] > r.iloc[-2] else -1 if r.iloc[-1] > 70 and r.iloc[-1] < r.iloc[-2] else 0)
    k, d = stoch(h)
    osc["Stochastic %K (14,3,3)"] = (k.iloc[-1], 1 if k.iloc[-1] < 20 and k.iloc[-1] > d.iloc[-1] else -1 if k.iloc[-1] > 80 and k.iloc[-1] < d.iloc[-1] else 0)
    cc = cci(h)
    osc["CCI (20)"] = (cc.iloc[-1], 1 if cc.iloc[-1] < -100 and cc.iloc[-1] > cc.iloc[-2] else -1 if cc.iloc[-1] > 100 and cc.iloc[-1] < cc.iloc[-2] else 0)
    ad, pdi, mdi = adx(h)
    osc["ADX (14)"] = (ad.iloc[-1], (1 if pdi.iloc[-1] > mdi.iloc[-1] else -1) if ad.iloc[-1] > 20 and ad.iloc[-1] > ad.iloc[-2] else 0)
    ao = sma((h.High + h.Low) / 2, 5) - sma((h.High + h.Low) / 2, 34)
    osc["Awesome oscillator"] = (ao.iloc[-1], 1 if ao.iloc[-1] > 0 and ao.iloc[-1] > ao.iloc[-2] else -1 if ao.iloc[-1] < 0 and ao.iloc[-1] < ao.iloc[-2] else 0)
    mom = c - c.shift(10)
    osc["Momentum (10)"] = (mom.iloc[-1], 1 if mom.iloc[-1] > mom.iloc[-2] else -1)
    m = ema(c, 12) - ema(c, 26); sg = ema(m, 9)
    osc["MACD (12,26)"] = (m.iloc[-1], 1 if m.iloc[-1] > sg.iloc[-1] else -1)
    lo, hi = c.rolling(14).min(), c.rolling(14).max()
    sr = rsi(c); srsi = 100 * (sr - sr.rolling(14).min()) / (sr.rolling(14).max() - sr.rolling(14).min())
    sk = sma(srsi, 3); sd = sma(sk, 3)
    osc["Stochastic RSI fast"] = (sk.iloc[-1], 1 if sk.iloc[-1] < 20 and sk.iloc[-1] > sd.iloc[-1] else -1 if sk.iloc[-1] > 80 and sk.iloc[-1] < sd.iloc[-1] else 0)
    wr = -100 * (h.High.rolling(14).max() - c) / (h.High.rolling(14).max() - h.Low.rolling(14).min())
    osc["Williams %R (14)"] = (wr.iloc[-1], 1 if wr.iloc[-1] < -80 and wr.iloc[-1] > wr.iloc[-2] else -1 if wr.iloc[-1] > -20 and wr.iloc[-1] < wr.iloc[-2] else 0)
    e13 = ema(c, 13)
    bbp = (h.High - e13) + (h.Low - e13)
    osc["Bull bear power"] = (bbp.iloc[-1], 1 if bbp.iloc[-1] > 0 and bbp.iloc[-1] > bbp.iloc[-2] else -1 if bbp.iloc[-1] < 0 and bbp.iloc[-1] < bbp.iloc[-2] else 0)
    bp = c - pd.concat([h.Low, c.shift()], axis=1).min(axis=1)
    tr = pd.concat([h.High, c.shift()], axis=1).max(axis=1) - pd.concat([h.Low, c.shift()], axis=1).min(axis=1)
    uo = 100 * (4 * bp.rolling(7).sum() / tr.rolling(7).sum() + 2 * bp.rolling(14).sum() / tr.rolling(14).sum() + bp.rolling(28).sum() / tr.rolling(28).sum()) / 7
    osc["Ultimate oscillator"] = (uo.iloc[-1], 1 if uo.iloc[-1] > 70 else -1 if uo.iloc[-1] < 30 else 0)

    def word(score):
        return "Strong buy" if score > 0.5 else "Buy" if score > 0.1 else "Strong sell" if score < -0.5 else "Sell" if score < -0.1 else "Neutral"
    mv = list(ma_votes.values()); ov = [v for _, v in osc.values()]
    ms, os_ = np.mean(mv) if mv else 0, np.mean(ov) if ov else 0
    return {"overall": word((ms + os_) / 2), "ma": word(ms), "osc": word(os_),
            "ma_counts": [sum(v > 0 for v in mv), sum(v == 0 for v in mv), sum(v < 0 for v in mv)],
            "osc_counts": [sum(v > 0 for v in ov), sum(v == 0 for v in ov), sum(v < 0 for v in ov)],
            "ma_detail": [[k, round(float(ma_value(h, k)), 2), v] for k, v in ma_votes.items()],
            "osc_detail": [[k, None if v is None or (isinstance(v, float) and np.isnan(v)) else round(float(v), 2), s] for k, (v, s) in osc.items()]}


def ma_value(h, label):
    c = h.Close
    if label.startswith("SMA"):
        return sma(c, int(label.split()[1])).iloc[-1]
    if label.startswith("EMA"):
        return ema(c, int(label.split()[1])).iloc[-1]
    if label.startswith("Ichimoku"):
        return ichimoku(h)[1].iloc[-1]
    return hma(c, 9).iloc[-1]


def pivots(h):
    """Classic and Fibonacci pivots from the last completed week and month."""
    out = {}
    for lab, rule in (("weekly", "W-FRI"), ("monthly", "ME")):
        g = h.resample(rule).agg({"High": "max", "Low": "min", "Close": "last"}).dropna()
        if len(g) < 2:
            continue
        p = g.iloc[-2]
        P = (p.High + p.Low + p.Close) / 3
        rg = p.High - p.Low
        out[lab] = {"P": P, "R1": 2 * P - p.Low, "S1": 2 * P - p.High, "R2": P + rg, "S2": P - rg, "R3": p.High + 2 * (P - p.Low), "S3": p.Low - 2 * (p.High - P),
                    "fibR1": P + 0.382 * rg, "fibS1": P - 0.382 * rg, "fibR2": P + 0.618 * rg, "fibS2": P - 0.618 * rg}
    return {k: {kk: round(float(vv), 2) for kk, vv in v.items()} for k, v in out.items()}


def swings(h, look=5, n=6):
    """Recent swing highs/lows (local extremes over ±look bars) as support/resistance candidates."""
    hi, lo = h.High.values, h.Low.values
    sh, sl = [], []
    for i in range(look, len(h) - look):
        if hi[i] == hi[i - look:i + look + 1].max():
            sh.append((h.index[i], hi[i]))
        if lo[i] == lo[i - look:i + look + 1].min():
            sl.append((h.index[i], lo[i]))
    last = h.Close.iloc[-1]
    res = sorted({round(float(v), 2) for _, v in sh[-40:] if v > last})[:n]
    sup = sorted({round(float(v), 2) for _, v in sl[-40:] if v < last}, reverse=True)[:n]
    return res, sup


def analyse(h, label):
    c = h.Close
    last, prev = float(c.iloc[-1]), float(c.iloc[-2])
    s20, s50, s100, s200 = (sma(c, n).iloc[-1] for n in (20, 50, 100, 200))
    r = float(rsi(c).iloc[-1])
    m = ema(c, 12) - ema(c, 26); sg = ema(m, 9); hist = m - sg
    a = float(atr(h).iloc[-1])
    ad, pdi, mdi = adx(h)
    mid, sd = sma(c, 20), c.rolling(20).std()
    bw = (4 * sd / mid).dropna()
    bw_pct = float((bw.iloc[-252:] < bw.iloc[-1]).mean() * 100) if len(bw) > 30 else None
    hi52, lo52 = float(h.High.iloc[-252:].max()), float(h.Low.iloc[-252:].min())
    # Fibonacci retracement of the last 52-week range, direction from where the extreme came last
    up = h.High.iloc[-252:].idxmax() > h.Low.iloc[-252:].idxmin()
    rng = hi52 - lo52
    fib = {f"{p:.1%}": round(hi52 - p * rng if up else lo52 + p * rng, 2) for p in (0.236, 0.382, 0.5, 0.618, 0.786)}
    res, sup = swings(h)
    s50s, s200s = sma(c, 50), sma(c, 200)
    cross = None
    for i in range(1, min(60, len(c) - 200)):
        a1, b1, a0, b0 = s50s.iloc[-i], s200s.iloc[-i], s50s.iloc[-i - 1], s200s.iloc[-i - 1]
        if a1 > b1 and a0 <= b0:
            cross = f"golden cross (50-day above 200-day) {i} sessions ago"; break
        if a1 < b1 and a0 >= b0:
            cross = f"death cross (50-day below 200-day) {i} sessions ago"; break
    w = weekly(h)
    rt_d, rt_w = rating(h), (rating(w) if len(w) > 60 else None)
    ret = {k: (float(c.iloc[-1] / c.iloc[-n - 1] - 1) * 100 if len(c) > n else None) for k, n in (("1w", 5), ("1m", 21), ("3m", 63))}
    ytd = c[c.index.year == c.index[-1].year]
    ret["ytd"] = float(c.iloc[-1] / ytd.iloc[0] - 1) * 100 if len(ytd) > 1 else None

    trend = ("up" if last > s50 > s200 else "down" if last < s50 < s200 else "mixed") if not np.isnan(s200) else ("up" if last > s50 else "down")
    t = []
    t.append(f"{label} at {last:.2f}¢ ({last - prev:+.2f} on the day), {ret['1w']:+.1f}% on the week and {ret['1m']:+.1f}% on the month; "
             f"52-week range {lo52:.2f}–{hi52:.2f}¢.")
    ma_txt = ", ".join(f"{n}-day {v:.2f}" for n, v in ((20, s20), (50, s50), (100, s100), (200, s200)) if not np.isnan(v))
    t.append(f"Trend: {trend}. Price is {'above' if last > s50 else 'below'} the 50-day and {'above' if not np.isnan(s200) and last > s200 else 'below'} the 200-day ({ma_txt})"
             + (f"; {cross}." if cross else "."))
    t.append(f"Momentum: RSI {r:.0f} ({'overbought' if r > 70 else 'oversold' if r < 30 else 'neutral'}); MACD {'above' if m.iloc[-1] > sg.iloc[-1] else 'below'} its signal "
             f"with the histogram {'rising' if hist.iloc[-1] > hist.iloc[-2] else 'falling'}; ADX {ad.iloc[-1]:.0f} "
             f"({'trending' if ad.iloc[-1] > 25 else 'weak trend' if ad.iloc[-1] > 20 else 'range-bound'}, {'+DI' if pdi.iloc[-1] > mdi.iloc[-1] else '−DI'} leading).")
    t.append(f"Volatility: ATR {a:.2f}¢ ({a / last * 100:.1f}% of price)" + (f"; Bollinger width at the {bw_pct:.0f}th percentile of the past year"
             + (" — a squeeze, often ahead of a range break." if bw_pct < 20 else ".") if bw_pct is not None else "."))
    if res or sup:
        t.append("Levels: resistance " + (", ".join(f"{v:.2f}" for v in res[:3]) or "—") + "; support " + (", ".join(f"{v:.2f}" for v in sup[:3]) or "—")
                 + f". 52-week Fibonacci 38.2% {fib['38.2%']:.2f}, 50% {fib['50.0%']:.2f}, 61.8% {fib['61.8%']:.2f}.")
    t.append(f"Rating: daily {rt_d['overall'].lower()} (moving averages {rt_d['ma'].lower()}, oscillators {rt_d['osc'].lower()})"
             + (f"; weekly {rt_w['overall'].lower()}." if rt_w else "."))
    return {"last": last, "chg": last - prev, "returns": ret, "trend": trend, "rsi": r, "atr": a, "adx": float(ad.iloc[-1]),
            "hi52": hi52, "lo52": lo52, "fib": fib, "resistance": res, "support": sup, "pivots": pivots(h),
            "rating_daily": rt_d, "rating_weekly": rt_w, "text": t, "date": h.index[-1].strftime("%Y-%m-%d")}


def main():
    today = date.today()
    series = [("Continuous (CT=F)", "CT=F", None)] + active_contracts(today)
    old = json.load(open(OUT)) if os.path.exists(OUT) else {}
    out = {"generated": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"), "series": [], "note":
           "Daily bars from Yahoo Finance (ICE Futures US). The continuous series rolls between contracts, so gaps at rolls are price jumps, not moves."}
    for label, tk, exp in series:
        h = fetch(tk)
        if h is None or len(h) < 60:
            prev = next((s for s in old.get("series", []) if s["ticker"] == tk), None)
            if prev:
                out["series"].append(prev)
            print("no data", tk); continue
        bars = [[d.strftime("%Y-%m-%d"), round(float(r.Open), 2), round(float(r.High), 2), round(float(r.Low), 2), round(float(r.Close), 2), int(r.Volume or 0)]
                for d, r in h.iterrows()]
        out["series"].append({"label": label, "ticker": tk, "expiry": exp.isoformat() if exp else None, "bars": bars,
                              "analysis": analyse(h, label if exp else "Continuous front month")})
        print(tk, len(bars), "bars", h.index[-1].date())
    os.makedirs(os.path.dirname(OUT), exist_ok=True)

    def clean(o):
        if isinstance(o, dict):
            return {k: clean(v) for k, v in o.items()}
        if isinstance(o, list):
            return [clean(v) for v in o]
        if isinstance(o, (np.floating, float)):
            return None if (math.isnan(o) or math.isinf(o)) else round(float(o), 4)
        if isinstance(o, (np.integer,)):
            return int(o)
        if isinstance(o, np.bool_):
            return bool(o)
        return o
    json.dump(clean(out), open(OUT, "w"), separators=(",", ":"))
    print("wrote", OUT, os.path.getsize(OUT) // 1024, "KB")


if __name__ == "__main__":
    sys.exit(main())
