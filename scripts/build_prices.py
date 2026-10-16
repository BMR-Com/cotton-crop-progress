#!/usr/bin/env python3
"""
docs/data/cotton_px.csv — cotton futures history in the Bloomberg generic layout the model tabs read
(Dates m/d/yy; CTMAR1/2, CTMAY1/2, CTJUL1/2, CTDEC1/2 × PX_LAST, OPEN_INT, HIST_CALL_IMP_VOL).

History comes from the Bloomberg export (BMR-Com/cotton-price, to Apr 2026). Later days are filled from
ICE settlements (BMR-Com/softs--options- daily market reports: settle, open interest, at-the-money
implied vol) and, for days without an ICE report, Yahoo Finance contract closes.
Generic n = n-th nearest unexpired contract of that month (rolls on the 8th of the contract month).
"""
import io, os, sys
from datetime import date, timedelta
import numpy as np
import pandas as pd
import requests

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUT = os.path.join(ROOT, "docs", "data", "cotton_px.csv")
SRC = {
    "bbg": ("https://raw.githubusercontent.com/BMR-Com/cotton-price/main/data/cotton_raw.csv", "BBG_LOCAL"),
    "fut": ("https://raw.githubusercontent.com/BMR-Com/softs--options-/main/data/csv/CT_FUTURES.csv", "ICE_FUT_LOCAL"),
    "opt": ("https://raw.githubusercontent.com/BMR-Com/softs--options-/main/data/csv/CT_MASTER.csv", "ICE_OPT_LOCAL"),
}
MONTHS = {"MAR": 3, "MAY": 5, "JUL": 7, "DEC": 12}
YCODE = {3: "H", 5: "K", 7: "N", 12: "Z"}
GEN = [f"CT{m}{n}" for m in MONTHS for n in (1, 2)]
FIELDS = ["PX_LAST", "OPEN_INT", "HIST_CALL_IMP_VOL"]


def read(key, **kw):
    url, env = SRC[key]
    p = os.environ.get(env)
    if p and os.path.exists(p):
        return pd.read_csv(p, **kw)
    r = requests.get(url, timeout=120)
    r.raise_for_status()
    return pd.read_csv(io.StringIO(r.text), **kw)


def generic_contract(d, mon, n):
    """Contract (e.g. 'DEC26') that generic CT<mon><n> refers to on date d."""
    m = MONTHS[mon]
    y = d.year if (d.month, d.day) < (m, 8) else d.year + 1
    y += n - 1
    return f"{mon}{y % 100:02d}"


def atm_iv(opt):
    o = opt[opt.expiry == opt.underlying_fut].copy()
    o["dist"] = (o.strike - o.futures_price).abs()
    o = o[o.implied_vol_pct > 0]
    best = o.sort_values("dist").groupby(["date", "expiry"]).head(2)       # nearest strike, call and put
    return best.groupby(["date", "expiry"]).implied_vol_pct.mean()


def yahoo(contracts, start):
    try:
        import yfinance as yf
    except ImportError:
        return pd.DataFrame()
    out = []
    for c in contracts:
        mon, yy = c[:3], int(c[3:])
        t = f"CT{YCODE[MONTHS[mon]]}{yy:02d}.NYB"
        try:
            h = yf.download(t, start=start, progress=False, auto_adjust=False)
        except Exception as e:
            print("yahoo", t, e); continue
        if h is None or not len(h):
            continue
        close = h["Close"].squeeze()
        out += [{"date": d.date().isoformat(), "contract": c, "settle": round(float(v), 2)} for d, v in close.dropna().items()]
    return pd.DataFrame(out)


def main():
    bbg = read("bbg", low_memory=False, na_values=["#N/A N/A"])
    keep = ["Dates"] + [f"{g} Comdty {f}" for g in GEN for f in FIELDS if f"{g} Comdty {f}" in bbg.columns]
    bbg = bbg[keep]
    bbg["_d"] = pd.to_datetime(bbg.Dates, format="%m/%d/%y")
    last_bbg = bbg._d.max().date()

    fut = read("fut")
    fut = fut[fut.change.abs() <= 8]                     # drop rows from partial (traded-only) reports
    opt = read("opt")
    iv = atm_iv(opt)
    start = last_bbg + timedelta(days=1)
    days = pd.bdate_range(start, date.today() - timedelta(days=1))
    need = sorted({generic_contract(d.date(), mon, n) for d in days for mon in MONTHS for n in (1, 2)})
    yh = yahoo(need, start.isoformat())
    px = {(r.date, r.contract): (r.settle, r.open_interest) for r in fut.itertuples()}
    yx = {(r.date, r.contract): r.settle for r in yh.itertuples()} if len(yh) else {}
    rows = []
    for d in days:
        ds = d.date().isoformat()
        row = {"Dates": f"{d.month}/{d.day}/{d.year % 100:02d}"}
        got = False
        for mon in MONTHS:
            for n in (1, 2):
                c, g = generic_contract(d.date(), mon, n), f"CT{mon}{n}"
                s, oi = px.get((ds, c), (None, None))
                if s is None:
                    s = yx.get((ds, c))
                if s is not None:
                    got = True
                row[f"{g} Comdty PX_LAST"] = s
                row[f"{g} Comdty OPEN_INT"] = oi
                v = iv.get((ds, c))
                row[f"{g} Comdty HIST_CALL_IMP_VOL"] = round(float(v), 2) if v is not None else None
        if got:
            rows.append(row)
    new = pd.DataFrame(rows)
    out = pd.concat([bbg.drop(columns="_d"), new], ignore_index=True)[keep]
    os.makedirs(os.path.dirname(OUT), exist_ok=True)
    out.to_csv(OUT, index=False)
    print(f"cotton_px: Bloomberg to {last_bbg}, +{len(new)} days from ICE/Yahoo → {OUT}")


if __name__ == "__main__":
    sys.exit(main())
