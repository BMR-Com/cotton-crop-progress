#!/usr/bin/env python3
"""
docs/data/market_model.json — inputs shared by the Spreads, Master and Conditional FV tabs.

1. Loan-mechanism floors per contract, from the latest USDA FSA AWP announcement:
   AWP = Far East price − (cost to market + SLM 1-1/16" adjustment).
   Each contract's futures-minus-AWP wedge is its average close over the AWP pricing week (Fri–Thu)
   minus the announced AWP, so the futures level at which the AWP hits the loan rate is
       hard floor  = loan rate + wedge                       (LDP / marketing-loan trigger)
   and the level at which cotton put in the loan in early December has no equity left by the month
   before delivery is
       loan break-even = loan rate + months × (storage + CCC interest on the loan) + wedge.
2. Calendar-spread fair value: one point per season = the spread's average over the 45 days before
   it is rolled (Dec/Mar: Oct 1–Nov 15 …), against the US ending-stocks estimate USDA published in
   that window; linear and 1/x fits, the better one kept; bounded below at full carry.
"""
import io, json, math, os, sys
from datetime import date, datetime, timedelta, timezone
import numpy as np
import pandas as pd
import requests

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUT = os.path.join(ROOT, "docs", "data", "market_model.json")
PX = os.path.join(ROOT, "docs", "data", "cotton_px.csv")
AWP = os.path.join(ROOT, "data", "awp", "awp.csv")
WASDE_URL = "https://raw.githubusercontent.com/BMR-Com/wasde-analytics/main/data/Cotton.json"

LOAN = 55.00                    # 2026 upland base loan rate (SLM 1-1/16")
STORAGE_PER_BALE = 3.00         # $/bale/month, 2026 cap outside AZ/CA
STORAGE = STORAGE_PER_BALE / 480 * 100   # c/lb/month = 0.625
CCC_RATE = 5.0                  # %/yr CCC commodity loan rate
MON = {"MAR": 3, "MAY": 5, "JUL": 7, "DEC": 12}
SEQ = ["MAR", "MAY", "JUL", "DEC"]
# carry months from early-December loan entry to the month before delivery
LOAN_MONTHS = {"MAR": 3, "MAY": 5, "JUL": 7}

# spread windows: (front, back, back-is-next-year?, window start (month, day), window end (month, day),
#                  marketing-year offset of the stocks used, months of carry between the contracts)
SPREADS = {
    "decmar": dict(a="DEC", b="MAR", start=(10, 1), end=(11, 15), cm=3, label="Dec − Mar", win="Oct 1 – Nov 15"),
    "marmay": dict(a="MAR", b="MAY", start=(1, 1), end=(2, 15), cm=2, label="Mar − May", win="Jan 1 – Feb 15"),
    "mayjul": dict(a="MAY", b="JUL", start=(3, 1), end=(4, 15), cm=2, label="May − Jul", win="Mar 1 – Apr 15"),
    "juldec": dict(a="JUL", b="DEC", start=(5, 1), end=(6, 15), cm=5, label="Jul − Dec (old/new)", win="May 1 – Jun 15"),
}


def gen_contract(d, mon, n):
    """Contract (month, year) held by generic CT<mon><n> on date d (rolls on the 8th of the delivery month)."""
    m = MON[mon]
    y = d.year if (d.month, d.day) < (m, 8) else d.year + 1
    return mon, y + n - 1


def col_for(d, mon, year):
    for n in (1, 2):
        if gen_contract(d, mon, n) == (mon, year):
            return f"CT{mon}{n} Comdty PX_LAST"
    return None


def price(px, d, mon, year):
    c = col_for(d, mon, year)
    if c is None or d not in px.index:
        return None
    v = px.at[d, c]
    return None if pd.isna(v) else float(v)


# 3-month T-bill (FRED DTB3) annual averages, used only if FRED cannot be reached
TBILL_FALLBACK = {2000: 5.8, 2001: 3.4, 2002: 1.6, 2003: 1.0, 2004: 1.4, 2005: 3.2, 2006: 4.7, 2007: 4.4, 2008: 1.4,
                  2009: 0.15, 2010: 0.14, 2011: 0.05, 2012: 0.09, 2013: 0.06, 2014: 0.03, 2015: 0.05, 2016: 0.32,
                  2017: 0.93, 2018: 1.94, 2019: 2.06, 2020: 0.36, 2021: 0.04, 2022: 2.02, 2023: 5.07, 2024: 4.97,
                  2025: 4.2, 2026: 3.8}


def load_rates():
    """Daily 3-month T-bill yield (%) from FRED; falls back to annual averages."""
    try:
        r = requests.get("https://fred.stlouisfed.org/graph/fredgraph.csv?id=DTB3", timeout=60)
        r.raise_for_status()
        d = pd.read_csv(io.StringIO(r.text))
        d.columns = ["d", "v"]
        d["d"] = pd.to_datetime(d.d).dt.date
        d["v"] = pd.to_numeric(d.v, errors="coerce")
        s = d.dropna().set_index("d").v
        if len(s) > 1000:
            return s, "FRED DTB3"
    except Exception as e:
        print("FRED unavailable:", e)
    return None, "annual averages"


def rate_on(rates, d0, d1):
    if rates is not None:
        x = rates[(rates.index >= d0) & (rates.index <= d1)]
        if len(x):
            return float(x.mean())
        x = rates[rates.index <= d1]
        if len(x):
            return float(x.iloc[-1])
    return TBILL_FALLBACK.get(d1.year, 4.0)


def load_px():
    p = pd.read_csv(PX, low_memory=False)
    p["d"] = pd.to_datetime(p.Dates, format="%m/%d/%y").dt.date
    return p.set_index("d").sort_index()


def load_wasde():
    loc = os.environ.get("WASDE_LOCAL")
    d = json.load(open(loc)) if loc else requests.get(WASDE_URL, timeout=90).json()
    rows = []
    for key, vals in d["data"].items():
        reg, attr, my = key.split("||")
        if reg != "United States" or attr not in ("Ending Stocks", "Ending stocks"):
            continue
        for v in vals:
            if v.get("value") is not None:
                rows.append((my, date(v["forecastYear"], v["forecastMonth"], 1), float(v["value"])))
    w = pd.DataFrame(rows, columns=["my", "rd", "val"])
    w["y0"] = w.my.str[:4].astype(int)
    return w.drop_duplicates(["my", "rd"], keep="last"), d.get("lastUpdated")


# ───────────────────────── floors ─────────────────────────
def floors(px, awp):
    a = awp.dropna(subset=["awp"]).sort_values("announce_date").iloc[-1]
    ann = date.fromisoformat(str(a.announce_date))
    wk = [ann - timedelta(days=k) for k in range(6, -1, -1)]          # Fri … Thu pricing week
    adj = round(float(a.fe_price) - float(a.awp), 2) if pd.notna(a.fe_price) else 23.46
    carry = STORAGE + LOAN * CCC_RATE / 1200
    # active contracts on the announcement date
    out, d0 = [], ann
    for n in range(1, 3):
        for mon in SEQ:
            out.append(gen_contract(d0, mon, n))
    out = sorted(set(out), key=lambda x: (x[1], MON[x[0]]))[:5]
    res = []
    for mon, yr in out:
        vals = [price(px, d, mon, yr) for d in wk if d in px.index]
        vals = [v for v in vals if v is not None]
        if not vals:
            continue
        fut = float(np.mean(vals))
        wedge = fut - float(a.awp)
        hard = LOAN + wedge
        m = LOAN_MONTHS.get(mon) if (yr == ann.year + 1 or (mon != "DEC" and yr == ann.year)) else None
        soft = hard + m * carry if m else None
        res.append({"contract": f"{mon.title()} {yr % 100:02d}", "mon": mon, "year": yr, "fut_week_avg": round(fut, 2),
                    "wedge": round(wedge, 2), "hard": round(hard, 2), "loan_months": m,
                    "loan_breakeven": round(soft, 2) if soft else None,
                    "awp_breakeven": round(LOAN + m * carry, 2) if m else None,
                    "basis_fe": round(float(a.fe_price) - fut, 2) if pd.notna(a.fe_price) else None})
    hist = awp.dropna(subset=["awp"]).sort_values("announce_date").tail(12)
    return {"announce_date": ann.isoformat(), "effective": [str(a.effective_start), str(a.effective_end)],
            "awp": float(a.awp), "fe_price": float(a.fe_price) if pd.notna(a.fe_price) else None, "adjustments": adj,
            "cost_to_market": 20.51, "slm_adj": 2.95, "ldp": float(a.ldp or 0), "loan": LOAN,
            "storage_bale_month": STORAGE_PER_BALE, "storage": round(STORAGE, 4), "ccc_rate": CCC_RATE,
            "carry_per_month": round(carry, 4), "pricing_week": [wk[0].isoformat(), wk[-1].isoformat()],
            "contracts": res,
            "history": [{"date": str(r.announce_date), "awp": float(r.awp), "fe": None if pd.isna(r.fe_price) else float(r.fe_price)}
                        for r in hist.itertuples()]}


# ───────────────────────── spread fair value ─────────────────────────
def fit(x, y):
    x, y = np.asarray(x, float), np.asarray(y, float)
    best = None
    for form in ("lin", "inv"):
        xx = x if form == "lin" else 1 / np.maximum(x, 0.3)
        b, a_ = np.polyfit(xx, y, 1)
        res = y - (a_ + b * xx)
        r2 = 1 - (res ** 2).sum() / ((y - y.mean()) ** 2).sum()
        sig = math.sqrt((res ** 2).sum() / max(len(x) - 2, 1))
        if best is None or r2 > best["r2"]:
            best = {"form": form, "a": float(a_), "b": float(b), "r2": float(r2), "sigma": sig}
    return best


def fv_at(f, st):
    return f["a"] + f["b"] * (st if f["form"] == "lin" else 1 / max(st, 0.3))


def fit_funding(x, fin, y):
    """Robust (Huber) fit  y = a + b / stocks + c × funding cost  — squeezes like 2011 and 2022 are down-weighted."""
    x, fin, y = (np.asarray(v, float) for v in (x, fin, y))
    X = np.c_[np.ones(len(x)), 1 / np.maximum(x, 0.3), fin]
    w = np.ones(len(y))
    for _ in range(80):
        W = np.sqrt(w)
        b = np.linalg.lstsq(X * W[:, None], y * W, rcond=None)[0]
        r = y - X @ b
        sc = np.median(np.abs(r)) / 0.6745 or 1.0
        u = np.abs(r) / (1.345 * sc)
        w = np.where(u <= 1, 1.0, 1 / u)
    r = y - X @ b
    wr2 = 1 - (w * r ** 2).sum() / (w * (y - np.average(y, weights=w)) ** 2).sum()
    return {"a": float(b[0]), "b": float(b[1]), "c": float(b[2]), "r2_weighted": float(wr2),
            "sigma": float(np.median(np.abs(r)) / 0.6745), "weights": [round(float(v), 2) for v in w]}


def spread_fv(px, w, today, rates):
    out = {}
    for sid, S in SPREADS.items():
        pts = []
        for yr in range(2000, today.year + 1):
            # yr = calendar year of the window; stocks are for the marketing year the front contract trades
            if sid == "decmar":
                a_c, b_c, y0 = ("DEC", yr), ("MAR", yr + 1), yr
            else:
                a_c, b_c, y0 = (S["a"], yr), (S["b"], yr), yr - 1
            ws, we = date(yr, *S["start"]), date(yr, *S["end"])
            days = [d for d in px.index if ws <= d <= we and d <= today]
            vals, backs = [], []
            for d in days:
                va, vb = price(px, d, *a_c), price(px, d, *b_c)
                if va is not None and vb is not None:
                    vals.append(va - vb); backs.append(vb)
            if len(vals) < 10:
                continue
            my = f"{y0}/{str(y0 + 1)[-2:]}"
            rel = w[(w.my == my) & (w.rd >= date(ws.year, ws.month, 1)) & (w.rd <= we)]
            if not len(rel):
                rel = w[(w.my == my) & (w.rd <= we)].tail(1)
            if not len(rel):
                continue
            complete = we < today
            rt = rate_on(rates, ws, min(we, today))
            fin = -(float(np.mean(backs)) * rt / 1200) * S["cm"]          # funding cost of carrying to the back month, c/lb
            pts.append({"yr": yr, "my": my, "x": round(float(rel.val.mean()), 3), "y": round(float(np.mean(vals)), 3),
                        "rate": round(rt, 2), "fin": round(fin, 3),
                        "n": len(vals), "complete": complete, "contracts": f"{a_c[0].title()}{a_c[1] % 100:02d}/{b_c[0].title()}{b_c[1] % 100:02d}"})
        hist = [p for p in pts if p["complete"] and p["yr"] >= 2008]
        f = fit([p["x"] for p in hist], [p["y"] for p in hist]) if len(hist) >= 8 else None
        ff = fit_funding([p["x"] for p in hist], [p["fin"] for p in hist], [p["y"] for p in hist]) if len(hist) >= 8 else None
        if ff:
            for p, wt in zip(hist, ff["weights"]):
                p["weight"] = wt
        out[sid] = {"label": S["label"], "window": S["win"], "carry_months": S["cm"], "fit": f, "fit_funding": ff, "points": pts,
                    "partial": next((p for p in pts if not p["complete"]), None),
                    "years": [hist[0]["yr"], hist[-1]["yr"]] if hist else None,
                    "median": float(np.median([p["y"] for p in hist])) if hist else None,
                    "share_carry": float(np.mean([p["y"] < 0 for p in hist])) if hist else None}
    return out


def main():
    px = load_px()
    awp = pd.read_csv(AWP)
    w, wl = load_wasde()
    today = px.index.max()
    cur_my = w.sort_values("rd").my.max()
    cur_stocks = float(w[w.my == cur_my].sort_values("rd").val.iloc[-1])
    F = floors(px, awp)
    rates, rsrc = load_rates()
    rate_now = rate_on(rates, today - timedelta(days=10), today)
    SP = spread_fv(px, w, today, rates)
    # current market per spread (front pair now trading) and FV at the latest WASDE stocks
    for sid, S in SP.items():
        a_mon, b_mon = SPREADS[sid]["a"], SPREADS[sid]["b"]
        a_c = gen_contract(today, a_mon, 1)
        b_c = gen_contract(today, b_mon, 1)
        if sid == "juldec":
            b_c = ("DEC", a_c[1])
        elif (b_c[1], MON[b_c[0]]) <= (a_c[1], MON[a_c[0]]):
            b_c = gen_contract(today, b_mon, 2)
        va, vb = price(px, today, *a_c), price(px, today, *b_c)
        S["market"] = round(va - vb, 2) if va is not None and vb is not None else None
        S["market_contracts"] = f"{a_c[0].title()}{a_c[1] % 100:02d}/{b_c[0].title()}{b_c[1] % 100:02d}"
        back = vb if vb is not None else 80.0
        S["full_carry"] = round(-(F["storage"] + back * rate_now / 1200) * S["carry_months"], 2)
        S["back_price"] = round(back, 2)
        S["fin_now"] = round(-(back * rate_now / 1200) * S["carry_months"], 3)
        if S["fit"]:
            S["fv_wasde_stocks_only"] = round(fv_at(S["fit"], cur_stocks), 2)
        ff = S.get("fit_funding")
        if ff:
            raw = ff["a"] + ff["b"] / max(cur_stocks, 0.3) + ff["c"] * S["fin_now"]
            S["fv_wasde_raw"] = round(raw, 2)
            S["fv_wasde"] = round(max(raw, S["full_carry"]), 2)
    out = {"generated": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"), "price_date": today.isoformat(),
           "wasde": {"my": cur_my, "end_stocks": cur_stocks, "updated": wl}, "rate": {"now": round(rate_now, 2), "source": rsrc},
           "floors": F, "spreads": SP}
    json.dump(out, open(OUT, "w"), indent=1, default=lambda o: None)
    print("AWP", F["awp"], "effective", F["effective"])
    for c in F["contracts"]:
        print(f"  {c['contract']}: fut {c['fut_week_avg']} wedge {c['wedge']} hard {c['hard']} loan b/e {c['loan_breakeven']}")
    for sid, S in SP.items():
        f = S["fit"]
        print(f"  {sid}: n={len([p for p in S['points'] if p['complete']])} {f and (f['form'], round(f['r2'], 2), round(f['sigma'], 2))} "
              f"FV@{cur_stocks} {S.get('fv_wasde')} (raw {S.get('fv_wasde_raw')}, stocks-only {S.get('fv_wasde_stocks_only')}) wr2 {S['fit_funding'] and round(S['fit_funding']['r2_weighted'],2)} carry {S['full_carry']} mkt {S['market']} {S['market_contracts']} "
              f"partial {S['partial'] and (S['partial']['y'], S['partial']['n'])}")


if __name__ == "__main__":
    sys.exit(main())
