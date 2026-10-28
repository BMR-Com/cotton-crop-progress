#!/usr/bin/env python3
"""USDA AMS daily spot cotton quotations (MyMarketNews report 3804, section "Basis and Bales").

Updates
  data/spot/ams_spot_markets.csv  one row per date x spot market: base-quality prices and basis for
                                  41-4-34 (Strict Low Middling, the futures base) and 31-3-36 (Middling),
                                  basis futures month, bales reported (Aug 2025 onward from the API)
  data/spot/spot41_daily.csv      daily 41-4-34 price, average of the designated markets, since Aug 2000
                                  (history parsed from AMS Annual Cotton Price Statistics, then the API)
Usage: python scripts/fetch_spot.py [days_back=21]   (needs AMS_API_KEY)
"""
import os, sys, datetime as dt
import requests, pandas as pd

KEY = os.environ["AMS_API_KEY"]
URL = "https://marsapi.ams.usda.gov/services/v3.1/reports/3804/Basis and Bales"
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
MK = os.path.join(ROOT, "data", "spot", "ams_spot_markets.csv")
HIST = os.path.join(ROOT, "data", "spot", "spot41_daily.csv")

def fetch(d0, d1):
    rows, d = [], d0
    while d <= d1:                                   # one month per request
        e = min(d1, (d.replace(day=28) + dt.timedelta(days=4)).replace(day=1) - dt.timedelta(days=1))
        r = requests.get(URL, params={"q": f"report_begin_date={d:%m/%d/%Y}:{e:%m/%d/%Y}"}, auth=(KEY, ""), timeout=180)
        r.raise_for_status(); j = r.json()
        rows += j.get("results", []) if isinstance(j, dict) else []
        d = e + dt.timedelta(days=1)
    if not rows: return pd.DataFrame()
    b = pd.DataFrame(rows); b = b[b.commodity == "Upland"]
    return pd.DataFrame({
        "date": pd.to_datetime(b.report_Begin_Date).dt.date, "market": b.spotMarket,
        "price_41_4_34": pd.to_numeric(b["41_4_34_Base_Price"], errors="coerce"),
        "basis_41_4_34_pts": pd.to_numeric(b.basis_41_4_34, errors="coerce"),
        "price_31_3_36": pd.to_numeric(b["31_3_36_Base_Price"], errors="coerce"),
        "basis_31_3_36_pts": pd.to_numeric(b.basis_31_3_36, errors="coerce"),
        "basis_month": b.basis_Month, "bales": pd.to_numeric(b.bales_Reported, errors="coerce")})

def main():
    back = int(sys.argv[1]) if len(sys.argv) > 1 else 21
    os.makedirs(os.path.dirname(MK), exist_ok=True)
    old = pd.read_csv(MK, parse_dates=["date"]) if os.path.exists(MK) else pd.DataFrame()
    d1 = dt.date.today(); d0 = d1 - dt.timedelta(days=back)
    if old.empty: d0 = dt.date(2025, 8, 1)
    new = fetch(d0, d1)
    if new.empty: print("no new spot quotations"); return
    new["date"] = pd.to_datetime(new.date)
    allm = pd.concat([old, new]).drop_duplicates(["date", "market"], keep="last").sort_values(["date", "market"])
    allm.to_csv(MK, index=False); print("markets:", len(allm), allm.date.max().date())
    us = allm[allm.market == "US Average"][["date", "price_41_4_34"]].rename(columns={"price_41_4_34": "spot41"})
    h = pd.read_csv(HIST, parse_dates=["date"]) if os.path.exists(HIST) else pd.DataFrame(columns=["date", "spot41", "source"])
    us = us[us.date > h.date.max()] if len(h) else us
    if len(us):
        h = pd.concat([h, us.assign(source="AMS API 3804")]).sort_values("date")
        h.to_csv(HIST, index=False); print("history:", len(h), h.date.max().date())

if __name__ == "__main__":
    main()
