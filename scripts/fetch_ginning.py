#!/usr/bin/env python3
"""
Download NASS Cotton Ginnings (by state, semi-monthly) plus annual acreage / production / yield
from QuickStats, in batches that stay under the API's 50,000-record limit.

Batches: one request per (data item, marketing year, level). Each batch is checked with
get_counts first; anything over the limit is split by state.

Output (tidy CSVs, one row per observation):
  data/ginning/ginnings.csv   year, state, item, period, ref_date, value
  data/ginning/annual.csv     year, state, item, period (YEAR = final, or the in-season forecast label), value
"""
import os, sys, time, json
from datetime import date
import requests
import pandas as pd

KEY = os.environ["USDA_API_KEY"]
B = "https://quickstats.nass.usda.gov/api/"
LIMIT = 50_000
OUT = "data/ginning"
FIRST_YEAR = int(os.environ.get("GIN_FIRST_YEAR", "2014"))      # ginnings: 2014 crop onward
ANNUAL_FIRST = int(os.environ.get("ANNUAL_FIRST_YEAR", "2001"))  # acreage / yield history for the condition model

GIN_ITEMS = {
    "all_rb":     "COTTON - GINNED BALES, MEASURED IN RUNNING BALES",
    "upland_rb":  "COTTON, UPLAND - GINNED BALES, MEASURED IN RUNNING BALES",
    "pima_rb":    "COTTON, PIMA - GINNED BALES, MEASURED IN RUNNING BALES",
    "all_480":    "COTTON - GINNED BALES, MEASURED IN 480 LB BALES",
    "all_lbbale": "COTTON - GINNED BALES, MEASURED IN LB / BALE, AVG",
}
ANNUAL_ITEMS = {
    "upland_planted":   "COTTON, UPLAND - ACRES PLANTED",
    "upland_harvested": "COTTON, UPLAND - ACRES HARVESTED",
    "upland_yield":     "COTTON, UPLAND - YIELD, MEASURED IN LB / ACRE",
    "upland_prod":      "COTTON, UPLAND - PRODUCTION, MEASURED IN 480 LB BALES",
    "all_prod":         "COTTON - PRODUCTION, MEASURED IN 480 LB BALES",
    "pima_prod":        "COTTON, PIMA - PRODUCTION, MEASURED IN 480 LB BALES",
}
MON = {m: i for i, m in enumerate("JAN FEB MAR APR MAY JUN JUL AUG SEP OCT NOV DEC".split(), 1)}


def get(path, **params):
    for attempt in range(5):
        try:
            r = requests.get(B + path, params={"key": KEY, **params}, timeout=180)
            if r.status_code == 200:
                return r.json()
            if r.status_code in (400, 413) and "exceeds" in r.text.lower():
                return {"too_big": True}
            if r.status_code == 400 and "no data" in r.text.lower():
                return {"data": []}
        except Exception as e:
            print("  retry:", e)
        time.sleep(3 * (attempt + 1))
    return {"data": []}


def batch(short_desc, year, level, **extra):
    q = dict(short_desc=short_desc, year=year, agg_level_desc=level, source_desc="SURVEY", domain_desc="TOTAL", **extra)
    n = get("get_counts/", **q).get("count", 0)
    if n == 0:
        return []
    if n > LIMIT:            # split by state if a batch would hit the cap
        states = get("get_param_values/", param="state_alpha", **q).get("state_alpha", [])
        rows = []
        for s in states:
            rows += batch(short_desc, year, level, state_alpha=s)
        return rows
    d = get("api_GET/", format="JSON", **q)
    time.sleep(0.4)
    return d.get("data", [])


def ref_date(year, period):
    """Map NASS ginning period labels to a date in the marketing year (crop year = `year`)."""
    p = period.upper().strip()
    words = p.split()
    for m, i in MON.items():
        if m in words:
            yr = year + (1 if i <= 7 else 0)          # Jan–Jul belong to the following calendar year
            day = 1 if p.startswith("FIRST") else 15 if p.startswith("MID") else 28   # bare month = season total
            return date(yr, i, day).isoformat()
    return None


def val(v):
    try:
        return float(str(v).replace(",", ""))
    except ValueError:
        return None


def main():
    os.makedirs(OUT, exist_ok=True)
    this_year = date.today().year
    years = range(FIRST_YEAR, this_year + 1)
    gpath, apath = f"{OUT}/ginnings.csv", f"{OUT}/annual.csv"
    old_g = pd.read_csv(gpath) if os.path.exists(gpath) else pd.DataFrame()
    old_a = pd.read_csv(apath) if os.path.exists(apath) else pd.DataFrame()
    if len(old_a) and "period" not in old_a.columns:   # older layout without forecast periods: rebuild
        old_a = pd.DataFrame()
    # full history once, then only the two most recent crop years (revisions + the season in progress)
    have = set(old_g.year.unique()) if len(old_g) else set()
    refresh = [y for y in years if y not in have or y >= this_year - 1]
    print("years to fetch:", refresh)

    g = []
    for y in refresh:
        for k, sd in GIN_ITEMS.items():
            for lvl in ("STATE", "NATIONAL"):
                rows = batch(sd, y, lvl)
                for r in rows:
                    v = val(r["Value"])
                    if v is None:
                        continue
                    g.append({"year": int(r["year"]), "state": r["state_alpha"] if lvl == "STATE" else "US", "item": k,
                              "period": r["reference_period_desc"], "ref_date": ref_date(int(r["year"]), r["reference_period_desc"]),
                              "value": v, "load_time": r.get("load_time", "")})
        print(f"  ginnings {y}: total rows so far {len(g)}")
    have_a = set(old_a.year.unique()) if len(old_a) else set()
    refresh_a = [y for y in range(ANNUAL_FIRST, this_year + 1) if y not in have_a or y >= this_year - 1]
    a = []
    for y in refresh_a:
        for k, sd in ANNUAL_ITEMS.items():
            for lvl in ("STATE", "NATIONAL"):
                # final ("YEAR") plus in-season forecasts ("YEAR - AUG FORECAST", "YEAR - JUN ACREAGE", ...)
                for r in batch(sd, y, lvl, freq_desc="ANNUAL"):
                    v = val(r["Value"])
                    if v is None:
                        continue
                    a.append({"year": int(r["year"]), "state": r["state_alpha"] if lvl == "STATE" else "US", "item": k,
                              "period": r["reference_period_desc"], "value": v, "load_time": r.get("load_time", "")})
    G = pd.DataFrame(g); A = pd.DataFrame(a)
    if len(old_g):
        G = pd.concat([old_g[~old_g.year.isin(refresh)], G], ignore_index=True)
    if len(old_a):
        A = pd.concat([old_a[~old_a.year.isin(refresh_a)], A], ignore_index=True)
    if len(G):
        G = G.drop_duplicates(["year", "state", "item", "period"], keep="last").sort_values(["year", "state", "item", "ref_date"])
        G.to_csv(gpath, index=False)
    if len(A):
        A = A.drop_duplicates(["year", "state", "item", "period"], keep="last").sort_values(["year", "state", "item", "period"])
        A.to_csv(apath, index=False)
    print(f"ginnings: {len(G)} rows, annual: {len(A)} rows")


if __name__ == "__main__":
    sys.exit(main())
