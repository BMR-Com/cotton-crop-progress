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
    for attempt in range(3):
        try:
            r = requests.get(B + path, params={"key": KEY, **params}, timeout=120)
            if r.status_code == 200:
                return r.json()
            if r.status_code == 413 or "exceeds" in r.text.lower():
                return {"too_big": True}
            if r.status_code == 400 and "no data" in r.text.lower():
                return {"data": []}
        except Exception as e:
            print("  retry:", e)
        time.sleep(3 * (attempt + 1))
    return {"data": []}


def batch(short_desc, year, level, **extra):
    """One request per item/year/level (a few thousand rows at most). If QuickStats refuses a batch
    for exceeding its 50,000-row cap, split it by state."""
    q = dict(short_desc=short_desc, year=year, agg_level_desc=level, source_desc="SURVEY", domain_desc="TOTAL", **extra)
    d = get("api_GET/", format="JSON", **q)
    if d.get("too_big"):
        states = get("get_param_values/", param="state_alpha", **q).get("state_alpha", [])
        rows = []
        for s in states:
            rows += batch(short_desc, year, level, state_alpha=s)
        return rows
    time.sleep(0.3)
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


def merge_save(path, old, rows, years, keys):
    """Replace `years` in the saved file with the freshly fetched rows (called after every year,
    so a long first download keeps its progress even if the job is cut off)."""
    new = pd.DataFrame(rows)
    df = old[~old.year.isin(years)] if len(old) else old
    df = pd.concat([df, new], ignore_index=True) if len(new) else df
    if len(df):
        df = df.drop_duplicates(keys, keep="last").sort_values(keys)
        df.to_csv(path, index=False)
    return df


def main():
    os.makedirs(OUT, exist_ok=True)
    this_year = date.today().year
    gpath, apath = f"{OUT}/ginnings.csv", f"{OUT}/annual.csv"
    old_g = pd.read_csv(gpath) if os.path.exists(gpath) else pd.DataFrame()
    old_a = pd.read_csv(apath) if os.path.exists(apath) else pd.DataFrame()
    if len(old_a) and "period" not in old_a.columns:   # older layout without forecast periods: rebuild
        old_a = pd.DataFrame()
    # full history once, then only the two most recent crop years (revisions + the season in progress)
    have_g = set(old_g.year.unique()) if len(old_g) else set()
    have_a = set(old_a.year.unique()) if len(old_a) else set()
    rg = [y for y in range(FIRST_YEAR, this_year + 1) if y not in have_g or y >= this_year - 1]
    ra = [y for y in range(ANNUAL_FIRST, this_year + 1) if y not in have_a or y >= this_year - 1]
    print("ginnings years:", rg, "annual years:", ra)

    for y in sorted(rg, reverse=True):          # newest first: the season in progress lands first
        g = []
        for k, sd in GIN_ITEMS.items():
            for lvl in ("STATE", "NATIONAL"):
                for r in batch(sd, y, lvl):
                    v = val(r["Value"])
                    if v is None:
                        continue
                    g.append({"year": int(r["year"]), "state": r["state_alpha"] if lvl == "STATE" else "US", "item": k,
                              "period": r["reference_period_desc"], "ref_date": ref_date(int(r["year"]), r["reference_period_desc"]),
                              "value": v, "load_time": r.get("load_time", "")})
        old_g = merge_save(gpath, old_g, g, [y], ["year", "state", "item", "period"])
        print(f"  ginnings {y}: {len(g)} rows", flush=True)

    for y in sorted(ra, reverse=True):
        a = []
        for k, sd in ANNUAL_ITEMS.items():
            for lvl in ("STATE", "NATIONAL"):
                # final ("YEAR") plus in-season forecasts ("YEAR - AUG FORECAST", "YEAR - JUN ACREAGE", ...)
                for r in batch(sd, y, lvl, freq_desc="ANNUAL"):
                    v = val(r["Value"])
                    if v is None:
                        continue
                    a.append({"year": int(r["year"]), "state": r["state_alpha"] if lvl == "STATE" else "US", "item": k,
                              "period": r["reference_period_desc"], "value": v, "load_time": r.get("load_time", "")})
        old_a = merge_save(apath, old_a, a, [y], ["year", "state", "item", "period"])
        print(f"  annual {y}: {len(a)} rows", flush=True)


if __name__ == "__main__":
    sys.exit(main())
