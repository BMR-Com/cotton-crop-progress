#!/usr/bin/env python3
"""
USDA AMS cotton classing (MyMarketNews report 1654, "Cotton Weekly Quality Report by State").

Each weekly report gives, by gin state and for Upland/Pima:
  Summary         bales classed (week, season), % tenderable, % CCC-loan eligible, gin count
  Other attrib.   staple, mike, strength, length, uniformity, leaf, extraneous matter (% of bales)
  Color Leaf      color-grade × leaf-grade distribution (% of bales)

Saved:
  data/classing/summary.csv     every week since 2015 (small; drives the weekly crop model)
  data/classing/attributes_<crop year>.csv  every week, Upland, % of bales by attribute bucket (+ averages)
  data/classing/colorleaf_<crop year>.csv   every week, Upland, color × leaf % of bales
Requests are batched by month (well under the 100,000-row API cap) and the current crop year is
refreshed on every run.
"""
import os, sys, time
from datetime import date, timedelta
import requests
import pandas as pd

KEY = os.environ["AMS_API_KEY"]
URL = "https://marsapi.ams.usda.gov/services/v3.1/reports/1654"
OUT = "data/classing"
FIRST = date(2015, 7, 1)


def crop_year(d):            # classing season runs Jul–Jun; Aug 2026 report = 2026 crop
    return d.year if d.month >= 7 else d.year - 1


def num(x):
    try:
        return float(str(x).replace(",", ""))
    except (TypeError, ValueError):
        return None


def month_windows(start, end):
    d = date(start.year, start.month, 1)
    while d <= end:
        nxt = date(d.year + (d.month == 12), d.month % 12 + 1, 1)
        yield d, min(nxt - timedelta(days=1), end)
        d = nxt


def fetch(a, b):
    q = f"report_begin_date={a:%m/%d/%Y}:{b:%m/%d/%Y}"
    for attempt in range(5):
        try:
            r = requests.get(URL, params={"q": q, "allSections": "true"}, auth=(KEY, ""), timeout=300)
            if r.status_code == 200:
                j = r.json()
                return j if isinstance(j, list) else [j]
            print("  http", r.status_code, r.text[:200])
        except Exception as e:
            print("  retry", e)
        time.sleep(5 * (attempt + 1))
    return []


def parse(secs, S, A, C):
    n = 0
    for sec in secs:
        name = (sec.get("reportSection") or "").lower()
        for r in sec.get("results") or []:
            try:
                wkd = pd.to_datetime(r.get("report_end_date")).date()
            except Exception:
                continue
            base = {"week_end": wkd.isoformat(), "crop_year": crop_year(wkd), "commodity": r.get("commodity"), "state": r.get("gin_State")}
            if name == "summary":
                S.append({**base, "period": (r.get("report_Period") or "").lower(), "gins": num(r.get("gin_Count")),
                          "bales": num(r.get("bales_Count")), "tenderable_pct": num(r.get("tenderable_Percent")),
                          "eligible_pct": num(r.get("eligible_Percent"))})
            elif base["commodity"] != "Upland":
                continue
            elif "color" in name:
                C.append({**base, "color": r.get("color_Grade"), "leaf": r.get("leaf_Grade"),
                          "week_pct": num(r.get("week_Percent")), "season_pct": num(r.get("season_Percent"))})
            else:
                A.append({**base, "attribute": r.get("attribute_Name"), "bucket": r.get("attribute_Value"),
                          "week_pct": num(r.get("week_Percent")), "season_pct": num(r.get("season_Percent"))})
            n += 1
    return n


KEYS = {"summary": ["week_end", "commodity", "state", "period"], "attributes": ["week_end", "state", "attribute", "bucket"],
        "colorleaf": ["week_end", "state", "color", "leaf"]}


def save_year(cy, S, A, C):
    """Write one crop year: replace its rows in summary.csv, rewrite its detail files."""
    spath = f"{OUT}/summary.csv"
    old = pd.read_csv(spath) if os.path.exists(spath) else pd.DataFrame()
    s = pd.DataFrame(S)
    if len(s):
        s = s[s.crop_year == cy]
        o = old[old.crop_year != cy] if len(old) else old
        s = pd.concat([o, s], ignore_index=True).drop_duplicates(KEYS["summary"], keep="last").sort_values(KEYS["summary"])
        s.to_csv(spath, index=False)
    for k, rows in (("attributes", A), ("colorleaf", C)):
        df = pd.DataFrame(rows)
        if len(df):
            df = df[(df.crop_year == cy) & (df.week_pct.notna() | df.season_pct.notna())]
            df.drop_duplicates(KEYS[k], keep="last").sort_values(KEYS[k]).to_csv(f"{OUT}/{k}_{cy}.csv", index=False)


def main():
    os.makedirs(OUT, exist_ok=True)
    today = date.today()
    spath = f"{OUT}/summary.csv"
    old_s = pd.read_csv(spath) if os.path.exists(spath) else pd.DataFrame()
    have = set(old_s.crop_year.unique()) if len(old_s) else set()
    cur = crop_year(today)
    years = [cur] + [y for y in range(cur - 1, FIRST.year - 1, -1) if y not in have]   # season in progress first
    print("crop years:", years, flush=True)
    for cy in years:
        S, A, C = [], [], []
        for a, b in month_windows(date(cy, 7, 1), min(date(cy + 1, 6, 30), today)):
            n = parse(fetch(a, b), S, A, C)
            print(f"  {a:%Y-%m}: {n} rows", flush=True)
            time.sleep(0.3)
        save_year(cy, S, A, C)
        print(f"crop year {cy} saved: {len(S)} summary rows", flush=True)


if __name__ == "__main__":
    sys.exit(main())
