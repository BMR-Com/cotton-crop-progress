#!/usr/bin/env python3
"""
USDA/NDMC "Agriculture in Drought" (agindrought.unl.edu): share of COTTON production area in each US
Drought Monitor category, weekly, for the United States and Texas.

Output: data/drought/cotton_area_drought.csv  (location, week, none, d0, d1, d2, d3, d4 = % of cotton area in
D0-D4 / D1-D4 / D2-D4 / D3-D4 / D4, cumulative as published).
"""
import os, re, sys, json
import requests
import pandas as pd

B = "https://agindrought.unl.edu/Home.aspx/"
H = {"User-Agent": "Mozilla/5.0 (BMR-Com research)", "Content-Type": "application/json; charset=utf-8",
     "Referer": "https://agindrought.unl.edu/Table.aspx"}
OUT = "data/drought/cotton_area_drought.csv"
WANT = {"1": ["United States"], "2": ["Texas"]}


def call(fn, **params):
    q = {k: f'"{v}"' for k, v in params.items()}         # the page sends JSON-quoted string arguments
    r = requests.get(B + fn, params=q, headers=H, timeout=90)
    r.raise_for_status()
    return r.json()["d"]


def num(x):
    try:
        return float(str(x).replace(",", "").replace("%", ""))
    except (TypeError, ValueError):
        return None


def main():
    os.makedirs("data/drought", exist_ok=True)
    rows = []
    for ltype, names in WANT.items():
        locs = call("ReturnLocations2020", ltype=ltype, ctype="cotton")
        for loc in locs:
            if loc["Text"].strip() not in names:
                continue
            data = call("ReturnCropData2020", ctype="cotton", ltype=ltype, loc=loc["Value"])
            print(loc["Text"], len(data), "weeks; fields:", list(data[0].keys()) if data else None)
            if data:
                json.dump(data[-3:], open("data/drought/_sample_fields.json", "w"), indent=1)
            for d in data:
                k = {re.sub(r"[^a-z0-9]", "", kk.lower()): v for kk, v in d.items()}
                def pick(prefix):            # 'd1d4', 'd1tod4', 'd1' ... → first key starting with the category
                    for kk, v in k.items():
                        if kk.startswith(prefix) and (prefix == "d4" or "d4" in kk[2:] or kk == prefix):
                            return num(v)
                    return None
                rows.append({"location": "US" if ltype == "1" else "TX",
                             "week": k.get("week") or k.get("date") or k.get("mapdate") or k.get("validstart"),
                             "none": num(k.get("none")), "d0": pick("d0"), "d1": pick("d1"), "d2": pick("d2"), "d3": pick("d3"),
                             "d4": num(k.get("d4"))})
    if not rows:
        print("no data returned"); return 1
    df = pd.DataFrame(rows)
    df["week"] = pd.to_datetime(df.week, errors="coerce").dt.strftime("%Y-%m-%d")
    df = df.dropna(subset=["week"]).drop_duplicates(["location", "week"], keep="last").sort_values(["location", "week"])
    os.makedirs(os.path.dirname(OUT), exist_ok=True)
    df.to_csv(OUT, index=False)
    print(df.groupby("location").week.agg(["min", "max", "count"]))


if __name__ == "__main__":
    sys.exit(main())
