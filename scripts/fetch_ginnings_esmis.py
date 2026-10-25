#!/usr/bin/env python3
"""
NASS Cotton Ginnings report archive on ESMIS (releases back to 1987): running bales ginned by state
as of each report date, for every crop year the report shows (usually the current and 1–3 prior).

Fills the crop years that QuickStats lacks (state ginnings there start with the 2014 crop).
Output: data/ginning/ginnings_esmis.csv  year, state, item (upland_rb | all_rb), period, ref_date, value, release
Env: GIN_ESMIS_FIRST (default 2000) — oldest crop year kept; GIN_ESMIS_PAGES (default 90).
"""
import os, re, sys, time
from datetime import date
from urllib.parse import urljoin
import requests
import pandas as pd

PUB = "https://esmis.nal.usda.gov/concern/publications/q524jn76v"
OUT = "data/ginning/ginnings_esmis.csv"
DONE = "data/ginning/esmis_processed.txt"
FIRST = int(os.environ.get("GIN_ESMIS_FIRST", "2000"))
PAGES = int(os.environ.get("GIN_ESMIS_PAGES", "90"))
MONTHS = "January February March April May June July August September October November December".split()
ST = {"Alabama": "AL", "Arizona": "AZ", "Arkansas": "AR", "California": "CA", "Florida": "FL", "Georgia": "GA",
      "Kansas": "KS", "Louisiana": "LA", "Mississippi": "MS", "Missouri": "MO", "New Mexico": "NM",
      "North Carolina": "NC", "Oklahoma": "OK", "South Carolina": "SC", "Tennessee": "TN", "Texas": "TX",
      "Virginia": "VA", "United States": "US", "Other States": "OT", "Other states": "OT"}
S = requests.Session()
S.headers["User-Agent"] = "Mozilla/5.0 (BMR-Com research data fetch)"
NOTES = []


def note(m):
    print(m)
    NOTES.append(m)


def get(u, **kw):
    for a in range(3):
        try:
            return S.get(u, timeout=60, **kw)
        except Exception as e:
            print("  retry", u, e)
            time.sleep(3 * (a + 1))
    return None


def period_of(mon, day):
    m = MONTHS[mon - 1][:3].upper()
    return ("FIRST OF " if day <= 5 else "MID ") + m


def ref(crop, mon, day):
    return date(crop + (1 if mon <= 7 else 0), mon, 1 if day <= 5 else 15)


def num(x):
    x = x.replace(",", "").strip()
    return float(x) if re.fullmatch(r"\d+(\.\d+)?", x) else None


def parse(text, release):
    """Rows of the state table(s): 'State ....: v1 v2 ...' under a header naming the crop years."""
    rows = []
    # date the table refers to, e.g. 'October 15, 2023-2026', 'prior to October 1', 'October 1'
    m = re.search(r"(?:prior to|as of|:)\s*(" + "|".join(MONTHS) + r")\s+(\d{1,2})", text)
    if not m:
        return rows
    mon, day = MONTHS.index(m.group(1)) + 1, int(m.group(2))
    lines = text.splitlines()
    crops, item = None, "all_rb"
    for ln in lines:
        cy = re.findall(r"(\d{4})\s+[Cc]rop", ln)
        if len(cy) >= 2:
            crops = [int(c) for c in cy]
            item = "all_rb"
            continue
        s = ln.strip()
        if crops is None:
            continue
        if re.match(r"^Upland\b", s):
            item = "upland_rb"; continue
        if re.match(r"^(American )?Pima\b", s):
            item = "pima_rb"; continue
        if re.match(r"^All( cotton)?\b", s, re.I) and ":" in s:
            item = "all_rb"
        mm = re.match(r"^([A-Z][A-Za-z ]+?)\s*\.{2,}\s*:(.*)$", s)
        if not mm or mm.group(1).strip() not in ST:
            continue
        vals = re.findall(r"\(NA\)|\(D\)|\(X\)|\(Z\)|-|[\d,]+", mm.group(2))
        if len(vals) != len(crops):
            continue
        for c, v in zip(crops, vals):
            v = num(v)
            if v is None or c < FIRST:
                continue
            rows.append({"year": c, "state": ST[mm.group(1).strip()], "item": item, "period": period_of(mon, day),
                         "ref_date": ref(c, mon, day).isoformat(), "value": v, "release": release})
        if mm.group(1).strip() == "United States" and item in ("upland_rb", "all_rb"):
            pass
    return rows


def releases():
    out = []
    for p in range(PAGES):
        r = get(PUB, params={"page": p})
        if r is None or r.status_code != 200:
            note(f"listing page {p}: {None if r is None else r.status_code}")
            break
        links = re.findall(r'href="([^"]*release-files[^"]+\.(?:txt|TXT|pdf|PDF))"', r.text)
        if not links:
            break
        out += [urljoin(PUB, l) for l in links]
    # one file per release: prefer .txt
    by = {}
    for u in out:
        k = u.rsplit(".", 1)[0]
        if k not in by or u.lower().endswith(".txt"):
            by[k] = u
    note(f"ESMIS ginnings: {len(by)} releases listed over {p + 1} pages")
    return list(by.values())


def main():
    old = pd.read_csv(OUT) if os.path.exists(OUT) else pd.DataFrame()
    done = set(open(DONE).read().split()) if os.path.exists(DONE) else set()
    rows, n, bad = [], 0, 0
    for u in releases():
        if u in done:
            continue
        if not u.lower().endswith(".txt"):
            continue                                   # pdf-only releases are recent; QuickStats covers them
        r = get(u)
        if r is None or r.status_code != 200:
            continue
        t = r.content.decode("latin-1", "replace")
        rm = re.search(r"Released\s+(" + "|".join(MONTHS) + r")\s+(\d{1,2}),\s+(\d{4})", t)
        rel = date(int(rm.group(3)), MONTHS.index(rm.group(1)) + 1, int(rm.group(2))).isoformat() if rm else ""
        x = parse(t, rel)
        if not x:
            bad += 1
            if bad <= 3:
                note(f"no rows: {u} | {' / '.join(l.strip() for l in t.splitlines()[:40] if l.strip())[:700]}")
        rows += x
        done.add(u)
        n += 1
        time.sleep(0.2)
    new = pd.DataFrame(rows)
    df = pd.concat([old, new], ignore_index=True) if len(old) else new
    if len(df):
        # the latest release that reports a crop/date wins (revisions)
        df = df.sort_values("release").drop_duplicates(["year", "state", "item", "period"], keep="last")
        df = df.sort_values(["year", "state", "item", "ref_date"])
        os.makedirs(os.path.dirname(OUT), exist_ok=True)
        df.to_csv(OUT, index=False)
    with open(DONE, "w") as f:
        f.write("\n".join(sorted(done)))
    cov = df[df.state == "TX"].groupby("year").period.nunique().to_dict() if len(df) else {}
    note(f"parsed {n} releases ({bad} without a table); TX report dates by crop year: {cov}")
    for m in NOTES[-20:]:
        print("::notice::" + m[:900])


if __name__ == "__main__":
    sys.exit(main())
