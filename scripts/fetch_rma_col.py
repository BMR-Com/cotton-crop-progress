#!/usr/bin/env python3
"""
USDA RMA Cause of Loss (Summary of Business, by month of loss): insured cotton losses.

Keeps only cotton (commodity name contains COTTON) and aggregates to keep the file small:
  data/rma/cotton_col.csv   year, state, county (Texas only; '' elsewhere), month, cause,
                            determined_acres, indemnity, policies_indemnified, planted_acres
Source: one pipe-delimited zip per crop year, linked from rma.usda.gov "Cause of Loss" page.
Env: RMA_FIRST_YEAR (default 2001). Completed years are kept; the two latest are refreshed each run.
"""
import io, os, re, sys, time, zipfile
from datetime import date
from urllib.parse import urljoin
import requests
import pandas as pd

OUT = "data/rma/cotton_col.csv"
PAGES = ["https://www.rma.usda.gov/tools-reports/summary-business/cause-loss",
         "https://rma.usda.gov/tools-reports/summary-business/cause-loss",
         "https://old.rma.usda.gov/en/Information-Tools/Summary-of-Business/Cause-of-Loss"]
PATTERNS = ["https://pubfs-rma.fpac.usda.gov/pub/Web_Data_Files/Summary_of_Business/cause_of_loss/colsom_{y}.zip",
            "https://pubfs-rma.fpac.usda.gov/pub/Web_Data_Files/Summary_of_Business/cause_of_loss/colsom{y}.zip",
            "https://www.rma.usda.gov/sites/default/files/information-tools/colsom_{y}.zip"]
FIRST = int(os.environ.get("RMA_FIRST_YEAR", "2001"))
COLS = ["year", "state_code", "state", "county_code", "county", "commodity_code", "commodity", "plan_code", "plan",
        "coverage", "stage", "cause_code", "cause", "month", "month_name", "year_of_loss", "policies_earning",
        "policies_indemnified", "planted_acres", "endorsed_acres", "liability", "premium", "producer_premium",
        "subsidy", "state_subsidy", "add_subsidy", "efa", "determined_acres", "indemnity", "loss_ratio"]
S = requests.Session()
S.headers["User-Agent"] = "Mozilla/5.0 (BMR-Com research data fetch)"
NOTES = []


def note(m):
    print(m)
    NOTES.append(m)


def links():
    found = {}
    for p in PAGES:
        try:
            r = S.get(p, timeout=60)
        except Exception as e:
            note(f"page {p}: {e}")
            continue
        if r.status_code != 200:
            note(f"page {p}: {r.status_code}")
            continue
        for h in re.findall(r'href="([^"]+\.zip)"', r.text, re.I):
            m = re.search(r"col.*?(\d{4})", h, re.I)
            if m and "som" in h.lower():
                found.setdefault(int(m.group(1)), urljoin(p, h))
        if found:
            break
    note(f"cause-of-loss zip links found on page: {len(found)} ({min(found) if found else '-'}–{max(found) if found else '-'})")
    return found


def fetch_year(y, found):
    cands = ([found[y]] if y in found else []) + [p.format(y=y) for p in PATTERNS]
    for u in cands:
        try:
            r = S.get(u, timeout=300)
        except Exception as e:
            print("  ", u, e)
            continue
        if r.status_code == 200 and r.content[:2] == b"PK":
            return r.content, u
    return None, None


def parse(blob):
    z = zipfile.ZipFile(io.BytesIO(blob))
    name = max((n for n in z.namelist() if not n.endswith("/")), key=lambda n: z.getinfo(n).file_size)
    raw = z.read(name).decode("latin-1")
    df = pd.read_csv(io.StringIO(raw), sep="|", header=None, dtype=str, engine="python", on_bad_lines="skip")
    df = df.iloc[:, :len(COLS)]
    df.columns = COLS[:df.shape[1]]
    for c in df.columns:
        df[c] = df[c].str.strip()
    df = df[df.commodity.str.upper().str.contains("COTTON", na=False)].copy()
    for c in ("year", "month", "policies_indemnified", "planted_acres", "determined_acres", "indemnity"):
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df["county"] = df.county.where(df.state == "TX", "")
    g = (df.groupby(["year", "state", "county", "month", "cause"], dropna=False)
           [["determined_acres", "indemnity", "policies_indemnified", "planted_acres"]].sum().reset_index())
    return g


def main():
    os.makedirs(os.path.dirname(OUT), exist_ok=True)
    old = pd.read_csv(OUT, dtype={"county": str}) if os.path.exists(OUT) else pd.DataFrame()
    have = set(old.year.unique()) if len(old) else set()
    this = date.today().year
    years = [y for y in range(FIRST, this + 1) if y not in have or y >= this - 1]
    found = links()
    got = []
    for y in sorted(years, reverse=True):
        blob, u = fetch_year(y, found)
        if blob is None:
            note(f"{y}: no file")
            continue
        try:
            g = parse(blob)
        except Exception as e:
            note(f"{y}: parse failed {e}")
            continue
        got.append(g)
        tx = g[g.state == "TX"]
        note(f"{y}: {len(g)} rows; TX determined acres {tx.determined_acres.sum() / 1e6:.2f}m from {u.rsplit('/', 1)[-1]}")
        time.sleep(1)
    if got:
        new = pd.concat(got, ignore_index=True)
        df = pd.concat([old[~old.year.isin(new.year.unique())], new], ignore_index=True) if len(old) else new
        df.sort_values(["year", "state", "county", "month", "cause"]).to_csv(OUT, index=False)
    if got:
        note("TX determined acres by year (m): " + ", ".join(f"{int(y)} {v / 1e6:.2f}" for y, v in
             df[df.state == "TX"].groupby("year").determined_acres.sum().items()))
    keep = [n for n in NOTES if n.startswith(("cause-of-loss", "TX determined"))] + [n for n in NOTES if "no file" in n or "failed" in n][:5]
    for m in keep[:9]:
        print("::notice::" + m[:900])


if __name__ == "__main__":
    sys.exit(main())
