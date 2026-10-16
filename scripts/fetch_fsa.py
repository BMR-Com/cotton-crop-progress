#!/usr/bin/env python3
"""
FSA Crop Acreage Data: planted, prevented and failed acres reported to FSA, released monthly Aug–Jan.
Downloads each year's releases from the FSA reading room, keeps the cotton rows of the state-level
summary, and writes data/fsa/fsa_cotton_state.csv (one row per release × state × crop type).
Files already processed are listed in data/fsa/processed.txt and skipped.
"""
import io, os, re, sys, zipfile
import requests
import pandas as pd

PAGE = "https://www.fsa.usda.gov/tools/informational/freedom-information-act-foia/electronic-reading-room/frequently-requested/crop-acreage-data"
ROOT = "https://www.fsa.usda.gov"
OUT = "data/fsa"
H = {"User-Agent": "Mozilla/5.0 (BMR-Com research)"}
FIRST_YEAR = 2014
MONTHS = "jan feb mar apr may jun jul aug sep oct nov dec".split()


def vintage(fname, year):
    """Release month label from the file name, e.g. 2016_fsa_acres_nov2016 -> 2016-11."""
    f = fname.lower()
    for i, m in enumerate(MONTHS, 1):
        if re.search(m, f):
            yr = year + 1 if i <= 6 else year
            m2 = re.search(r"(20\d\d)", f[f.find(m):]) if m in f else None
            return f"{yr if not m2 else m2.group(1)}-{i:02d}"
    m = re.search(r"(\d{2})(\d{2})(20\d\d)", f) or re.search(r"(20\d\d)[_-]?(\d{2})(\d{2})", f)
    if m:
        g = m.groups()
        return f"{g[2]}-{g[0]}" if len(g[2]) == 4 else f"{g[0]}-{g[1]}"
    return "unknown"


def header_row(df):
    for i in range(min(15, len(df))):
        row = " ".join(str(x).lower() for x in df.iloc[i].tolist())
        if "crop" in row and "state" in row:
            return i
    return None


def cotton_rows(xbytes, name):
    out = []
    try:
        sheets = pd.read_excel(io.BytesIO(xbytes), sheet_name=None, header=None)
    except Exception as e:
        print("   cannot read", name, e)
        return out
    for sh, df in sheets.items():
        h = header_row(df)
        if h is None:
            continue
        cols = [str(c).strip() for c in df.iloc[h].tolist()]
        body = df.iloc[h + 1:].copy()
        body.columns = cols
        lc = {c.lower(): c for c in cols}
        if any("county" in c for c in lc):          # keep state summaries only (county detail is huge)
            continue
        crop_col = next((lc[c] for c in lc if c.startswith("crop")), None)
        if not crop_col:
            continue
        sel = body[body[crop_col].astype(str).str.upper().str.contains("COTTON", na=False)]
        for _, r in sel.iterrows():
            out.append({"sheet": sh, **{c: r[c] for c in cols if c and c != "nan"}})
    return out


def main():
    os.makedirs(OUT, exist_ok=True)
    done_path = f"{OUT}/processed.txt"
    done = set(open(done_path).read().split()) if os.path.exists(done_path) else set()
    html = requests.get(PAGE, headers=H, timeout=90).text
    links = sorted(set(re.findall(r'href="([^"]+?\.(?:zip|xlsx))"', html)))
    open(f"{OUT}/links.txt", "w").write("\n".join(links))
    rows = []
    for href in links:
        fname = href.rsplit("/", 1)[-1]
        m = re.match(r"(20\d\d)", fname)
        if not m or int(m.group(1)) < FIRST_YEAR or fname in done:
            continue
        year = int(m.group(1))
        url = href if href.startswith("http") else ROOT + href
        try:
            b = requests.get(url, headers=H, timeout=300).content
        except Exception as e:
            print("skip", fname, e); continue
        files = []
        if fname.endswith(".zip"):
            try:
                z = zipfile.ZipFile(io.BytesIO(b))
                files = [(n, z.read(n)) for n in z.namelist() if n.lower().endswith((".xlsx", ".xls")) and "county" not in n.lower()]
            except zipfile.BadZipFile:
                print("bad zip", fname); continue
        else:
            files = [(fname, b)]
        n0 = len(rows)
        for n, xb in files:
            for r in cotton_rows(xb, n):
                rows.append({"crop_year": year, "release": vintage(fname, year), "file": fname, "member": n, **r})
        print(f"{fname}: {len(files)} sheets-files, {len(rows) - n0} cotton rows")
        done.add(fname)
    if rows:
        new = pd.DataFrame(rows)
        p = f"{OUT}/fsa_cotton_raw.csv"
        old = pd.read_csv(p) if os.path.exists(p) else pd.DataFrame()
        pd.concat([old, new], ignore_index=True).to_csv(p, index=False)
    open(done_path, "w").write("\n".join(sorted(done)))
    print("done; total new rows", len(rows))


if __name__ == "__main__":
    sys.exit(main())
