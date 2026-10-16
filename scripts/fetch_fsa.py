#!/usr/bin/env python3
"""
FSA Crop Acreage Data (acres reported to FSA; releases monthly from August through January).
For every release from 2014 onward, sums the county_data sheet's cotton rows by state, cotton type
(UPLAND / ELS) and irrigation practice: planted, failed, prevented, volunteer and not-planted acres.

Output: data/fsa/fsa_cotton_state.csv
  crop_year, release (date of the data), state, type, irr, planted, failed, prevented, volunteer, not_planted
Each file is appended as soon as it is processed (data/fsa/processed.txt lists files already done).
"""
import io, os, re, sys, zipfile
import requests
import pandas as pd

PAGE = "https://www.fsa.usda.gov/tools/informational/freedom-information-act-foia/electronic-reading-room/frequently-requested/crop-acreage-data"
ROOT = "https://www.fsa.usda.gov"
OUT = "data/fsa"
H = {"User-Agent": "Mozilla/5.0 (BMR-Com research)"}
FIRST_YEAR = 2014
COLS = {"planted acres": "planted", "failed acres": "failed", "prevented acres": "prevented",
        "volunteer acres": "volunteer", "not planted acres": "not_planted"}


def read_sheet(xb, name):
    for eng in ("calamine", None):
        try:
            return pd.read_excel(io.BytesIO(xb), sheet_name=name, engine=eng) if eng else pd.read_excel(io.BytesIO(xb), sheet_name=name)
        except Exception as e:
            err = e
    raise err


def release_date(xb, fname):
    try:
        d = pd.read_excel(io.BytesIO(xb), sheet_name=0, header=None, nrows=4, engine="calamine")
        for v in d.values.ravel():
            t = pd.to_datetime(v, errors="coerce")
            if pd.notna(t) and 2000 < t.year < 2100:
                return t.date().isoformat()
    except Exception:
        pass
    m = re.search(r"(\d{2})(\d{2})(\d{2})(?!\d)", fname)          # e.g. ..._110921 -> 2021-11-09
    if m:
        return f"20{m.group(3)}-{m.group(1)}-{m.group(2)}"
    return None


def cotton_by_state(xb):
    try:
        sheets = pd.ExcelFile(io.BytesIO(xb), engine="calamine").sheet_names
    except Exception:
        sheets = pd.ExcelFile(io.BytesIO(xb)).sheet_names
    name = next((s for s in sheets if "county_data" in s.lower()), None)
    if not name:
        return None
    d = read_sheet(xb, name)
    d.columns = [str(c).strip() for c in d.columns]
    lc = {c.lower(): c for c in d.columns}
    crop = lc.get("crop")
    if not crop:
        return None
    d = d[d[crop].astype(str).str.upper().str.contains("COTTON", na=False)].copy()
    d["type"] = d[crop].astype(str).str.upper().str.replace(r"COTTON\s+", "", regex=True).str.strip()
    d["irr"] = d[lc["irrigation practice"]].astype(str).str.upper().str.strip() if "irrigation practice" in lc else "ALL"
    keep = {lc[k]: v for k, v in COLS.items() if k in lc}
    for c in keep:
        d[c] = pd.to_numeric(d[c], errors="coerce").fillna(0)
    g = d.groupby([lc["state"], "type", "irr"])[list(keep)].sum().reset_index().rename(columns={lc["state"]: "state", **keep})
    return g


def main():
    os.makedirs(OUT, exist_ok=True)
    done_path, out_path = f"{OUT}/processed.txt", f"{OUT}/fsa_cotton_state.csv"
    done = set(open(done_path).read().split()) if os.path.exists(done_path) else set()
    html = requests.get(PAGE, headers=H, timeout=90).text
    links = re.findall(r'<a[^>]+href="([^"]+?\.(?:zip|xlsx)|/documents/[A-Za-z0-9_-]+)"[^>]*>(.*?)</a>', html, flags=re.S)
    links = [(h, t) for h, t in links if "acreage data" in re.sub("<[^>]+>", "", t).lower() or h.endswith((".zip", ".xlsx"))]
    open(f"{OUT}/links.txt", "w").write("\n".join(f"{h}\t{re.sub('<[^>]+>', '', t).strip()}" for h, t in links))
    for href, text in links:
        fname = href.rsplit("/", 1)[-1]
        label = re.sub("<[^>]+>", "", text)
        m = re.match(r"\s*(20\d\d)", label) or re.match(r"(20\d\d)", fname)
        if not m or int(m.group(1)) < FIRST_YEAR or fname in done:
            continue
        year = int(m.group(1))
        url = href if href.startswith("http") else ROOT + href
        try:
            resp = requests.get(url, headers=H, timeout=300)
            b = resp.content
            if "html" in (resp.headers.get("content-type") or ""):     # /documents/<id> is a landing page → follow its file link
                m2 = re.search(r'href="([^"]+?\.(?:zip|xlsx))"', resp.text)
                if not m2:
                    print("no file link on", url); continue
                fu = m2.group(1)
                b = requests.get(fu if fu.startswith("http") else ROOT + fu, headers=H, timeout=300).content
            files = []
            try:
                z = zipfile.ZipFile(io.BytesIO(b))
                names = z.namelist()
                if any(n.startswith("xl/") for n in names):        # the download is itself an .xlsx
                    files = [(fname + ".xlsx", b)]
                else:
                    files = [(n, z.read(n)) for n in names if n.lower().endswith((".xlsx", ".xls"))]
            except zipfile.BadZipFile:
                files = [(fname, b)]
            n_rows = 0
            for n, xb in files:
                g = cotton_by_state(xb)
                if g is None or not len(g):
                    continue
                lab = re.search(r"as of\s+([A-Za-z]+\.?\s+\d{1,2},?\s+20\d\d)", label)
                rd = pd.to_datetime(lab.group(1).replace(".", ""), errors="coerce") if lab else pd.NaT
                g.insert(0, "release", rd.date().isoformat() if pd.notna(rd) else (release_date(xb, n) or release_date(xb, fname)))
                g.insert(0, "crop_year", year)
                g["file"] = fname
                g.to_csv(out_path, mode="a", header=not os.path.exists(out_path), index=False)
                n_rows += len(g)
            print(f"{label.strip()[:60]} ({fname}): {n_rows} rows", flush=True)
            done.add(fname)
            open(done_path, "w").write("\n".join(sorted(done)))
        except Exception as e:
            print("skip", fname, e, flush=True)
    if os.path.exists(out_path):
        d = pd.read_csv(out_path).drop_duplicates(["crop_year", "release", "state", "type", "irr"], keep="last")
        d.sort_values(["crop_year", "release", "state", "type", "irr"]).to_csv(out_path, index=False)


if __name__ == "__main__":
    sys.exit(main())
