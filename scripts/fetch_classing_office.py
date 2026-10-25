#!/usr/bin/env python3
"""
USDA AMS "Quality of Cotton Classed by Classing Office" (weekly, CN810 / CNWWQO): upland bales
classed by classing office, week and season to date.

Texas offices: Lubbock, Lamesa, Abilene (West Texas) and Corpus Christi (South Texas).
Other offices: Dumas AR, Rayville LA, Memphis TN, Macon GA, Florence SC, Visalia CA (+ closed
Birmingham AL, Phoenix AZ, Bakersfield CA).

Sources, oldest first:
  1. AMS historical archive  apps.ams.usda.gov/Cotton/Historical Weekly Reports/MY yyyy-yyyy/...
  2. ESMIS (Cornell/NAL) publication releases (recent seasons)
  3. The current week:       www.ams.usda.gov/mnreports/cnwwqo.pdf

Output: data/classing/office.csv  report_date, crop_year, office, week, season, file
Files already parsed are listed in data/classing/office_processed.txt and skipped.
Env: OFFICE_FIRST_YEAR (default 2000), OFFICE_MODE = backfill | latest (default latest)
"""
import os, re, sys, time, subprocess, tempfile
from datetime import date, timedelta
from urllib.parse import quote, urljoin
import requests
import pandas as pd

OUT = "data/classing"
CSV = f"{OUT}/office.csv"
DONE = f"{OUT}/office_processed.txt"
FIRST = int(os.environ.get("OFFICE_FIRST_YEAR", "2000"))
MODE = os.environ.get("OFFICE_MODE", "latest")
ARCH = "https://apps.ams.usda.gov/Cotton/Historical%20Weekly%20Reports/"
FOLDERS = ["mp_cn810_WQO", "mp_cn810 CO Quality - Weekly", "mp_cn810", "mp_cn810 WQO", "mp_cn810_CO Quality - Weekly"]
LATEST = ["https://www.ams.usda.gov/mnreports/cnwwqo.pdf"]
ESMIS = "https://esmis.nal.usda.gov"
OFFICES = ["Abilene", "Corpus Christi", "Dumas", "Florence", "Lamesa", "Lubbock", "Macon", "Memphis", "Rayville",
           "Visalia", "Birmingham", "Phoenix", "Bakersfield", "Hayti", "Greenwood", "Harlingen"]
S = requests.Session()
S.headers["User-Agent"] = "Mozilla/5.0 (BMR-Com research data fetch)"


NOTES = []


def note(msg):
    """Diagnostics surfaced as a workflow annotation (readable without the raw log)."""
    print(msg)
    NOTES.append(msg)


def get(url, **kw):
    for a in range(3):
        try:
            r = S.get(url, timeout=60, **kw)
            return r
        except Exception as e:
            print("  retry", url, e)
            time.sleep(3 * (a + 1))
    return None


def crop_year(d):
    return d.year if d.month >= 7 else d.year - 1


def pdf_text(content):
    """Text of the PDF with its layout kept (pdftotext if installed, else pdfplumber)."""
    with tempfile.NamedTemporaryFile(suffix=".pdf") as f:
        f.write(content)
        f.flush()
        try:
            t = subprocess.run(["pdftotext", "-layout", f.name, "-"], capture_output=True, text=True, timeout=60).stdout
            if t.strip():
                return t
        except Exception:
            pass
        try:
            import pdfplumber
            with pdfplumber.open(f.name) as pdf:
                return "\n".join((pg.extract_text(layout=True) or "") for pg in pdf.pages)
        except Exception as e:
            print("  pdf text failed", e)
            return ""


NUM = r"(Withheld|withheld|-+|\*+|[\d,]+)"
ROW = re.compile(r"^\s*(" + "|".join(OFFICES) + r")\b[\s.:]*" + NUM + r"\s+" + NUM, re.M)


def to_num(x):
    x = x.replace(",", "")
    return float(x) if x.isdigit() else None


def parse(text):
    """First table in the report = upland samples classed by office (week, season)."""
    out, seen = [], set()
    # stop before the American Pima block so Visalia Pima doesn't overwrite Visalia upland
    cut = re.search(r"American\s+Pima", text)
    body = text[: cut.start()] if cut and cut.start() > 200 else text
    for m in ROW.finditer(body):
        off = m.group(1)
        if off in seen:
            continue
        seen.add(off)
        out.append({"office": off, "week": to_num(m.group(2)), "season": to_num(m.group(3))})
    return out


def report_date_from(text, fallback):
    m = re.search(r"(\d{1,2})-(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)-(\d{2,4})", text)
    if m:
        y = int(m.group(3)); y = y + 2000 if y < 100 else y
        mon = "Jan Feb Mar Apr May Jun Jul Aug Sep Oct Nov Dec".split().index(m.group(2)) + 1
        return date(y, mon, int(m.group(1)))
    m = re.search(r"(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{1,2}),\s+(\d{4})", text)
    if m:
        mon = "January February March April May June July August September October November December".split().index(m.group(1)) + 1
        return date(int(m.group(3)), mon, int(m.group(2)))
    return fallback


# ── 1. historical archive ──
def archive_files(cy):
    my = f"MY {cy}-{cy + 1}"
    files = []
    for fo in FOLDERS:
        base = ARCH + quote(my) + "/" + quote(fo) + "/"
        r = get(base)
        if r is not None and r.status_code == 200 and "WQO" in r.text.upper():
            for h in re.findall(r'href="([^"]+\.pdf)"', r.text, re.I):
                if "WQO" in h.upper():
                    files.append(urljoin(base, h))
            if files:
                note(f"{my}: listing {fo} -> {len(files)} files")
                return sorted(set(files))
    # no listing: probe report dates (Thursdays/Fridays/Wednesdays, Aug–Apr)
    for fo in FOLDERS[:2]:
        base = ARCH + quote(my) + "/" + quote(fo) + "/"
        found = []
        d = date(cy, 7, 25)
        while d <= date(cy + 1, 5, 15):
            if d.weekday() in (2, 3, 4):
                for pat in ("CN{}WWQO.pdf", "CN{}wWQO.pdf", "CN{}WWQO.PDF"):
                    u = base + pat.format(d.strftime("%Y%m%d"))
                    r = S.head(u, timeout=30) if True else None
                    if r.status_code == 200:
                        found.append(u)
                        break
            d += timedelta(days=1)
        if found:
            note(f"{my}: probed {fo} -> {len(found)} files")
            return found
    note(f"{my}: nothing in archive")
    return []


# ── 2. ESMIS (combined weekly report by classing office, publication qb98mf474) ──
ESMIS_PUB = ESMIS + "/concern/publications/qb98mf474"


def esmis_files(max_pages=60):
    files = []
    for p in range(max_pages):
        r = get(ESMIS_PUB, params={"page": p})
        if r is None or r.status_code != 200:
            break
        links = re.findall(r'href="([^"]*release-files[^"]+\.pdf)"', r.text, re.I)
        if not links:
            break
        files += [urljoin(ESMIS_PUB, l) for l in links]
    note(f"ESMIS combined report: {len(set(files))} files over {p} pages")
    return sorted(set(files))


def main():
    os.makedirs(OUT, exist_ok=True)
    old = pd.read_csv(CSV) if os.path.exists(CSV) else pd.DataFrame()
    done = set(open(DONE).read().split()) if os.path.exists(DONE) else set()
    urls = []
    if MODE == "backfill":
        for cy in range(FIRST, date.today().year):
            urls += archive_files(cy)
        urls += esmis_files()
    urls += LATEST
    rows, n_new = [], 0
    for u in urls:
        if u in done and u not in LATEST:
            continue
        r = get(u)
        if r is None or r.status_code != 200 or not r.content[:4] == b"%PDF":
            print("  skip", u, None if r is None else r.status_code)
            continue
        text = pdf_text(r.content)
        m = re.search(r"CN(\d{8})", u)
        fb = date(int(m.group(1)[:4]), int(m.group(1)[4:6]), int(m.group(1)[6:])) if m else date.today()
        d = report_date_from(text, fb)
        recs = parse(text)
        if not recs:
            if sum("no rows" in n for n in NOTES) < 3:
                note(f"no rows parsed: {u} | sample: {' / '.join(l.strip() for l in text.splitlines()[:30] if l.strip())[:700]}")
            continue
        for x in recs:
            x.update({"report_date": d.isoformat(), "crop_year": crop_year(d), "file": u.split("/")[-1]})
        rows += recs
        done.add(u)
        n_new += 1
        if n_new % 25 == 0:
            print(f"  parsed {n_new} reports", flush=True)
        time.sleep(0.3)
    new = pd.DataFrame(rows)
    df = pd.concat([old, new], ignore_index=True) if len(old) else new
    if len(df):
        df = df.drop_duplicates(["report_date", "office"], keep="last").sort_values(["report_date", "office"])
        df.to_csv(CSV, index=False, columns=["report_date", "crop_year", "office", "week", "season", "file"])
    with open(DONE, "w") as f:
        f.write("\n".join(sorted(x for x in done if x not in LATEST)))
    note(f"office classing: {n_new} reports parsed, {len(df)} rows; reports by crop year "
         f"{df.groupby('crop_year').report_date.nunique().to_dict() if len(df) else {}}")
    for n in [x for x in NOTES if not x.endswith("nothing in archive")][-9:]:
        print("::notice::" + n.replace("\n", " ")[:900])


if __name__ == "__main__":
    sys.exit(main())
