#!/usr/bin/env python3
"""
USDA FSA weekly upland cotton Adjusted World Price announcement (Thursdays, effective Fri–Thu).
Downloads the latest PDFs (sites/default/files/YYYY-MM/<mon><dd>upl.pdf) and appends to data/awp/awp.csv:
announce_date, effective_start, effective_end, fe_price, cost_to_market, slm_adj, awp, ldp, fca, cca.
"""
import io, os, re, sys
from datetime import date, timedelta
import requests
import pandas as pd

OUT = "data/awp/awp.csv"
H = {"User-Agent": "Mozilla/5.0 (BMR-Com research)"}


def candidates(d):
    mon = d.strftime("%b").lower()
    base = f"https://www.fsa.usda.gov/sites/default/files/{d:%Y-%m}/{mon}{d.day:02d}upl"
    return [base + ".pdf", base + "_0.pdf", base + "_1.pdf"]


def num_after(text, label, n=1):
    m = re.search(label + r"[^0-9\-]*(-?\d+\.\d+)", text, flags=re.I | re.S)
    return float(m.group(1)) if m else None


def parse(pdf_bytes):
    import pdfplumber
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        t = "\n".join(p.extract_text() or "" for p in pdf.pages)
    out = {"fe_price": num_after(t, r"FE\)?\s*Price"), "cost_to_market": num_after(t, r"cost to market"),
           "slm_adj": num_after(t, r"SLM 1-1/16"), "awp": num_after(t, r"Adjusted World Price"),
           "ldp": num_after(t, r"Loan Deficiency Payment"), "fca": num_after(t, r"Fine Count Adjustment"),
           "cca": num_after(t, r"Coarse Count Adjustment")}
    m = re.search(r"Friday,\s*([A-Za-z]+ \d{1,2},\s*\d{4}).*?Thursday,\s*([A-Za-z]+ \d{1,2},\s*\d{4})", t, re.S)
    if m:
        out["effective_start"] = pd.to_datetime(m.group(1)).date().isoformat()
        out["effective_end"] = pd.to_datetime(m.group(2)).date().isoformat()
    # the AWP is FE price less the two adjustments; recompute if the label search missed
    if out["awp"] is None and out["fe_price"] and out["cost_to_market"] is not None and out["slm_adj"] is not None:
        out["awp"] = round(out["fe_price"] + out["cost_to_market"] + out["slm_adj"], 2)
    return out, t


def main():
    os.makedirs(os.path.dirname(OUT), exist_ok=True)
    old = pd.read_csv(OUT) if os.path.exists(OUT) else pd.DataFrame(columns=["announce_date"])
    have = set(old.announce_date.astype(str))
    today = date.today()
    rows = []
    for k in range(0, 63):                       # Thursdays over the last ~9 weeks
        d = today - timedelta(days=k)
        if d.weekday() != 3 or d.isoformat() in have:
            continue
        for u in candidates(d):
            r = requests.get(u, headers=H, timeout=60)
            if r.status_code == 200 and r.content[:4] == b"%PDF":
                rec, text = parse(r.content)
                rec["announce_date"] = d.isoformat(); rec["url"] = u
                rows.append(rec)
                if k < 7:
                    open("data/awp/latest.txt", "w").write(text)
                print(d, rec)
                break
    if rows:
        df = pd.concat([old, pd.DataFrame(rows)], ignore_index=True).drop_duplicates("announce_date", keep="last").sort_values("announce_date")
        df.to_csv(OUT, index=False)
    print("rows added:", len(rows))


if __name__ == "__main__":
    sys.exit(main())
