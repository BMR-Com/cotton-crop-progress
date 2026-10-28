#!/usr/bin/env python3
"""Parse the daily base-quality spot quotation table (color 41, leaf 4, staple 34; average of the designated
markets, cents/lb) from AMS Annual Cotton Price Statistics text (pdftotext -layout output).

Usage: python scripts/parse_spot_acps.py <dir with ACPS-YYYY-YYYY.txt / cnaacps-*.txt> <out csv>
Output columns: date, spot41 (cents/lb), source
"""
import os, re, sys, glob
import pandas as pd

MON = ['Aug', 'Sep', 'Oct', 'Nov', 'Dec', 'Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul']
MNUM = dict(Aug=8, Sep=9, Oct=10, Nov=11, Dec=12, Jan=1, Feb=2, Mar=3, Apr=4, May=5, Jun=6, Jul=7)

def my_start(path, text):
    m = re.search(r'(\d{4})-(\d{4})', os.path.basename(path))
    if m and 'ACPS' in os.path.basename(path): return int(m.group(1))
    m = re.search(r'daily and monthly averages,\s*(\d{4})-(\d{4})', text)
    return int(m.group(1)) if m else None

def parse(path):
    text = open(path, errors='ignore').read(); y0 = my_start(path, text)
    if y0 is None: return []
    lines = text.splitlines(); out = []
    # the daily table: heading mentioning color 41 ... daily, then a header row with Date + month names
    for i, l in enumerate(lines):
        if re.search(r'color 41.*daily', l, re.I) or (re.search(r'color 41', l, re.I) and i + 1 < len(lines) and re.search(r'daily', lines[i + 1], re.I)):
            for j in range(i + 1, min(i + 12, len(lines))):
                h = lines[j]
                if re.search(r'\bDate\b', h) and sum(1 for mn in MON if re.search(r'\b' + mn + r'\b', h)) >= 10:
                    cols = [(mn, m.end()) for mn in MON for m in [re.search(r'\b' + mn + r'\b', h)] if m]
                    for k in range(j + 1, min(j + 45, len(lines))):
                        r = lines[k]
                        if re.match(r'\s*(Average|Avg)', r, re.I): break
                        dm = re.match(r'\s*(\d{1,2})\s', r)
                        if not dm: continue
                        day = int(dm.group(1))
                        for v in re.finditer(r'\d{2,3}\.\d{2}', r[dm.end():]):
                            end = dm.end() + v.end()
                            mn = min(cols, key=lambda c: abs(c[1] - end))[0]
                            mo = MNUM[mn]; yr = y0 if mo >= 8 else y0 + 1
                            try:
                                d = pd.Timestamp(yr, mo, day)
                            except ValueError:
                                continue
                            out.append(dict(date=d.date(), spot41=float(v.group()), source=os.path.basename(path)))
                    return out
    return out

if __name__ == '__main__':
    src, dst = sys.argv[1], sys.argv[2]
    rows = []
    for p in sorted(glob.glob(os.path.join(src, '*.txt'))):
        r = parse(p); rows += r; print(os.path.basename(p), len(r))
    d = pd.DataFrame(rows).drop_duplicates('date', keep='last').sort_values('date')
    d.to_csv(dst, index=False); print('saved', dst, len(d), d.date.min(), d.date.max())
