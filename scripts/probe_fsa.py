import io, json, os, re, zipfile, requests, pandas as pd
os.makedirs("probe", exist_ok=True)
H = {"User-Agent": "Mozilla/5.0"}
out = {}
for u in ["https://www.fsa.usda.gov/documents/iv4i8vwmylge", "https://www.fsa.usda.gov/documents/kue2z66bqlhk"]:
    r = requests.get(u, headers=H, timeout=120)
    m = re.search(r'href="([^"]+?\.(?:zip|xlsx))"', r.text)
    fu = m.group(1) if m else None
    info = {"file": fu}
    if fu:
        b = requests.get(fu if fu.startswith("http") else "https://www.fsa.usda.gov" + fu, headers=H, timeout=300).content
        info["len"] = len(b)
        try:
            z = zipfile.ZipFile(io.BytesIO(b)); names = z.namelist(); info["members"] = names[:20]
            xs = [(n, z.read(n)) for n in names if n.lower().endswith((".xlsx", ".xls"))] if not any(n.startswith("xl/") for n in names) else [("self.xlsx", b)]
        except Exception as e:
            xs = [("raw", b)]; info["err"] = str(e)
        for n, xb in xs[:2]:
            xl = pd.ExcelFile(io.BytesIO(xb), engine="calamine")
            info[n] = {}
            for sh in xl.sheet_names:
                df = pd.read_excel(xl, sheet_name=sh, header=None, nrows=8)
                info[n][sh] = df.astype(str).values.tolist()
    out[u] = info
json.dump(out, open("probe/fsa3.json", "w"), indent=1, default=str)
