import json, os, re, requests
os.makedirs("probe", exist_ok=True)
H = {"User-Agent": "Mozilla/5.0"}
out = {}
for u in ["https://www.fsa.usda.gov/documents/iv4i8vwmylge", "https://www.fsa.usda.gov/documents/vdhkphsbjuj0zip"]:
    r = requests.get(u, headers=H, timeout=120, allow_redirects=True)
    out[u] = {"status": r.status_code, "final": r.url, "ctype": r.headers.get("content-type"), "cd": r.headers.get("content-disposition"),
              "len": len(r.content), "head": r.content[:200].decode("latin1"),
              "links": re.findall(r'href="([^"]+)"', r.text)[:80] if "html" in (r.headers.get("content-type") or "") else []}
json.dump(out, open("probe/fsa2.json", "w"), indent=1)
