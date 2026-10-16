import io, re, json, zipfile, requests, pandas as pd, os
os.makedirs("probe", exist_ok=True)
H = {"User-Agent": "Mozilla/5.0"}
PAGE = "https://www.fsa.usda.gov/tools/informational/freedom-information-act-foia/electronic-reading-room/frequently-requested/crop-acreage-data"
html = requests.get(PAGE, headers=H, timeout=90).text
open("probe/fsa_page.html", "w").write(html)
out = {"anchors": re.findall(r'<a[^>]+href="([^"]+\.(?:zip|xlsx))"[^>]*>(.*?)</a>', html, flags=re.S)[:200]}
for href in ["/sites/default/files/documents/2021_fsa_acres_web_as_of_110921.zip", "/sites/default/files/documents/yLDyuZz8gcaNQN8F87Xh.zip"]:
    b = requests.get("https://www.fsa.usda.gov" + href, headers=H, timeout=300).content
    z = zipfile.ZipFile(io.BytesIO(b)); info = {"members": z.namelist()}
    for n in z.namelist():
        if n.lower().endswith((".xlsx", ".xls")):
            xs = pd.read_excel(io.BytesIO(z.read(n)), sheet_name=None, header=None, nrows=12)
            info[n] = {sh: df.astype(str).values.tolist() for sh, df in xs.items()}
    out[href] = info
json.dump(out, open("probe/fsa.json", "w"), indent=1, default=str)
