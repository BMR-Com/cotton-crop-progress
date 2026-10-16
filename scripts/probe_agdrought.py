import os, re, requests
os.makedirs("probe", exist_ok=True)
H = {"User-Agent": "Mozilla/5.0"}
s = requests.Session()
r = s.get("https://agindrought.unl.edu/Table.aspx?1", headers=H, timeout=60)
open("probe/table.html", "w").write(r.text)
for src in set(re.findall(r'<script[^>]+src="([^"]+)"', r.text)):
    u = src if src.startswith("http") else "https://agindrought.unl.edu/" + src.lstrip("/")
    if "agindrought" in u:
        try:
            open("probe/js_" + re.sub(r"[^A-Za-z0-9]", "_", src)[-60:] + ".js", "w").write(s.get(u, headers=H, timeout=60).text)
        except Exception as e:
            print(u, e)
print("done", len(r.text))
