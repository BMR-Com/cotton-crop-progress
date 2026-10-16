"""One-off: inspect AMS MyMarketNews cotton quality report structures."""
import os, json, requests
os.makedirs("probe", exist_ok=True)
A = (os.environ["AMS_API_KEY"], "")
B = "https://marsapi.ams.usda.gov/services/v3.1/reports/"
out = {}
for rid in ("1654", "1646", "1659", "1658", "1652"):
    try:
        r = requests.get(B + rid, params={"lastReports": "1", "allSections": "true"}, auth=A, timeout=180)
        j = r.json()
        out[rid] = {"status": r.status_code, "type": str(type(j)), "sample": j if isinstance(j, dict) and len(json.dumps(j)) < 30000 else (j[:6] if isinstance(j, list) else str(j)[:3000])}
        if isinstance(j, list):
            out[rid]["sections"] = [x.get("reportSection") for x in j]
            out[rid]["sample"] = [{"reportSection": x.get("reportSection"), "stats": x.get("stats"), "results": (x.get("results") or [])[:4]} for x in j]
    except Exception as e:
        out[rid] = str(e)[:500]
# history depth for the weekly-by-state report
try:
    r = requests.get(B + "1654", params={"q": "report_begin_date=08/01/2015:08/31/2015"}, auth=A, timeout=180)
    j = r.json(); out["1654_2015"] = {"status": r.status_code, "stats": j.get("stats") if isinstance(j, dict) else None, "first": (j.get("results") or [])[:3] if isinstance(j, dict) else str(j)[:800]}
except Exception as e:
    out["1654_2015"] = str(e)[:300]
json.dump(out, open("probe/ams.json", "w"), indent=1, default=str)
print("ok")
