"""One-off probe of USDA endpoints (run in Actions; network is not available locally). Writes probe/ outputs."""
import os, json, re, requests
os.makedirs("probe", exist_ok=True)
K = os.environ.get("USDA_API_KEY", "")
B = "https://quickstats.nass.usda.gov/api/"
out = {}
def pv(param, **q):
    r = requests.get(B + "get_param_values/", params={"key": K, "param": param, **q}, timeout=120)
    try: return r.json().get(param, r.text[:300])
    except Exception: return r.text[:300]
sd = pv("short_desc", commodity_desc="COTTON")
out["cotton_short_desc_n"] = len(sd) if isinstance(sd, list) else sd
out["ginned"] = [s for s in sd if "GINN" in s] if isinstance(sd, list) else []
out["area"] = [s for s in sd if "ACRES" in s and "UPLAND" in s][:40] if isinstance(sd, list) else []
out["yield_prod"] = [s for s in sd if ("YIELD" in s or "PRODUCTION, MEASURED IN 480" in s)][:40] if isinstance(sd, list) else []
for s in out["ginned"][:6]:
    r = requests.get(B + "get_counts/", params={"key": K, "short_desc": s, "year__GE": 2014}, timeout=120)
    out.setdefault("counts", {})[s] = r.text[:100]
    r = requests.get(B + "api_GET/", params={"key": K, "short_desc": s, "year": 2025, "agg_level_desc": "STATE", "format": "JSON"}, timeout=120)
    try:
        d = r.json()["data"]; out.setdefault("sample", {})[s] = [{k: x[k] for k in ("state_alpha", "year", "reference_period_desc", "week_ending", "freq_desc", "Value", "end_code")} for x in d[:8]]
    except Exception as e: out.setdefault("sample", {})[s] = r.text[:200]
# AMS / FSA reachability
def st(u, **kw):
    try:
        r = requests.get(u, timeout=60, **kw); return [r.status_code, r.headers.get("content-type", ""), r.text[:400]]
    except Exception as e: return ["ERR", str(e)[:200]]
out["ams"] = {u: st(u) for u in [
    "https://marsapi.ams.usda.gov/services/v1.2/reports",
    "https://www.ams.usda.gov/mnreports/cnaqcw.txt",
    "https://www.ams.usda.gov/mnreports/cnaacq.txt",
    "https://mymarketnews.ams.usda.gov/filerepo/reports?field_slug_id_value=&name=cotton%20quality",
]}
fsa = "https://www.fsa.usda.gov/tools/informational/freedom-information-act-foia/electronic-reading-room/frequently-requested/crop-acreage-data"
s = st(fsa, headers={"User-Agent": "Mozilla/5.0"})
out["fsa_status"] = s[:2]
try:
    html = requests.get(fsa, timeout=60, headers={"User-Agent": "Mozilla/5.0"}).text
    out["fsa_links"] = sorted(set(re.findall(r'href="([^"]+\.(?:xlsx|zip|xls))"', html)))[:60]
except Exception as e: out["fsa_links"] = str(e)
json.dump(out, open("probe/probe.json", "w"), indent=1)
print(json.dumps(out, indent=1)[:20000])
