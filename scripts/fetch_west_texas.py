#!/usr/bin/env python3
"""
West Texas (High Plains + Rolling Plains) inputs for the abandonment model. Compact outputs in data/westtx/:

  nass_district.csv   year, district, planted, harvested (upland cotton acres; NASS QuickStats, needs USDA_API_KEY)
  nass_county.csv     year, county_fips, county, district, planted, harvested
  climdiv.csv         year, month, div (1 High Plains, 2 Low Rolling Plains), element, value   (NOAA nClimDiv)
  usdm_westtx.csv     week, d0..d4 (% of West Texas cotton area, county values weighted by cotton planted acres)
  ndvi.csv            point, date, ndvi_mean, ndvi_p25, ndvi_low_share   (MODIS MOD13Q1 250 m, Jun–Sep)
Env: WTX_FIRST (default 2001), WTX_PARTS (comma list: nass,climdiv,usdm,ndvi; default all).
Each part keeps completed years and refreshes the latest two.
"""
import io, json, os, re, sys, time
from datetime import date, timedelta
import requests
import pandas as pd
import numpy as np

OUT = "data/westtx"
FIRST = int(os.environ.get("WTX_FIRST", "2001"))
PARTS = os.environ.get("WTX_PARTS", "nass,climdiv,usdm,ndvi").split(",")
THIS = date.today().year
S = requests.Session()
S.headers["User-Agent"] = "Mozilla/5.0 (BMR-Com research data fetch)"
NOTES = []
WEST_DISTRICTS = ("NORTHERN HIGH PLAINS", "SOUTHERN HIGH PLAINS", "NORTHERN LOW PLAINS", "SOUTHERN LOW PLAINS")
# main West Texas cotton counties (name, lat, lon) for the vegetation sample
NDVI_POINTS = [("Lubbock", 33.61, -101.82), ("Hale", 34.07, -101.83), ("Lynn", 33.18, -101.82), ("Dawson", 32.74, -101.95),
               ("Gaines", 32.74, -102.63), ("Terry", 33.17, -102.33), ("Hockley", 33.61, -102.34), ("Lamb", 34.07, -102.35),
               ("Floyd", 34.07, -101.30), ("Crosby", 33.61, -101.30), ("Martin", 32.31, -101.95), ("Haskell", 33.18, -99.73),
               ("Knox", 33.61, -99.75), ("Fisher", 32.74, -100.40), ("Jones", 32.74, -99.88), ("Wilbarger", 34.08, -99.24)]


def note(m):
    print(m, flush=True)
    NOTES.append(m)


def get(url, **kw):
    for a in range(4):
        try:
            r = S.get(url, timeout=kw.pop("timeout", 120), **kw)
            if r.status_code == 200:
                return r
            print("  http", r.status_code, url[:120])
            if r.status_code in (400, 404):
                return r
        except Exception as e:
            print("  retry", url[:120], e)
        time.sleep(4 * (a + 1))
    return None


def keep_old(path, years_new, key="year"):
    if not os.path.exists(path):
        return pd.DataFrame()
    o = pd.read_csv(path)
    return o[~o[key].isin(years_new)] if key in o else o


def years_todo(path, key="year"):
    have = set(pd.read_csv(path)[key].unique()) if os.path.exists(path) else set()
    return [y for y in range(FIRST, THIS + 1) if y not in have or y >= THIS - 1]


# ── NASS district and county acres (target + weights) ──
def nass():
    key = os.environ.get("USDA_API_KEY")
    if not key:
        note("nass: no USDA_API_KEY")
        return
    B = "https://quickstats.nass.usda.gov/api/api_GET/"
    out_d, out_c = [], []
    for level, sink in (("AGRICULTURAL DISTRICT", out_d), ("COUNTY", out_c)):
        for item, col in (("COTTON, UPLAND - ACRES PLANTED", "planted"), ("COTTON, UPLAND - ACRES HARVESTED", "harvested")):
            for y0 in range(FIRST, THIS + 1, 5):
                q = {"key": key, "format": "JSON", "short_desc": item, "agg_level_desc": level, "state_alpha": "TX",
                     "year__GE": y0, "year__LE": min(y0 + 4, THIS), "source_desc": "SURVEY", "reference_period_desc": "YEAR"}
                r = get(B, params=q)
                rows = (r.json().get("data", []) if r is not None and r.status_code == 200 else [])
                for x in rows:
                    try:
                        v = float(str(x["Value"]).replace(",", ""))
                    except ValueError:
                        continue
                    sink.append({"year": int(x["year"]), "district": x.get("asd_desc", ""), "county": x.get("county_name", ""),
                                 "county_fips": (x.get("state_fips_code", "") + x.get("county_code", "")) if level == "COUNTY" else "",
                                 "item": col, "value": v})
                time.sleep(0.5)
    for rows, path, idx in ((out_d, "nass_district.csv", ["year", "district"]), (out_c, "nass_county.csv", ["year", "county_fips", "county", "district"])):
        if not rows:
            note(f"nass: no rows for {path}")
            continue
        d = pd.DataFrame(rows).pivot_table(index=idx, columns="item", values="value", aggfunc="max").reset_index()
        d.to_csv(os.path.join(OUT, path), index=False)
        w = d[d.district.str.upper().isin(WEST_DISTRICTS)] if "district" in d else d
        note(f"nass {path}: {len(d)} rows, years {int(d.year.min())}-{int(d.year.max())}; West TX planted latest "
             f"{w[w.year == w.year.max()].planted.sum() / 1e6:.2f}m" if len(w) else f"nass {path}: {len(d)} rows")


# ── NOAA nClimDiv: Texas divisions 1 and 2 ──
def climdiv():
    base = "https://www.ncei.noaa.gov/pub/data/cirs/climdiv/"
    r = get(base)
    if r is None or r.status_code != 200:
        note(f"climdiv: listing failed {None if r is None else r.status_code}")
        return
    rows = []
    for el in ("pcpn", "tmax", "tmpc", "pdsi", "zndx", "phdi"):
        names = sorted(set(re.findall(rf'(climdiv-{el}dv-v[\d.]+-\d+)', r.text)))
        if not names:
            note(f"climdiv: no file for {el}")
            continue
        t = get(base + names[-1], timeout=300)
        if t is None or t.status_code != 200:
            continue
        for ln in t.text.splitlines():
            if len(ln) < 10 or not ln[:2] == "41":         # 41 = Texas in nClimDiv state codes
                continue
            div, yr = int(ln[2:4]), int(ln[6:10])
            if div not in (1, 2) or yr < FIRST:
                continue
            vals = ln[10:].split()
            for m, v in enumerate(vals[:12], 1):
                try:
                    f = float(v)
                except ValueError:
                    continue
                if f <= -99:
                    continue
                rows.append({"year": yr, "month": m, "div": div, "element": el, "value": f})
    if rows:
        pd.DataFrame(rows).to_csv(os.path.join(OUT, "climdiv.csv"), index=False)
        note(f"climdiv: {len(rows)} values, elements {sorted(set(x['element'] for x in rows))}")


# ── US Drought Monitor, Texas counties → West Texas cotton-weighted ──
def usdm():
    path = os.path.join(OUT, "usdm_westtx.csv")
    wpath = os.path.join(OUT, "nass_county.csv")
    weights = None
    if os.path.exists(wpath):
        c = pd.read_csv(wpath, dtype={"county_fips": str})
        c = c[c.district.str.upper().isin(WEST_DISTRICTS)]
        weights = c.groupby("county_fips").planted.mean()
    have = pd.read_csv(path) if os.path.exists(path) else pd.DataFrame()
    have_years = set(pd.to_datetime(have.week).dt.year) if len(have) else set()
    out = []
    for y in range(FIRST, THIS + 1):
        if y in have_years and y < THIS - 1:
            continue
        u = ("https://usdmdataservices.unl.edu/api/CountyStatistics/GetDroughtSeverityStatisticsByAreaPercent"
             f"?aoi=TX&startdate=1/1/{y}&enddate=12/31/{y}&statisticsType=1")
        r = get(u, headers={"Accept": "application/json"}, timeout=300)
        if r is None or r.status_code != 200:
            note(f"usdm {y}: failed")
            continue
        try:
            d = pd.DataFrame(r.json())
        except Exception:
            d = pd.read_csv(io.StringIO(r.text))
        cols = {c.lower(): c for c in d.columns}
        fips = cols.get("fips") or cols.get("countyfips") or next((c for c in d.columns if "fips" in c.lower()), None)
        wk = cols.get("validstart") or cols.get("mapdate") or next((c for c in d.columns if "date" in c.lower() or "valid" in c.lower()), None)
        if fips is None or wk is None:
            note(f"usdm {y}: unexpected columns {list(d.columns)[:12]}")
            continue
        d["fips"] = d[fips].astype(str).str.zfill(5)
        if weights is not None and len(weights):
            d = d[d.fips.isin(weights.index)]
            d["w"] = d.fips.map(weights).fillna(0)
        else:
            d["w"] = 1.0
        d["week"] = pd.to_datetime(d[wk]).dt.strftime("%Y-%m-%d")
        lvl = {k: cols.get(k) for k in ("d0", "d1", "d2", "d3", "d4")}
        for k, c in lvl.items():
            d[k] = pd.to_numeric(d[c], errors="coerce") if c else np.nan
        g = d.groupby("week").apply(lambda x: pd.Series({k: np.average(x[k].fillna(0), weights=x.w) if x.w.sum() > 0 else x[k].mean()
                                                          for k in lvl}))
        out.append(g.reset_index())
        time.sleep(1)
    if out:
        new = pd.concat(out)
        yrs = set(pd.to_datetime(new.week).dt.year)
        old = have[~pd.to_datetime(have.week).dt.year.isin(yrs)] if len(have) else have
        pd.concat([old, new]).sort_values("week").to_csv(path, index=False)
        note(f"usdm: {len(new)} new weeks ({min(yrs)}-{max(yrs)}), weighted by {'cotton acres' if weights is not None else 'equal'}")


# ── MODIS NDVI over West Texas cotton counties, June–September ──
def ndvi():
    path = os.path.join(OUT, "ndvi.csv")
    have = pd.read_csv(path) if os.path.exists(path) else pd.DataFrame()
    have_years = set(pd.to_datetime(have.date).dt.year) if len(have) else set()
    B = "https://modis.ornl.gov/rst/api/v1/MOD13Q1/subset"
    out = []
    for y in range(max(FIRST, 2001), THIS + 1):
        if y in have_years and y < THIS:
            continue
        for name, lat, lon in NDVI_POINTS:
            # Jun 1 – Sep 30 holds 8 composites; the service returns at most 10 dates per call
            q = {"latitude": lat, "longitude": lon, "band": "250m_16_days_NDVI", "startDate": f"A{y}145", "endDate": f"A{y}280",
                 "kmAboveBelow": 5, "kmLeftRight": 5}
            r = get(B, params=q, headers={"Accept": "application/json"}, timeout=180)
            if r is None or r.status_code != 200:
                note(f"ndvi {y} {name}: failed {None if r is None else r.status_code}")
                continue
            try:
                j = r.json()
            except Exception:
                continue
            for s in j.get("subset", []):
                if s.get("band") != "250m_16_days_NDVI":
                    continue
                v = np.array([x for x in s.get("data", []) if x is not None and x > -2000], float) / 10000.0
                v = v[(v > -0.2) & (v <= 1)]
                if not len(v):
                    continue
                d = date(int(s["modis_date"][1:5]), 1, 1) + timedelta(days=int(s["modis_date"][5:]) - 1)
                out.append({"point": name, "date": d.isoformat(), "ndvi_mean": round(float(v.mean()), 4),
                            "ndvi_p25": round(float(np.percentile(v, 25)), 4), "ndvi_low_share": round(float((v < 0.25).mean()), 4)})
            time.sleep(0.3)
        note(f"ndvi {y}: {sum(1 for x in out if x['date'].startswith(str(y)))} point-dates")
    if out:
        new = pd.DataFrame(out)
        yrs = set(pd.to_datetime(new.date).dt.year)
        old = have[~pd.to_datetime(have.date).dt.year.isin(yrs)] if len(have) else have
        pd.concat([old, new]).sort_values(["date", "point"]).to_csv(path, index=False)


def main():
    os.makedirs(OUT, exist_ok=True)
    for p, f in (("nass", nass), ("climdiv", climdiv), ("usdm", usdm), ("ndvi", ndvi)):
        if p in PARTS:
            try:
                f()
            except Exception as e:
                import traceback; traceback.print_exc()
                note(f"{p}: failed {e}")
    keep = [n for n in NOTES if not n.startswith("ndvi 2")] + [n for n in NOTES if n.startswith("ndvi 2")][-2:]
    for m in keep[:9]:
        print("::notice::" + m[:900])


if __name__ == "__main__":
    sys.exit(main())
