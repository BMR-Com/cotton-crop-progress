"""Shared loaders for the build: NASS crop progress/condition, drought, annual NASS, ginnings, classing, FSA."""
import glob, io, os
import numpy as np
import pandas as pd
import requests

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
D = os.path.join(ROOT, "data")
DROUGHT_RAW = "https://raw.githubusercontent.com/BMR-Com/drought-monitor/main/data/state/{s}.csv"
DROUGHT_LOCAL = os.environ.get("DROUGHT_LOCAL", "")          # optional local clone for development

STATE_NAME = {"AL": "Alabama", "AZ": "Arizona", "AR": "Arkansas", "CA": "California", "FL": "Florida", "GA": "Georgia",
              "KS": "Kansas", "LA": "Louisiana", "MS": "Mississippi", "MO": "Missouri", "NM": "New Mexico",
              "NC": "North Carolina", "OK": "Oklahoma", "SC": "South Carolina", "TN": "Tennessee", "TX": "Texas",
              "VA": "Virginia", "US": "United States"}
NAME_STATE = {v.upper(): k for k, v in STATE_NAME.items()}
COND_W = {"VERY POOR": 1, "POOR": 2, "FAIR": 3, "GOOD": 4, "EXCELLENT": 5}
STAGES = ["PLANTED", "SQUARING", "SETTING BOLLS", "BOLLS OPENING", "HARVESTED"]


def nass_weekly():
    """Weekly condition (VP..EX) and progress (stages) by state, current-year values only."""
    fr = []
    for f in sorted(glob.glob(os.path.join(D, "cotton_*.csv"))):
        fr.append(pd.read_csv(f, usecols=["year", "state_alpha", "short_desc", "week_ending", "Value"], low_memory=False))
    d = pd.concat(fr, ignore_index=True)
    d = d[~d.short_desc.str.contains("5 YEAR|PREVIOUS YEAR", regex=True)]
    d["v"] = pd.to_numeric(d.Value.astype(str).str.replace(",", ""), errors="coerce")
    d["week_ending"] = pd.to_datetime(d.week_ending)
    d = d.dropna(subset=["v", "week_ending"])
    cond = d[d.short_desc.str.contains("CONDITION")].copy()
    cond["cat"] = cond.short_desc.str.extract(r"PCT (.*)$")[0]
    c = cond.pivot_table(index=["year", "state_alpha", "week_ending"], columns="cat", values="v", aggfunc="last").reset_index()
    c = c.dropna(subset=list(COND_W))
    c["ci"] = sum(c[k] * w for k, w in COND_W.items()) / 100.0           # 1 (all very poor) … 5 (all excellent)
    c["ge"] = c["GOOD"] + c["EXCELLENT"]
    c["pvp"] = c["POOR"] + c["VERY POOR"]
    prog = d[d.short_desc.str.contains("PROGRESS")].copy()
    prog["stage"] = prog.short_desc.str.extract(r"PCT (.*)$")[0]
    p = prog.pivot_table(index=["year", "state_alpha", "week_ending"], columns="stage", values="v", aggfunc="last").reset_index()
    for x in (c, p):
        x.rename(columns={"state_alpha": "state"}, inplace=True)
        x["woy"] = x.week_ending.dt.isocalendar().week.astype(int)
    return c, p


def drought(states):
    out = []
    for s in states:
        if s == "US":
            continue
        try:
            if DROUGHT_LOCAL:
                t = pd.read_csv(os.path.join(DROUGHT_LOCAL, "data/state", f"{s}.csv"))
            else:
                t = pd.read_csv(io.StringIO(requests.get(DROUGHT_RAW.format(s=s), timeout=60).text))
        except Exception as e:
            print("drought missing", s, e)
            continue
        t["state"] = s
        out.append(t)
    d = pd.concat(out, ignore_index=True)
    d["week"] = pd.to_datetime(d.Week)
    d["year"] = d.week.dt.year
    d["woy"] = d.week.dt.isocalendar().week.astype(int)
    return d.rename(columns={"D2-D4": "d2", "D1-D4": "d1", "D3-D4": "d3"})[["state", "week", "year", "woy", "d1", "d2", "d3"]]


def annual():
    """Annual NASS state data. final(): the 'YEAR' value; latest(): the latest in-season estimate."""
    p = os.path.join(D, "ginning", "annual.csv")
    if not os.path.exists(p):
        return pd.DataFrame(columns=["year", "state", "item", "period", "value"])
    a = pd.read_csv(p)
    if "period" not in a.columns:
        a["period"] = "YEAR"
    return a


ORDER = ["YEAR - MAR ACREAGE", "YEAR - JUN ACREAGE", "YEAR - AUG FORECAST", "YEAR - SEP FORECAST", "YEAR - OCT FORECAST",
         "YEAR - NOV FORECAST", "YEAR - DEC FORECAST", "YEAR"]


def pick(a, item, year=None, final_only=False):
    """One value per (year, state): the final if present, else the latest in-season estimate."""
    x = a[a.item == item].copy()
    if year is not None:
        x = x[x.year == year]
    if final_only:
        x = x[x.period == "YEAR"]
    x["rank"] = x.period.map({p: i for i, p in enumerate(ORDER)}).fillna(-1)
    x = x.sort_values("rank").groupby(["year", "state"]).tail(1)
    return x.set_index(["year", "state"]).value


def classing_summary():
    p = os.path.join(D, "classing", "summary.csv")
    if not os.path.exists(p):
        return pd.DataFrame()
    s = pd.read_csv(p)
    s["week_end"] = pd.to_datetime(s.week_end)
    s["st"] = s.state.str.upper().map(NAME_STATE)
    return s


def classing_detail(kind, years):
    fr = []
    for y in years:
        p = os.path.join(D, "classing", f"{kind}_{y}.csv")
        if os.path.exists(p):
            fr.append(pd.read_csv(p))
    if not fr:
        return pd.DataFrame()
    d = pd.concat(fr, ignore_index=True)
    d["week_end"] = pd.to_datetime(d.week_end)
    d["st"] = d.state.str.upper().map(NAME_STATE)
    return d
