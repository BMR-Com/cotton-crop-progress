import os, re, json, requests
K=os.environ['AMS_API_KEY']; A=(K,''); B='https://marsapi.ams.usda.gov/services/v3.1'
os.makedirs('pout',exist_ok=True); out={}
def get(path,q=None):
    try:
        r=requests.get(B+path,params={'q':q} if q else None,auth=A,timeout=120); return r.json()
    except Exception as e: return {'err':str(e)}
for s in (3804,3895):
    out[f'{s}_sections']=get(f'/reports/{s}/sections') if True else None
    cov={}
    for y in range(2000,2027):
        j=get(f'/reports/{s}',f'report_begin_date=01/01/{y}:01/31/{y}')
        cov[y]=len(j.get('results',[])) if isinstance(j,dict) else ('list',len(j))
    out[f'{s}_coverage_jan']=cov; print(s,cov,flush=True)
# detail rows for one day / one month
for s,q in ((3804,'report_begin_date=10/09/2025:10/10/2025'),(3895,'report_begin_date=10/01/2025:10/31/2025')):
    for sec in ('Report Detail','Details','Report Details'):
        j=get(f'/reports/{s}/{sec}',q)
        if isinstance(j,dict) and j.get('results'):
            out[f'{s}_{sec}']=j['results'][:40]; print(s,sec,len(j['results'])); break
    else:
        out[f'{s}_detail_raw']=str(get(f'/reports/{s}',q+'&allSections=true'))[:4000]
H={'User-Agent':'Mozilla/5.0'}
for u in ['https://apps.ams.usda.gov/Cotton/AnnualCNMarketNewsReports/','https://apps.ams.usda.gov/Cotton/Historical%20Weekly%20Reports/MY%202005-2006/','https://apps.ams.usda.gov/Cotton/Historical%20Weekly%20Reports/MY%202015-2016/']:
    try:
        t=requests.get(u,headers=H,timeout=60).text; out['dir '+u]=re.findall(r'HREF="([^"]+)"',t,re.I)[:300]; print(u,len(out['dir '+u]))
    except Exception as e: print('dir fail',u,e)
json.dump(out,open('pout/probe2.json','w'),indent=1,default=str)
