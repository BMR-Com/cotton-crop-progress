import os, re, json, requests
K=os.environ['AMS_API_KEY']; A=(K,''); B='https://marsapi.ams.usda.gov/services/v3.1'
os.makedirs('pout',exist_ok=True); out={}
r=requests.get(B+'/reports',auth=A,timeout=120); L=r.json()
cot=[x for x in L if 'otton' in json.dumps(x)]
out['cotton_reports']=[{k:x.get(k) for k in ('slug_id','slug_name','report_title','published_date','markets','market_types','offices')} for x in cot]
print(len(L),'reports;',len(cot),'cotton')
for x in out['cotton_reports']: print(x['slug_id'],x['slug_name'],'|',x['report_title'],'|',x['published_date'])
# earliest data probe for each cotton report
probe={}
for x in cot:
    s=x['slug_id']; res={}
    for y in (2000,2005,2010,2015,2019,2022,2025):
        try:
            q=requests.get(f'{B}/reports/{s}',params={'q':f'report_begin_date={1}/01/{y}:12/31/{y}'.replace('{1}','01')},auth=A,timeout=120)
            j=q.json(); n=len(j.get('results',[])) if isinstance(j,dict) else 0; res[y]=n
        except Exception as e: res[y]=str(e)[:60]
    probe[s]=res; print('probe',s,res,flush=True)
out['probe']=probe
# sample rows of spot-quote style reports for the latest year
for x in cot:
    if re.search(r'spot|quot|price|basis',x['report_title'] or '',re.I):
        q=requests.get(f"{B}/reports/{x['slug_id']}",params={'q':'report_begin_date=10/01/2025:10/10/2025'},auth=A,timeout=120).json()
        out['sample_'+str(x['slug_id'])]=(q.get('results',[]) if isinstance(q,dict) else q)[:6]
# AMS historical archive directory listing
H={'User-Agent':'Mozilla/5.0'}
for u in ['https://apps.ams.usda.gov/Cotton/','https://apps.ams.usda.gov/Cotton/Historical%20Weekly%20Reports/','https://apps.ams.usda.gov/Cotton/Historical%20Daily%20Reports/']:
    try:
        t=requests.get(u,headers=H,timeout=60).text; out['dir '+u]=re.findall(r'HREF="([^"]+)"',t,re.I)[:200]; print(u,len(out['dir '+u]))
    except Exception as e: print('dir fail',u,e)
json.dump(out,open('pout/probe.json','w'),indent=1,default=str)
