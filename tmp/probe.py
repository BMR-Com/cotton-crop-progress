import os, json, requests, subprocess
K=os.environ['AMS_API_KEY']; A=(K,''); B='https://marsapi.ams.usda.gov/services/v3.1'; H={'User-Agent':'Mozilla/5.0'}
os.makedirs('pout/acps',exist_ok=True)
for y in range(1998,2026):
    for u in [f'https://apps.ams.usda.gov/Cotton/AnnualCNMarketNewsReports/CottonPriceStatistics/ACPS-{y}-{y+1}.pdf']:
        r=requests.get(u,headers=H,timeout=120)
        if r.status_code==200 and r.content[:4]==b'%PDF':
            fn=f'pout/acps/ACPS-{y}-{y+1}.pdf'; open(fn,'wb').write(r.content)
            subprocess.run(['pdftotext','-layout',fn,fn[:-4]+'.txt']); os.remove(fn); print('got',fn,len(r.content))
        else: print('miss',u,r.status_code)
r=requests.get('https://www.ams.usda.gov/mnreports/cnaacps.pdf',headers=H,timeout=120)
if r.content[:4]==b'%PDF':
    open('pout/acps/cnaacps_latest.pdf','wb').write(r.content); subprocess.run(['pdftotext','-layout','pout/acps/cnaacps_latest.pdf','pout/acps/cnaacps_latest.txt']); os.remove('pout/acps/cnaacps_latest.pdf')
# API: 3804 sections for Aug 2025 -> now, month by month
out={}
import datetime as dt
d=dt.date(2025,8,1)
while d<=dt.date.today():
    e=(d.replace(day=28)+dt.timedelta(days=4)).replace(day=1)-dt.timedelta(days=1)
    for sec in ('Daily Spot Quotations Differences','Basis and Bales','ICE CN Futures Settlement Prices'):
        j=requests.get(f'{B}/reports/3804/{sec}',params={'q':f'report_begin_date={d:%m/%d/%Y}:{e:%m/%d/%Y}'},auth=A,timeout=180).json()
        out.setdefault(sec,[]).extend(j.get('results',[]) if isinstance(j,dict) else [])
    print(d, {k:len(v) for k,v in out.items()}, flush=True)
    d=e+dt.timedelta(days=1)
json.dump(out,open('pout/ams3804.json','w'))
