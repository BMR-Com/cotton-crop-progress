import os, re, json, requests, subprocess, datetime as dt
H={'User-Agent':'Mozilla/5.0'}; os.makedirs('pout/acps',exist_ok=True); log=[]
def save_pdf(content, name):
    fn=f'pout/acps/{name}.pdf'; open(fn,'wb').write(content); subprocess.run(['pdftotext','-layout',fn,fn[:-4]+'.txt'])
    t=open(fn[:-4]+'.txt',errors='ignore').read()
    if len(t)<5000:   # scanned: OCR
        subprocess.run(['pdftoppm','-r','300','-png',fn,f'pout/acps/{name}_p'])
        txt=''
        for p in sorted(x for x in os.listdir('pout/acps') if x.startswith(name+'_p') and x.endswith('.png')):
            r=subprocess.run(['tesseract',f'pout/acps/{p}','-','--psm','6'],capture_output=True,text=True); txt+=r.stdout+'\n\f\n'; os.remove(f'pout/acps/{p}')
        open(fn[:-4]+'.txt','w').write(txt); log.append(('ocr',name,len(txt)))
    os.remove(fn); log.append(('saved',name))
# 1) 2001-02 scanned
r=requests.get('https://apps.ams.usda.gov/Cotton/AnnualCNMarketNewsReports/CottonPriceStatistics/ACPS-2001-2002.pdf',headers=H,timeout=120)
if r.content[:4]==b'%PDF': save_pdf(r.content,'ACPS-2001-2002')
# 2) MMN file repository listings for annual (3896) and monthly (3895) price statistics
for slug in (3896,3895):
    for page in range(0,6):
        try:
            t=requests.get('https://mymarketnews.ams.usda.gov/filerepo/reports',params={'field_slug_id_value':slug,'page':page},headers=H,timeout=60).text
            links=sorted(set(re.findall(r'href="([^"]+\.pdf)"',t,re.I))); log.append(('filerepo',slug,page,links[:40]))
            if not links: break
        except Exception as e: log.append(('filerepo err',slug,str(e))); break
# 3) dated copies of the annual report, Jul 20 - Sep 10 of 2021..2025
for y in range(2021,2026):
    d=dt.date(y,7,20); got=False
    while d<=dt.date(y,9,10) and not got:
        for u in [f'https://s3.amazonaws.com/barchart-news-store-prod/external_links/USDA/{d:%Y%m%d}-cnaacps.pdf']:
            try:
                r=requests.get(u,headers=H,timeout=30)
                if r.status_code==200 and r.content[:4]==b'%PDF': save_pdf(r.content,f'cnaacps-{d:%Y%m%d}'); got=True; log.append(('barchart',u))
            except Exception: pass
        d+=dt.timedelta(days=1)
    if not got: log.append(('no annual found',y))
json.dump(log,open('pout/log2.json','w'),indent=1)
print(json.dumps(log,indent=1)[:6000])
