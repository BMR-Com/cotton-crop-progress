import os
import requests
import pandas as pd
from io import StringIO
import time
from datetime import datetime

API_KEY = os.environ['USDA_API_KEY']
BASE_URL = "https://quickstats.nass.usda.gov/api/api_GET"
CSV_FILE = "cotton_historical.csv"

DATA_ITEMS = {
    "condition_current_excellent": "COTTON, UPLAND - CONDITION, MEASURED IN PCT EXCELLENT",
    "condition_current_fair": "COTTON, UPLAND - CONDITION, MEASURED IN PCT FAIR",
    "condition_current_good": "COTTON, UPLAND - CONDITION, MEASURED IN PCT GOOD",
    "condition_current_poor": "COTTON, UPLAND - CONDITION, MEASURED IN PCT POOR",
    "condition_current_very_poor": "COTTON, UPLAND - CONDITION, MEASURED IN PCT VERY POOR",
    "condition_5yr_excellent": "COTTON, UPLAND - CONDITION, 5 YEAR AVG, MEASURED IN PCT EXCELLENT",
    "condition_5yr_fair": "COTTON, UPLAND - CONDITION, 5 YEAR AVG, MEASURED IN PCT FAIR",
    "condition_5yr_good": "COTTON, UPLAND - CONDITION, 5 YEAR AVG, MEASURED IN PCT GOOD",
    "condition_5yr_poor": "COTTON, UPLAND - CONDITION, 5 YEAR AVG, MEASURED IN PCT POOR",
    "condition_5yr_very_poor": "COTTON, UPLAND - CONDITION, 5 YEAR AVG, MEASURED IN PCT VERY POOR",
    "condition_prev_excellent": "COTTON, UPLAND - CONDITION, PREVIOUS YEAR, MEASURED IN PCT EXCELLENT",
    "condition_prev_fair": "COTTON, UPLAND - CONDITION, PREVIOUS YEAR, MEASURED IN PCT FAIR",
    "condition_prev_good": "COTTON, UPLAND - CONDITION, PREVIOUS YEAR, MEASURED IN PCT GOOD",
    "condition_prev_poor": "COTTON, UPLAND - CONDITION, PREVIOUS YEAR, MEASURED IN PCT POOR",
    "condition_prev_very_poor": "COTTON, UPLAND - CONDITION, PREVIOUS YEAR, MEASURED IN PCT VERY POOR",
    "progress_current_bolls_opening": "COTTON, UPLAND - PROGRESS, MEASURED IN PCT BOLLS OPENING",
    "progress_current_harvested": "COTTON, UPLAND - PROGRESS, MEASURED IN PCT HARVESTED",
    "progress_current_planted": "COTTON, UPLAND - PROGRESS, MEASURED IN PCT PLANTED",
    "progress_current_setting_bolls": "COTTON, UPLAND - PROGRESS, MEASURED IN PCT SETTING BOLLS",
    "progress_current_squaring": "COTTON, UPLAND - PROGRESS, MEASURED IN PCT SQUARING",
    "progress_current_emerged": "COTTON, UPLAND - PROGRESS, MEASURED IN PCT EMERGED",
    "progress_5yr_bolls_opening": "COTTON, UPLAND - PROGRESS, 5 YEAR AVG, MEASURED IN PCT BOLLS OPENING",
    "progress_5yr_harvested": "COTTON, UPLAND - PROGRESS, 5 YEAR AVG, MEASURED IN PCT HARVESTED",
    "progress_5yr_planted": "COTTON, UPLAND - PROGRESS, 5 YEAR AVG, MEASURED IN PCT PLANTED",
    "progress_5yr_setting_bolls": "COTTON, UPLAND - PROGRESS, 5 YEAR AVG, MEASURED IN PCT SETTING BOLLS",
    "progress_5yr_squaring": "COTTON, UPLAND - PROGRESS, 5 YEAR AVG, MEASURED IN PCT SQUARING",
    "progress_5yr_emerged": "COTTON, UPLAND - PROGRESS, 5 YEAR AVG, MEASURED IN PCT EMERGED",
    "progress_prev_bolls_opening": "COTTON, UPLAND - PROGRESS, PREVIOUS YEAR, MEASURED IN PCT BOLLS OPENING",
    "progress_prev_harvested": "COTTON, UPLAND - PROGRESS, PREVIOUS YEAR, MEASURED IN PCT HARVESTED",
    "progress_prev_planted": "COTTON, UPLAND - PROGRESS, PREVIOUS YEAR, MEASURED IN PCT PLANTED",
    "progress_prev_setting_bolls": "COTTON, UPLAND - PROGRESS, PREVIOUS YEAR, MEASURED IN PCT SETTING BOLLS",
    "progress_prev_squaring": "COTTON, UPLAND - PROGRESS, PREVIOUS YEAR, MEASURED IN PCT SQUARING",
    "progress_prev_emerged": "COTTON, UPLAND - PROGRESS, PREVIOUS YEAR, MEASURED IN PCT EMERGED",
}

def fetch_item(year, key, desc):
    all_dfs = []
    for level in ['NATIONAL', 'STATE']:
        params = {
            "key": API_KEY,
            "program_desc": "SURVEY",
            "sector_desc": "CROPS",
            "group_desc": "FIELD CROPS",
            "commodity_desc": "COTTON",
            "domain_desc": "TOTAL",
            "year": year,
            "period_type": "WEEKLY",
            "short_desc": desc,
            "agg_level_desc": level,
            "format": "CSV"
        }
        try:
            r = requests.get(BASE_URL, params=params, timeout=60)
            if r.status_code == 200:
                df = pd.read_csv(StringIO(r.text))
                if len(df) > 0:
                    df['data_item_key'] = key
                    df['level'] = level
                    all_dfs.append(df)
        except:
            pass
        time.sleep(0.2)
    return pd.concat(all_dfs) if all_dfs else None

# Load existing or create new
if os.path.exists(CSV_FILE):
    existing = pd.read_csv(CSV_FILE)
    print(f"Loaded existing: {len(existing)} rows")
else:
    existing = pd.DataFrame()
    print("No existing file")

# Fetch current year only (respects 50k limit)
current_year = datetime.now().year
print(f"Fetching year {current_year}...")

new_data = []
for key, desc in DATA_ITEMS.items():
    df = fetch_item(current_year, key, desc)
    if df is not None:
        new_data.append(df)

if new_data:
    new_df = pd.concat(new_data, ignore_index=True)
    print(f"Fetched {len(new_df)} new rows")
    
    # Merge and deduplicate
    combined = pd.concat([existing, new_df], ignore_index=True)
    combined = combined.drop_duplicates(
        subset=['year', 'week_ending', 'state_name', 'short_desc', 'data_item_key'],
        keep='last'
    )
    print(f"Final: {len(combined)} rows")
else:
    combined = existing
    print("No new data")

# Save
combined.to_csv(CSV_FILE, index=False)
print(f"Saved {CSV_FILE} ({os.path.getsize(CSV_FILE)/1024/1024:.1f} MB)")
