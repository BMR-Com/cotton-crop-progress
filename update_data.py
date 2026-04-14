#!/usr/bin/env python3
import os
import requests
import pandas as pd
from io import StringIO
import time
from datetime import datetime

API_KEY = os.environ['USDA_API_KEY']
BASE_URL = "https://quickstats.nass.usda.gov/api/api_GET"

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

FILE_RANGES = [
    ('data/cotton_2001_2005.csv', 2001, 2005),
    ('data/cotton_2006_2010.csv', 2006, 2010),
    ('data/cotton_2011_2015.csv', 2011, 2015),
    ('data/cotton_2016_2020.csv', 2016, 2020),
    ('data/cotton_2021_2025.csv', 2021, 2025),
]


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


def main():
    os.makedirs('data', exist_ok=True)
    current_year = datetime.now().year
    
    for filename, start_year, end_year in FILE_RANGES:
        print(f"\nProcessing {filename}...")
        
        # Load existing
        if os.path.exists(filename):
            existing = pd.read_csv(filename)
            print(f"  Loaded existing: {len(existing)} rows")
        else:
            existing = pd.DataFrame()
            print(f"  Creating new file")
        
        # Fetch current year if in range
        if start_year <= current_year <= end_year:
            print(f"  Fetching {current_year}...")
            new_data = []
            for key, desc in DATA_ITEMS.items():
                df = fetch_item(current_year, key, desc)
                if df is not None:
                    new_data.append(df)
            
            if new_data:
                new_df = pd.concat(new_data, ignore_index=True)
                print(f"  Fetched {len(new_df)} new rows")
                
                # Merge and dedup
                combined = pd.concat([existing, new_df], ignore_index=True)
                dup_cols = ['year', 'week_ending', 'state_name', 'short_desc', 'data_item_key']
                dup_cols = [c for c in dup_cols if c in combined.columns]
                combined = combined.drop_duplicates(subset=dup_cols, keep='last')
                print(f"  Final: {len(combined)} rows")
                combined.to_csv(filename, index=False)
            else:
                print(f"  No new data")
                existing.to_csv(filename, index=False)
        else:
            print(f"  Year {current_year} not in range, skipping")
            if not existing.empty:
                existing.to_csv(filename, index=False)
        
        if os.path.exists(filename):
            print(f"  Size: {os.path.getsize(filename)/1024/1024:.1f} MB")


if __name__ == '__main__':
    main()
    print("\nDone!")
