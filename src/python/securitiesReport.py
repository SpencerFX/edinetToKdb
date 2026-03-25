# ============================================

# ============================================

# ============================================
# import core libraries
import os
import time
import json
import requests
import pandas as pd
from datetime import date, timedelta
import edinet_tools
# ============================================

# ============================================
# Note: Is this necessary?
# endpoints
DOC_LIST_URL = "https://api.edinet-fsa.go.jp/api/v2/documents.json"
DOC_GET_URL = "https://api.edinet-fsa.go.jp/api/v2/documents/{doc_id}"
# ============================================

# ============================================
# Core data ingest
from datetime import date, timedelta
import pandas as pd
import calendar

def month_ranges(start_year: int, start_month: int, end_year: int, end_month: int):
    y, m = start_year, start_month
    while (y < end_year) or (y == end_year and m <= end_month):
        month_start = date(y, m, 1)
        month_end = date(y, m, calendar.monthrange(y, m)[1])
        yield month_start, month_end

        if m == 12:
            y += 1
            m = 1
        else:
            m += 1

def daterange(start_date: date, end_date: date):
    d = start_date
    while d <= end_date:
        yield d
        d += timedelta(days=1)

all_frames = []

for month_start, month_end in month_ranges(2025, 3, 2025, 12):
    print(f"Processing {month_start:%Y-%m} ...")

    for d in daterange(month_start, month_end):
        daily = collect_annual_statements_by_date(d.isoformat())
        all_frames.append(daily)

df_all2 = pd.concat(all_frames, ignore_index=True)
# ============================================

# ============================================
# Save the data to csv
OUTPUT_DIR = "edinet_output"
os.makedirs(OUTPUT_DIR, exist_ok=True)

full_path = os.path.join(OUTPUT_DIR, "ipo_financials_2025_full.csv")
df_all2.to_csv(full_path, index=False, encoding="utf-8-sig")
# ============================================