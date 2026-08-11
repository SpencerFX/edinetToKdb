# import core libraries
from bs4 import BeautifulSoup
import calendar
from dataclasses import asdict
from datetime import date, timedelta
from __future__ import annotations
import json
import llm
import os
import pandas as pd
from pathlib import Path
from pydantic import BaseModel, Field
import re
import requests
import shutil
import time
from typing import Iterable, Optional
import zipfile

# We will use a llm to construct data from text
# As such, please choose a provider and set an api key
os.environ["OPENAI_API_KEY"] = "some key"
API_KEY = os.environ["EDINET_API_KEY"] 

LIST_URL = "https://api.edinet-fsa.go.jp/api/v2/documents.json"
GET_URL = "https://api.edinet-fsa.go.jp/api/v2/documents/{doc_id}"

def daterange(start_date: date, end_date: date):
    d = start_date
    while d <= end_date:
        yield d
        d += timedelta(days=1)

def fetch_doc_list(day: str) -> dict:
    params = {
        "date": day,
        "type": 2,  # JSON metadata list
        "Subscription-Key": API_KEY,
    }
    r = requests.get(LIST_URL, params=params, timeout=60)
    r.raise_for_status()
    return r.json()

def is_teisei_hakkou_tourokusho(item: dict) -> bool:
    text = " ".join([
        str(item.get("docDescription", "") or ""),
        str(item.get("docTypeCode", "") or ""),
        str(item.get("formCode", "") or ""),
        str(item.get("ordinanceCode", "") or ""),
    ])
    return "訂正発行登録書" in text

def collect_teisei_hakkou_tourokusho(start_date: date, end_date: date) -> pd.DataFrame:
    rows = []

    for d in daterange(start_date, end_date):
        day = d.isoformat()
        data = fetch_doc_list(day)
        results = data.get("results", [])

        matches = [x for x in results if is_teisei_hakkou_tourokusho(x)]
        print(f"{day}: {len(matches)} matches")

        for item in matches:
            rows.append({
                "filed_date": day,
                "docID": item.get("docID"),
                "parentDocID": item.get("parentDocID"),
                "edinetCode": item.get("edinetCode"),
                "secCode": item.get("secCode"),
                "filerName": item.get("filerName"),
                "docDescription": item.get("docDescription"),
                "docTypeCode": item.get("docTypeCode"),
                "formCode": item.get("formCode"),
                "ordinanceCode": item.get("ordinanceCode"),
                "submitDateTime": item.get("submitDateTime"),
            })

        time.sleep(0.1)

    return pd.DataFrame(rows)

def download_document(doc_id: str, out_path: str, file_type: int = 1):
    """
    file_type:
      1 = zip package
      2 = PDF if available
    """
    params = {
        "type": file_type,
        "Subscription-Key": API_KEY,
    }
    r = requests.get(GET_URL.format(doc_id=doc_id), params=params, timeout=120)
    r.raise_for_status()

    with open(out_path, "wb") as f:
        f.write(r.content)

# Example usage
df_teisei = collect_teisei_hakkou_tourokusho(
    start_date=date(2026, 1, 1),
    end_date=date(2026, 12, 31)
)

# Download all matched filings as ZIP
for _, row in df_teisei.iterrows():
    doc_id = row["docID"]
    if pd.notna(doc_id):
        download_document(str(doc_id), f"{doc_id}.zip", file_type=1)