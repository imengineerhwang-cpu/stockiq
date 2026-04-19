import requests
import zipfile
import io
import pandas as pd
from dotenv import load_dotenv
import os

load_dotenv()
DART_KEY = os.getenv("DART_API_KEY")

def get_corp_codes():
    url = "https://opendart.fss.or.kr/api/corpCode.xml"
    r = requests.get(url, params={"crtfc_key": DART_KEY})
    with zipfile.ZipFile(io.BytesIO(r.content)) as z:
        with z.open("CORPCODE.xml") as f:
            df = pd.read_xml(f)
    return df[df["stock_code"].notna()]

df = get_corp_codes()
print(f"전체 상장사 수: {len(df)}")
print(df[df["corp_name"].str.contains("와이지-원")][["corp_name","corp_code","stock_code"]])

def get_disclosures(corp_code, days=30):
    from datetime import datetime, timedelta
    today = datetime.today()
    start = (today - timedelta(days=days)).strftime("%Y%m%d")
    r = requests.get(
        "https://opendart.fss.or.kr/api/list.json",
        params={
            "crtfc_key": DART_KEY,
            "corp_code": corp_code,
            "bgn_de": start,
            "end_de": today.strftime("%Y%m%d"),
            "page_count": 20,
        }
    )
    data = r.json()
    return data.get("list", [])

# 와이지-원 corp_code (8자리)
corp_code = "00139719"
disclosures = get_disclosures(corp_code, days=30)

if disclosures:
    for d in disclosures:
        print(f"[{d['rcept_dt']}] {d['report_nm']}")
else:
    print("최근 30일 공시 없음")