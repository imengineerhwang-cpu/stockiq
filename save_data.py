import requests
import zipfile
import io
import pandas as pd
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from datetime import datetime, timedelta
import os

load_dotenv()
DART_KEY = os.getenv("DART_API_KEY")

# 저장 폴더 생성
os.makedirs("data", exist_ok=True)

def get_stock_info(stock_code):
    url = f"https://finance.naver.com/item/main.naver?code={stock_code}"
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"}
    r = requests.get(url, headers=headers)
    soup = BeautifulSoup(r.text, "html.parser")

    price = soup.select_one("p.no_today span.blind")
    lines = [l.strip() for l in soup.get_text().splitlines() if l.strip()]

    per = "N/A"
    for i, line in enumerate(lines):
        if line == "PER(배)" and i+1 < len(lines):
            per = lines[i+1]
            break

    marcap = "N/A"
    for i, line in enumerate(lines):
        if line == "시가총액" and i+2 < len(lines) and lines[i+2] == "억원":
            marcap = lines[i+1] + "억원"
            break

    return {
        "수집일시": datetime.now().strftime("%Y-%m-%d %H:%M"),
        "종목코드": stock_code,
        "현재가": price.text.strip() if price else "N/A",
        "PER": per,
        "시가총액": marcap,
    }

def get_disclosures(corp_code, days=30):
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
    return r.json().get("list", [])

# 관심 종목 목록 (종목코드, corp_code)
watchlist = [
    {"name": "와이지-원", "stock_code": "019210", "corp_code": "00139719"},
]

# 시세 수집 + 저장
print("=== 시세 수집 중 ===")
stock_rows = []
for stock in watchlist:
    info = get_stock_info(stock["stock_code"])
    info["종목명"] = stock["name"]
    stock_rows.append(info)
    print(f"✓ {stock['name']} — {info['현재가']}원")

df_stocks = pd.DataFrame(stock_rows)
df_stocks.to_csv("data/stocks.csv", index=False, encoding="utf-8-sig")
print("→ data/stocks.csv 저장 완료\n")

# 공시 수집 + 저장
print("=== 공시 수집 중 ===")
disc_rows = []
for stock in watchlist:
    disclosures = get_disclosures(stock["corp_code"])
    for d in disclosures:
        disc_rows.append({
            "수집일시": datetime.now().strftime("%Y-%m-%d %H:%M"),
            "종목명": stock["name"],
            "공시일": d["rcept_dt"],
            "제목": d["report_nm"],
        })
    print(f"✓ {stock['name']} — 공시 {len(disclosures)}건")

df_disc = pd.DataFrame(disc_rows)
df_disc.to_csv("data/disclosures.csv", index=False, encoding="utf-8-sig")
print("→ data/disclosures.csv 저장 완료")