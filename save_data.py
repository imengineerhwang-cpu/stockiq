"""관심 종목 시세·공시·재무 데이터 수집.

companies.csv에 등록된 회사를 기준으로:
  - 시세 → data/stocks.csv
  - 공시 → data/disclosures.csv
  - 재무제표 → data/companies/{name}/financials/*.csv
"""
import os
import requests
import pandas as pd
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from datetime import datetime, timedelta
import financials  # DART 재무제표 수집 모듈

load_dotenv()
DART_KEY = os.getenv("DART_API_KEY")

os.makedirs("data", exist_ok=True)

# ── 회사 목록 로드 ──────────────────────────────────────────────────────────
companies = pd.read_csv("companies.csv")
print(f"등록 회사: {companies['name'].tolist()}\n")


def get_stock_info(stock_code):
    url = f"https://finance.naver.com/item/main.naver?code={stock_code}"
    headers = {"User-Agent": "Mozilla/5.0"}
    r = requests.get(url, headers=headers, timeout=10)
    soup = BeautifulSoup(r.text, "html.parser")
    price = soup.select_one("p.no_today span.blind")
    lines = [l.strip() for l in soup.get_text().splitlines() if l.strip()]
    per = next((lines[i+1] for i, l in enumerate(lines) if l == "PER(배)" and i+1 < len(lines)), "N/A")
    marcap = next(
        (lines[i+1] + "억원" for i, l in enumerate(lines)
         if l == "시가총액" and i+2 < len(lines) and lines[i+2] == "억원"),
        "N/A"
    )
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
        },
        timeout=15,
    )
    return r.json().get("list", [])


# ── 시세 수집 ───────────────────────────────────────────────────────────────
print("=== 시세 수집 ===")
stock_rows = []
for _, row in companies.iterrows():
    try:
        info = get_stock_info(str(row["stock_code"]).zfill(6))
        info["종목명"] = row["name"]
        stock_rows.append(info)
        print(f"  ✓ {row['name']} — {info['현재가']}원")
    except Exception as e:
        print(f"  ✗ {row['name']} 실패: {e}")

pd.DataFrame(stock_rows).to_csv("data/stocks.csv", index=False, encoding="utf-8-sig")
print("→ data/stocks.csv 저장\n")

# ── 공시 수집 ───────────────────────────────────────────────────────────────
print("=== 공시 수집 ===")
disc_rows = []
for _, row in companies.iterrows():
    try:
        items = get_disclosures(str(row["corp_code"]).zfill(8))
        for d in items:
            disc_rows.append({
                "수집일시": datetime.now().strftime("%Y-%m-%d %H:%M"),
                "종목명": row["name"],
                "공시일": d["rcept_dt"],
                "제목": d["report_nm"],
            })
        print(f"  ✓ {row['name']} — {len(items)}건")
    except Exception as e:
        print(f"  ✗ {row['name']} 실패: {e}")

pd.DataFrame(disc_rows).to_csv("data/disclosures.csv", index=False, encoding="utf-8-sig")
print("→ data/disclosures.csv 저장\n")

# ── 재무제표 수집 ───────────────────────────────────────────────────────────
print("=== 재무제표 수집 ===")
for _, row in companies.iterrows():
    name = row["name"]
    corp_code = str(row["corp_code"]).zfill(8)
    fin_dir = f"data/companies/{name}/financials"
    os.makedirs(fin_dir, exist_ok=True)
    try:
        financials.save_all(corp_code=corp_code, output_dir=fin_dir)
        print(f"  ✓ {name} 재무제표 저장 → {fin_dir}")
    except Exception as e:
        print(f"  ✗ {name} 재무제표 실패: {e}")

print("\n완료!")
