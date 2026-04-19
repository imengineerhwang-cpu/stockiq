import requests
import pandas as pd
from dotenv import load_dotenv
import os

load_dotenv()
DART_KEY = os.getenv("DART_API_KEY")
os.makedirs("data/financials", exist_ok=True)

def get_financial(corp_code, year, report_code, fs_div="CFS"):
    r = requests.get(
        "https://opendart.fss.or.kr/api/fnlttSinglAcntAll.json",
        params={
            "crtfc_key": DART_KEY,
            "corp_code": corp_code,
            "bsns_year": year,
            "reprt_code": report_code,
            "fs_div": fs_div,
        }
    )
    data = r.json()
    if data.get("status") != "000":
        print(f"  → {year} {report_code} 데이터 없음: {data.get('message')}")
        return None
    df = pd.DataFrame(data["list"])
    df["연도"] = year
    return df

def format_amount(val):
    try:
        num = int(str(val).replace(",", ""))
        billion = round(num / 100000000, 1)
        return f"{billion:,.1f}억원"
    except:
        return None

ACCOUNT_MAP = {
    "수익(매출액)": "매출액",
    "매출액": "매출액",
    "영업이익(손실)": "영업이익",
    "영업이익": "영업이익",
    "법인세비용(수익)": "법인세비용",
    "법인세비용": "법인세비용",
    "당기순이익(손실)": "당기순이익",
    "당기순이익": "당기순이익",
    "금융원가": "금융비용",
    "금융비용": "금융비용",
    "기타이익": "기타수익",
    "기타수익": "기타수익",
    "기타손실": "기타비용",
    "기타비용": "기타비용",
    "총포괄손익": "총포괄이익",
    "당기총포괄이익(손실)": "총포괄이익",
    "당기총포괄손익": "총포괄이익",
    "자산총계": "자산총계",
    "부채총계": "부채총계",
    "자본총계": "자본총계",
    "유동자산": "유동자산",
    "비유동자산": "비유동자산",
    "유동부채": "유동부채",
    "비유동부채": "비유동부채",
    "영업활동현금흐름": "영업활동현금흐름",
    "투자활동현금흐름": "투자활동현금흐름",
    "재무활동현금흐름": "재무활동현금흐름",
}

def pivot_statement(frames, sj_div):
    all_rows = []
    for df, label in frames:
        sub = df[df["sj_div"] == sj_div][["account_nm", "thstrm_amount"]].copy()
        sub["thstrm_amount"] = sub["thstrm_amount"].apply(format_amount)
        sub["account_nm"] = sub["account_nm"].map(ACCOUNT_MAP).fillna(sub["account_nm"])
        sub = sub.drop_duplicates(subset="account_nm")
        sub = sub.rename(columns={"account_nm": "항목", "thstrm_amount": label})
        sub = sub.set_index("항목")
        all_rows.append(sub)

    if not all_rows:
        return pd.DataFrame()

    result = pd.concat(all_rows, axis=1)
    return result

CORP_CODE = "00139719"
YEARS = ["2022", "2023", "2024"]

# 연간 수집
print("=== 연간 재무제표 수집 중 ===")
annual_frames = []
for year in YEARS:
    print(f"  {year}년...")
    df = get_financial(CORP_CODE, year, "11011")
    if df is not None:
        label = f"{year}/12"
        annual_frames.append((df, label))

# 분기 수집
print("\n=== 분기 재무제표 수집 중 ===")
quarter_frames = []
quarter_map = [
    ("2023", "11013", "2023/03"),
    ("2023", "11012", "2023/06"),
    ("2023", "11014", "2023/09"),
    ("2023", "11011", "2023/12"),
    ("2024", "11013", "2024/03"),
    ("2024", "11012", "2024/06"),
    ("2024", "11014", "2024/09"),
    ("2024", "11011", "2024/12"),
]
for year, code, label in quarter_map:
    print(f"  {label}...")
    df = get_financial(CORP_CODE, year, code)
    if df is not None:
        quarter_frames.append((df, label))

# 연간 저장
if annual_frames:
    for sj_div, name in [("CIS","연간_손익계산서"), ("BS","연간_재무상태표"), ("CF","연간_현금흐름표")]:
        df_pivot = pivot_statement(annual_frames, sj_div)
        if not df_pivot.empty:
            df_pivot.to_csv(f"data/financials/{name}.csv", encoding="utf-8-sig")
            print(f"✓ {name}.csv 저장")

# 분기 저장
if quarter_frames:
    for sj_div, name in [("CIS","분기_손익계산서"), ("BS","분기_재무상태표"), ("CF","분기_현금흐름표")]:
        df_pivot = pivot_statement(quarter_frames, sj_div)
        if not df_pivot.empty:
            df_pivot.to_csv(f"data/financials/{name}.csv", encoding="utf-8-sig")
            print(f"✓ {name}.csv 저장")

print("\n=== 완료! ===")