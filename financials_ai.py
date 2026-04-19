import os
import json
import requests
import pandas as pd
import anthropic
from dotenv import load_dotenv

load_dotenv()
DART_KEY = os.getenv("DART_API_KEY")
FORCE_REGEN = os.getenv("FORCE_REGEN") == "1"
client = anthropic.Anthropic()
os.makedirs("data/financials", exist_ok=True)
os.makedirs("data/raw", exist_ok=True)

CORP_CODE = "00139719"
LATEST_YEAR = 2025
NUM_YEARS = 10
YEARS = [str(y) for y in range(LATEST_YEAR - NUM_YEARS + 1, LATEST_YEAR + 1)]
QUARTER_CODES = [("11013", "03"), ("11012", "06"), ("11014", "09"), ("11011", "12")]
QUARTER_MAP = [(y, code, f"{y}/{m}") for y in YEARS for code, m in QUARTER_CODES]
STATEMENTS = [
    ("CIS", "손익계산서"),
    ("BS",  "재무상태표"),
    ("CF",  "현금흐름표"),
]

SYSTEM_PROMPT = """당신은 DART 재무제표 원본 데이터를 정리하는 전문가입니다.

다음 규칙을 반드시 지켜서 정리하세요:
1. 연도별로 계정명이 다른 경우 통일 (예: "영업이익", "영업이익(손실)" → "영업이익")
2. 중복 항목 제거
3. 재무제표 순서에 맞게 정렬
4. 금액은 억원 단위, 소수점 없이 정수, 천단위 콤마, 숫자만 표기 (억원 글자 절대 붙이지 말 것, 예: 5,750)
5. 계층 구조 반영 — 대분류 항목은 그대로, 하위 항목은 앞에 공백 2칸씩 추가
   재무상태표:
   "자산" → "자산"
   "유동자산" → "  유동자산"
   "현금및현금성자산" → "    현금및현금성자산"
   "비유동자산" → "  비유동자산"
   "부채" → "부채"
   "유동부채" → "  유동부채"
   손익계산서:
   "매출액" → "매출액"
   "  매출원가" → "  매출원가"
   "매출총이익" → "매출총이익"
   "  판매비와관리비" → "  판매비와관리비"
   "영업이익" → "영업이익"
   현금흐름표:
   "영업활동으로인한현금흐름" → "영업활동으로인한현금흐름"
   "당기순이익" → "  당기순이익"
   "가감" → "  가감"
   "자산부채의증감" → "  자산부채의증감"
   "법인세납부" → "  법인세납부"
   "투자활동으로인한현금흐름" → "투자활동으로인한현금흐름"
   "유형자산의취득" → "  유형자산의취득"
   "무형자산의취득" → "  무형자산의취득"
   "재무활동으로인한현금흐름" → "재무활동으로인한현금흐름"
   "단기차입금의증가" → "  단기차입금의증가"
   "이자의지급" → "  이자의지급"
   "배당금지급" → "  배당금지급"
   "현금및현금성자산의순증가" → "현금및현금성자산의순증가"
   "기초현금및현금성자산" → "기초현금및현금성자산"
   "기말현금및현금성자산" → "기말현금및현금성자산"
6. 핵심 항목만 남기기 (지나치게 세부적인 항목 제외)

첫번째 열 이름은 반드시 "항목(단위:억원)" 으로 합니다.
응답은 다음 JSON 형식으로만, 다른 텍스트 없이:
{"items": [{"항목(단위:억원)": "...", "<기간1>": "5,750", "<기간2>": "...", ...}, ...]}"""


def get_financial(corp_code, year, report_code, fs_div="CFS"):
    cache_path = f"data/raw/{corp_code}_{year}_{report_code}_{fs_div}.csv"
    if os.path.exists(cache_path):
        print(f"  [캐시] {cache_path}")
        return pd.read_csv(cache_path, encoding="utf-8-sig")

    r = requests.get(
        "https://opendart.fss.or.kr/api/fnlttSinglAcntAll.json",
        params={
            "crtfc_key": DART_KEY,
            "corp_code": corp_code,
            "bsns_year": year,
            "reprt_code": report_code,
            "fs_div": fs_div,
        },
    )
    data = r.json()
    if data.get("status") != "000":
        print(f"  [없음] {year} {report_code}")
        return None

    df = pd.DataFrame(data["list"])
    df.to_csv(cache_path, index=False, encoding="utf-8-sig")
    print(f"  [DART] {cache_path}")
    return df


def collect_all(corp_code, years, quarter_map):
    print("=== 연간 데이터 수집 ===")
    annual_raw = {}
    for year in years:
        df = get_financial(corp_code, year, "11011")
        if df is not None:
            annual_raw[f"{year}/12"] = df

    print("\n=== 분기 데이터 수집 ===")
    quarter_raw = {}
    for year, code, label in quarter_map:
        df = get_financial(corp_code, year, code)
        if df is not None:
            quarter_raw[label] = df

    return annual_raw, quarter_raw


def _to_int(v):
    try:
        return int(float(str(v).replace(",", "")))
    except (ValueError, TypeError):
        return None


def convert_q4_cis_to_single(quarter_raw):
    """사업보고서(11011)의 CIS 값은 연간 누적이므로 Q4 단독값으로 변환."""
    periods_by_year = {}
    for period in quarter_raw:
        periods_by_year.setdefault(period.split("/")[0], []).append(period)

    for year, periods in periods_by_year.items():
        q4_period = f"{year}/12"
        priors = [p for p in periods if p != q4_period]
        if q4_period not in quarter_raw or len(priors) < 3:
            continue

        cum = {}
        for p in priors:
            cis_rows = quarter_raw[p][quarter_raw[p]["sj_div"] == "CIS"]
            for _, row in cis_rows.iterrows():
                key = row["account_id"] if pd.notna(row.get("account_id")) else row["account_nm"]
                amt = _to_int(row["thstrm_amount"])
                if amt is not None:
                    cum[key] = cum.get(key, 0) + amt

        q4_df = quarter_raw[q4_period].copy()

        def fix(row):
            if row["sj_div"] != "CIS":
                return row["thstrm_amount"]
            key = row["account_id"] if pd.notna(row.get("account_id")) else row["account_nm"]
            amt = _to_int(row["thstrm_amount"])
            return str(amt - cum.get(key, 0)) if amt is not None else row["thstrm_amount"]

        q4_df["thstrm_amount"] = q4_df.apply(fix, axis=1)
        quarter_raw[q4_period] = q4_df

    return quarter_raw


def fix_cis_q4_in_df(df, annual_df):
    """분기 CIS CSV의 /12 열을 '연간값 - (Q1+Q2+Q3)'로 덮어쓴다. 멱등."""
    label_col = annual_df.columns[0]
    ann_lookup = {str(r[label_col]).strip(): r for _, r in annual_df.iterrows()}
    years = sorted({c.split("/")[0] for c in df.columns if "/" in c})

    for year in years:
        q_cols = [f"{year}/{m}" for m in ("03", "06", "09", "12")]
        if not all(c in df.columns for c in q_cols) or q_cols[3] not in annual_df.columns:
            continue
        for idx in df.index:
            label = str(idx).strip()
            if label not in ann_lookup:
                continue
            ann = _to_int(ann_lookup[label][q_cols[3]])
            q1, q2, q3 = [_to_int(df.at[idx, c]) for c in q_cols[:3]]
            if None in (ann, q1, q2, q3):
                continue
            df.at[idx, q_cols[3]] = f"{ann - q1 - q2 - q3:,}"
    return df


def build_raw_text(raw_dict, sj_div, periods):
    lines = []
    for period in periods:
        if period not in raw_dict:
            continue
        df = raw_dict[period]
        cols = ["account_nm", "thstrm_amount"]
        has_indent = "indent" in df.columns
        if has_indent:
            cols.append("indent")
        sub = df[df["sj_div"] == sj_div][cols]
        lines.append(f"\n[{period}]")
        for _, row in sub.iterrows():
            amt = _to_int(row["thstrm_amount"])
            val = f"{round(amt/100000000):,}" if amt is not None else row["thstrm_amount"]
            indent = int(row["indent"]) if has_indent and pd.notna(row.get("indent")) else 0
            lines.append(f"  {'  ' * indent}{row['account_nm']}: {val}")
    return "\n".join(lines)


def ai_clean_statement(raw_data, statement_type, periods):
    user_msg = f"{statement_type} 원본 데이터 (기간: {', '.join(periods)}):\n\n{raw_data}"
    with client.messages.stream(
        model="claude-sonnet-4-5",
        max_tokens=32000,
        system=[{"type": "text", "text": SYSTEM_PROMPT, "cache_control": {"type": "ephemeral"}}],
        messages=[{"role": "user", "content": user_msg}],
    ) as stream:
        message = stream.get_final_message()

    u = message.usage
    print(f"    토큰: 입력 {u.input_tokens} / 캐시쓰기 {u.cache_creation_input_tokens} / 캐시읽기 {u.cache_read_input_tokens} / 출력 {u.output_tokens}")

    text = message.content[0].text.strip()
    if "```" in text:
        text = text.split("```")[1]
        if text.startswith("json"):
            text = text[4:]
    return pd.DataFrame(json.loads(text)["items"]).set_index("항목(단위:억원)")


def generate_csv(sj_div, name, raw_dict, periods, kind):
    path = f"data/financials/{kind}_{name}.csv"
    if not FORCE_REGEN and os.path.exists(path):
        print(f"  [SKIP] {path}")
        return
    print(f"  [AI]   {kind}_{name}")
    df = ai_clean_statement(build_raw_text(raw_dict, sj_div, periods), name, periods)
    df.to_csv(path, encoding="utf-8-sig")


def main():
    annual_raw, quarter_raw = collect_all(CORP_CODE, YEARS, QUARTER_MAP)
    quarter_raw = convert_q4_cis_to_single(quarter_raw)
    annual_periods = [f"{y}/12" for y in YEARS]
    quarter_periods = [q[2] for q in QUARTER_MAP]

    print("\n=== AI 정리 ===")
    for sj_div, name in STATEMENTS:
        generate_csv(sj_div, name, annual_raw,  annual_periods,  "연간")
        generate_csv(sj_div, name, quarter_raw, quarter_periods, "분기")

        if sj_div == "CIS":
            q_path = f"data/financials/분기_{name}.csv"
            a_path = f"data/financials/연간_{name}.csv"
            if os.path.exists(q_path) and os.path.exists(a_path):
                q = pd.read_csv(q_path, dtype=str).set_index("항목(단위:억원)")
                q = fix_cis_q4_in_df(q, pd.read_csv(a_path, dtype=str))
                q.to_csv(q_path, encoding="utf-8-sig")

    print("\n=== 완료 ===")


if __name__ == "__main__":
    main()
