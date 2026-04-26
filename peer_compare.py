"""YG-1 vs KMT vs OSG 연도별 재무·밸류에이션 비교표 빌더 (2015~2025).

데이터 소스:
- YG-1: DART 사업보고서 CSV (data/raw/00139719_YYYY_11011_CFS.csv)
- KMT : SEC XBRL companyfacts API
- OSG : IR 결산단신 PDF (data/raw/osg/fy*.pdf) — 본 스크립트엔 이미 추출한 값 하드코딩

산출 항목 (단위: 억원, 연평균 환율 적용):
- 매출액, 영업이익, 당기순이익, 영업이익률(%)
- 재고자산, 재고자산 중 원재료
- 시가총액(연말), PER, PBR
"""
import os, sys, json, requests, glob
import pandas as pd
import yfinance as yf
from dotenv import load_dotenv

load_dotenv()
sys.stdout.reconfigure(encoding="utf-8") if hasattr(sys.stdout, "reconfigure") else None

YEARS = list(range(2015, 2026))


# ── 환율 (연평균) ─────────────────────────────────────────────────────────────
def yearly_avg_fx() -> tuple[pd.Series, pd.Series]:
    """USD→KRW, JPY→KRW 연평균 환율."""
    krw = yf.Ticker("KRW=X").history(start="2014-12-01", interval="1d", auto_adjust=False)
    jpy = yf.Ticker("JPY=X").history(start="2014-12-01", interval="1d", auto_adjust=False)
    krw.index = krw.index.tz_localize(None)
    jpy.index = jpy.index.tz_localize(None)
    krw_y = krw["Close"].resample("YE").mean()
    jpy_y = jpy["Close"].resample("YE").mean()
    krw_y.index = krw_y.index.year
    jpy_y.index = jpy_y.index.year
    # JPY→KRW = (USD→KRW) / (USD→JPY)
    jpy_to_krw = krw_y / jpy_y
    return krw_y, jpy_to_krw


# ── 시가총액 (연말 종가 × 연말 발행주식수) ────────────────────────────────────
YG1_SHARES_Q = {
    "2015-12-31": 28_466_303, "2016-12-31": 29_457_513, "2017-12-31": 31_448_331,
    "2018-12-31": 31_449_158, "2019-12-31": 31_449_239, "2020-12-31": 34_193_728,
    "2021-12-31": 34_193_728, "2022-12-31": 34_193_728, "2023-12-31": 34_193_728,
    "2024-12-31": 37_193_728, "2025-12-31": 37_193_728,
}
KMT_SHARES_FY_END = {  # SEC 10-Q Jan31 (Dec31 근사)
    2015: 79_672_229, 2016: 80_193_977, 2017: 81_573_415, 2018: 82_233_615,
    2019: 82_898_074, 2020: 83_533_003, 2021: 83_090_710, 2022: 80_527_022,
    2023: 79_269_782, 2024: 77_360_327, 2025: 76_198_792,
}
OSG_SHARES_FY_END = {  # IR PDF Nov30 (보통주 ex-treasury)
    2015: 95_046_256, 2016: 90_025_147, 2017: 97_184_575, 2018: 97_970_188,
    2019: 97_191_831, 2020: 97_450_361, 2021: 97_668_266, 2022: 95_668_994,
    2023: 95_944_700, 2024: 84_921_343, 2025: 82_150_384,
}


def yearly_close(ticker: str) -> pd.Series:
    d = yf.Ticker(ticker).history(start="2014-12-01", interval="1d", auto_adjust=False)
    d.index = d.index.tz_localize(None)
    y = d["Close"].resample("YE").last()
    y.index = y.index.year
    return y


# ── YG-1 DART CSV 파싱 ────────────────────────────────────────────────────────
DART_KEY = os.getenv("DART_API_KEY")
YG1_CORP = "00139719"


def _pick_first(df, sj_div, name_options, col="thstrm_amount"):
    """sj_div 내에서 account_nm 이 name_options 중 하나에 정확/부분 일치하는 첫 값."""
    sub = df[df["sj_div"] == sj_div]
    for name in name_options:
        # 정확 일치 우선
        exact = sub[sub["account_nm"] == name]
        if not exact.empty:
            try: return float(exact.iloc[0][col])
            except: pass
        # 부분 일치
        contains = sub[sub["account_nm"].str.contains(name, na=False, regex=False)]
        if not contains.empty:
            try: return float(contains.iloc[0][col])
            except: pass
    return None


def yg1_metrics() -> pd.DataFrame:
    rows = []
    for y in YEARS:
        if y < 2016:
            continue
        path = f"data/raw/{YG1_CORP}_{y}_11011_CFS.csv"
        if not os.path.exists(path):
            continue
        df = pd.read_csv(path)
        # 연도별 항목명 변동: 2016~2018 "매출액"/"영업이익", 2020 "영업이익(손실)", 2024 "수익(매출액)"
        revenue = _pick_first(df, "CIS", ["수익(매출액)", "매출액", "수익"])
        op_inc  = _pick_first(df, "CIS", ["영업이익(손실)", "영업이익"])
        ni = _pick_first(df, "CIS", [
            "지배기업의 소유주에게 귀속되는 당기순이익(손실)",
            "지배기업의 소유주에게 귀속되는 당기순이익",
            "당기순이익(손실)", "당기순이익",
        ])
        equity = _pick_first(df, "BS", ["자본총계"])
        inv    = _pick_first(df, "BS", ["재고자산"])
        rows.append({"year": y, "revenue_krw": revenue, "op_income_krw": op_inc,
                     "net_income_krw": ni, "equity_krw": equity, "inventory_krw": inv})
        # 2015 = 2016 보고서의 frmtrm
        if y == 2016:
            rev15 = _pick_first(df, "CIS", ["수익(매출액)", "매출액", "수익"], col="frmtrm_amount")
            op15  = _pick_first(df, "CIS", ["영업이익(손실)", "영업이익"], col="frmtrm_amount")
            ni15  = _pick_first(df, "CIS", [
                "지배기업의 소유주에게 귀속되는 당기순이익(손실)",
                "지배기업의 소유주에게 귀속되는 당기순이익",
                "당기순이익(손실)", "당기순이익",
            ], col="frmtrm_amount")
            eq15  = _pick_first(df, "BS", ["자본총계"], col="frmtrm_amount")
            inv15 = _pick_first(df, "BS", ["재고자산"], col="frmtrm_amount")
            rows.append({"year": 2015, "revenue_krw": rev15, "op_income_krw": op15,
                         "net_income_krw": ni15, "equity_krw": eq15, "inventory_krw": inv15})
    return pd.DataFrame(rows).sort_values("year").reset_index(drop=True)


def yg1_raw_materials() -> dict[int, float]:
    """DART 사업보고서 XML 에서 '재고자산 현황' 표의 원재료(보통 천원 단위) 추출.

    하나의 사업보고서에 3개년 비교가 있으므로 각 연도에서 자기 연도(thstrm) 값만 사용.
    반환값 단위: 원 (KRW).
    """
    import dart_report, re
    out = {}
    NUM = r"\(?-?[\d,]+\)?"
    for y in YEARS:
        if y < 2016:
            continue
        try:
            sections = dart_report.get_report_sections(YG1_CORP, y)
            rcept = sections.get("_rcept_no") if sections else None
            if not rcept:
                # 후보 rcept 직접 시도
                cands = dart_report.list_rcept_candidates(YG1_CORP, y)
                rcept = cands[0] if cands else None
            xml = dart_report.fetch_document_xml(rcept) if rcept else None
        except Exception:
            xml = None
        if not xml:
            continue
        text = dart_report._strip_tags(xml)
        # "원재료  NUM  NUM  NUM  저장품" 패턴 (당기/전기/전전기 3개년)
        # NUM 사이 공백·줄바꿈 다양 → 비탐욕 일반화
        m = re.search(
            rf"원재료\s+({NUM})\s+({NUM})\s+({NUM})\s+저장품",
            text,
        )
        if not m:
            # 2개년만 표시 (오래된 보고서 일부)
            m = re.search(rf"원재료\s+({NUM})\s+({NUM})\s+저장품", text)
        if not m:
            # 마지막 fallback: "원재료 NUM 저장품"
            m = re.search(rf"원재료\s+({NUM})\s+저장품", text)
        if not m:
            continue

        def to_int(s):
            s = s.replace(",", "").replace("(", "-").replace(")", "")
            try: return int(s)
            except: return None
        cur = to_int(m.group(1))
        if cur is None:
            continue
        # 단위 판별: YG-1 원재료는 통상 500~750억 → 천원이면 5e7~7.5e7, 원이면 5e10~7.5e10
        if cur >= 1e10:        # 원
            v_won = cur
        elif cur >= 1e7:       # 천원
            v_won = cur * 1_000
        elif cur >= 1e4:       # 백만원
            v_won = cur * 1_000_000
        else:
            v_won = None
        if v_won:
            out[y] = v_won
        # 전기·전전기도 같이 채움 (있으면)
        if len(m.groups()) >= 2 and (y - 1) not in out:
            prv = to_int(m.group(2))
            if prv:
                if prv >= 1e10: w = prv
                elif prv >= 1e7: w = prv * 1_000
                elif prv >= 1e4: w = prv * 1_000_000
                else: w = None
                if w: out[y - 1] = w
        if len(m.groups()) >= 3 and (y - 2) not in out:
            pp = to_int(m.group(3))
            if pp:
                if pp >= 1e10: w = pp
                elif pp >= 1e7: w = pp * 1_000
                elif pp >= 1e4: w = pp * 1_000_000
                else: w = None
                if w: out[y - 2] = w
    return out


# ── KMT SEC XBRL ──────────────────────────────────────────────────────────────
HDR = {"User-Agent": "StockIQ Research imengineerhwang@gmail.com"}


def _kmt_fact(concept: str) -> list[dict]:
    url = f"https://data.sec.gov/api/xbrl/companyconcept/CIK0000055242/us-gaap/{concept}.json"
    r = requests.get(url, headers=HDR, timeout=30)
    if r.status_code != 200:
        return []
    j = r.json()
    rows = []
    for unit, vals in j.get("units", {}).items():
        for v in vals:
            v["_unit"] = unit
            rows.append(v)
    return rows


def _kmt_annual_value(rows: list[dict], year: int, fp_filter=("FY",)):
    """KMT 회계연도 종료일 = year년 6/30 인 12개월짜리 record 매칭 (10-K).

    fy=2017 record는 2016/7-2017/6, 2015/7-2016/6, 2014/7-2015/6 이 모두 들어있어서
    fy 필터만으로는 부정확. end='YYYY-06-30' 으로 정확 매칭.
    """
    target_end = f"{year}-06-30"
    cands = [r for r in rows
             if r.get("end") == target_end
             and r.get("form") == "10-K"
             and r.get("fp") in fp_filter]
    # 12개월짜리만 (start이 있다면 검증)
    def is_12m(r):
        s, e = r.get("start"), r.get("end")
        if not s or not e: return True  # BS는 start 없음
        try:
            from datetime import date
            sd = date.fromisoformat(s); ed = date.fromisoformat(e)
            months = (ed.year - sd.year) * 12 + (ed.month - sd.month)
            return abs(months - 12) <= 1
        except:
            return True
    cands = [r for r in cands if is_12m(r)]
    if not cands:
        return None
    cands.sort(key=lambda x: x.get("filed", ""), reverse=True)
    return cands[0]["val"]


def _kmt_balance_sheet_value(rows: list[dict], year: int):
    """KMT 회계연도말(6/30) 기준 BS 값."""
    target = f"{year}-06-30"
    cands = [r for r in rows if r.get("end") == target or r.get("end","").startswith(f"{year}-06")]
    if not cands:
        return None
    # 10-K 우선
    cands.sort(key=lambda x: (x.get("form") != "10-K", -int(x.get("filed","0").replace("-",""))))
    return cands[0]["val"]


def kmt_metrics() -> pd.DataFrame:
    sales = _kmt_fact("SalesRevenueNet") + _kmt_fact("Revenues")
    op = _kmt_fact("OperatingIncomeLoss")
    ni = _kmt_fact("NetIncomeLoss")
    inv = _kmt_fact("InventoryNet")
    raw = _kmt_fact("InventoryRawMaterialsAndSupplies")
    eq = _kmt_fact("StockholdersEquity")
    rows = []
    for y in YEARS:
        rev = _kmt_annual_value(sales, y)
        opi = _kmt_annual_value(op, y)
        n = _kmt_annual_value(ni, y)
        i = _kmt_balance_sheet_value(inv, y)
        r_ = _kmt_balance_sheet_value(raw, y)
        e = _kmt_balance_sheet_value(eq, y)
        rows.append({"year": y, "revenue_usd": rev, "op_income_usd": opi,
                     "net_income_usd": n, "equity_usd": e,
                     "inventory_usd": i, "raw_materials_usd": r_})
    return pd.DataFrame(rows)


# ── OSG IR PDF (이미 추출한 값을 하드코딩) ──────────────────────────────────
# 단위: 백만엔 (Million yen)
OSG_DATA = {
    # year(FY 기준, OSG FY는 12/1 시작 ~ 11/30 종료): (revenue, op_income, net_income, equity, inv_total, raw_materials)
    # equity = 자본 (Reference Equity = 모회사 귀속자본) / Total net assets 중 모회사귀속만 사용
    # inventory = Merchandise + WIP + Raw materials (consolidated balance sheet)
    # raw_materials = "Raw materials and supplies"
    2015: (111_917, 21_597, 12_518, 102_566, 30_672,  6_083),
    2016: (105_561, 18_246, 10_134,  92_216, 31_691,  5_834),
    2017: (120_198, 19_137, 13_993, 115_810, 34_849,  6_217),
    2018: (131_368, 22_520, 14_710, 125_332, 38_801,  7_121),
    2019: (126_964, 19_554, 13_686, 129_078, 43_878,  7_658),
    2020: (104_388,  8_396,  5_639, 129_338, 42_024,  7_321),
    2021: (126_156, 16_105, 10_989, 143_811, 42_837,  7_718),
    2022: (142_525, 21_898, 16_534, 164_659, 52_025, 10_146),
    2023: (147_703, 19_800, 14_307, 181_561, 56_493, 10_420),
    2024: (155_517, 18_868, 13_439, 166_633, 56_001,  9_904),
    2025: (160_619, 20_330, 14_334, 180_811, 59_489, 10_765),
}


def osg_metrics() -> pd.DataFrame:
    rows = []
    for y, (rev, opi, ni, eq, inv, raw) in OSG_DATA.items():
        rows.append({"year": y, "revenue_jpy_m": rev, "op_income_jpy_m": opi,
                     "net_income_jpy_m": ni, "equity_jpy_m": eq,
                     "inventory_jpy_m": inv, "raw_materials_jpy_m": raw})
    return pd.DataFrame(rows)


# ── 메인 빌더 ──────────────────────────────────────────────────────────────
def build_compare() -> pd.DataFrame:
    fx_usd, fx_jpy = yearly_avg_fx()
    yg = yg1_metrics()
    yg_raw = yg1_raw_materials()
    km = kmt_metrics()
    og = osg_metrics()

    # 종가
    yg_close = yearly_close("019210.KS")
    km_close = yearly_close("KMT")
    og_close = yearly_close("6136.T")

    out_rows = []
    for y in YEARS:
        usd_krw = fx_usd.get(y)
        jpy_krw = fx_jpy.get(y)
        # YG-1
        yg_r = yg[yg["year"] == y]
        if not yg_r.empty:
            r = yg_r.iloc[0]
            shares_yg = YG1_SHARES_Q.get(f"{y}-12-31")
            close_yg = yg_close.get(y)
            mcap = (close_yg or 0) * (shares_yg or 0) / 1e8 if close_yg and shares_yg else None
            out_rows.append({
                "연도": y, "회사": "YG-1",
                "매출액(억원)": (r.revenue_krw or 0) / 1e8 if r.revenue_krw else None,
                "영업이익(억원)": (r.op_income_krw or 0) / 1e8 if r.op_income_krw else None,
                "당기순이익(억원)": (r.net_income_krw or 0) / 1e8 if r.net_income_krw else None,
                "영업이익률(%)": (r.op_income_krw / r.revenue_krw * 100) if r.revenue_krw and r.op_income_krw else None,
                "재고자산(억원)": (r.inventory_krw or 0) / 1e8 if r.inventory_krw else None,
                "원재료(억원)": (yg_raw.get(y) or 0) / 1e8 if yg_raw.get(y) else None,
                "시가총액(억원)": mcap,
                "PER": (mcap / ((r.net_income_krw or 0) / 1e8)) if mcap and r.net_income_krw else None,
                "PBR": (mcap / ((r.equity_krw or 0) / 1e8)) if mcap and r.equity_krw else None,
            })
        # KMT
        km_r = km[km["year"] == y]
        if not km_r.empty and usd_krw:
            r = km_r.iloc[0]
            shares = KMT_SHARES_FY_END.get(y)
            close = km_close.get(y)
            mcap = close * shares * usd_krw / 1e8 if close and shares else None
            ni_krw = (r.net_income_usd or 0) * usd_krw / 1e8 if r.net_income_usd else None
            eq_krw = (r.equity_usd or 0) * usd_krw / 1e8 if r.equity_usd else None
            out_rows.append({
                "연도": y, "회사": "KMT",
                "매출액(억원)": (r.revenue_usd or 0) * usd_krw / 1e8 if r.revenue_usd else None,
                "영업이익(억원)": (r.op_income_usd or 0) * usd_krw / 1e8 if r.op_income_usd else None,
                "당기순이익(억원)": ni_krw,
                "영업이익률(%)": (r.op_income_usd / r.revenue_usd * 100) if r.revenue_usd and r.op_income_usd else None,
                "재고자산(억원)": (r.inventory_usd or 0) * usd_krw / 1e8 if r.inventory_usd else None,
                "원재료(억원)": (r.raw_materials_usd or 0) * usd_krw / 1e8 if r.raw_materials_usd else None,
                "시가총액(억원)": mcap,
                "PER": (mcap / ni_krw) if mcap and ni_krw else None,
                "PBR": (mcap / eq_krw) if mcap and eq_krw else None,
            })
        # OSG
        og_r = og[og["year"] == y]
        if not og_r.empty and jpy_krw:
            r = og_r.iloc[0]
            shares = OSG_SHARES_FY_END.get(y)
            close = og_close.get(y)
            # close ¥, shares주, jpy_krw 환율 → 시총 KRW
            mcap = close * shares * jpy_krw / 1e8 if close and shares else None
            # 매출액 등은 백만엔 → 엔 → 원 / 1e8 = 억원
            rev_krw = r.revenue_jpy_m * 1e6 * jpy_krw / 1e8 if r.revenue_jpy_m else None
            opi_krw = r.op_income_jpy_m * 1e6 * jpy_krw / 1e8 if r.op_income_jpy_m else None
            ni_krw = r.net_income_jpy_m * 1e6 * jpy_krw / 1e8 if r.net_income_jpy_m else None
            inv_krw = r.inventory_jpy_m * 1e6 * jpy_krw / 1e8 if r.inventory_jpy_m else None
            raw_krw = r.raw_materials_jpy_m * 1e6 * jpy_krw / 1e8 if r.raw_materials_jpy_m else None
            eq_krw = r.equity_jpy_m * 1e6 * jpy_krw / 1e8 if r.equity_jpy_m else None
            out_rows.append({
                "연도": y, "회사": "OSG",
                "매출액(억원)": rev_krw,
                "영업이익(억원)": opi_krw,
                "당기순이익(억원)": ni_krw,
                "영업이익률(%)": (r.op_income_jpy_m / r.revenue_jpy_m * 100) if r.revenue_jpy_m and r.op_income_jpy_m else None,
                "재고자산(억원)": inv_krw,
                "원재료(억원)": raw_krw,
                "시가총액(억원)": mcap,
                "PER": (mcap / ni_krw) if mcap and ni_krw else None,
                "PBR": (mcap / eq_krw) if mcap and eq_krw else None,
            })

    df = pd.DataFrame(out_rows)
    return df


if __name__ == "__main__":
    df = build_compare()
    out_csv = "data/companies/와이지-원/peer_compare.csv"
    df.to_csv(out_csv, index=False, encoding="utf-8-sig")
    print(f"saved: {out_csv}, rows={len(df)}")
    # 회사별 피벗 출력
    for col in ["매출액(억원)", "영업이익(억원)", "당기순이익(억원)", "영업이익률(%)",
                "재고자산(억원)", "원재료(억원)", "시가총액(억원)", "PER", "PBR"]:
        pivot = df.pivot(index="연도", columns="회사", values=col).round(1)
        print(f"\n=== {col} ===")
        print(pivot.to_string())
