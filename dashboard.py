import streamlit as st
import pandas as pd
import os
from datetime import datetime
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import anthropic
from dotenv import load_dotenv
import dart_report
import machine_tool_crawler as mt_crawler
import tungsten_crawler as w_crawler

load_dotenv()

# Streamlit Cloud Secrets → 환경변수 주입 (로컬은 .env 우선, Cloud는 Secrets 탭 사용)
for _k in ("DART_API_KEY", "ANTHROPIC_API_KEY"):
    if _k not in os.environ:
        try:
            os.environ[_k] = st.secrets[_k]
        except Exception:
            pass

ANALYSIS_DIR = "data/analysis"
os.makedirs(ANALYSIS_DIR, exist_ok=True)

# ── 비밀번호 게이트 ────────────────────────────────────────────────────────────
try:
    _PASSWORD = st.secrets.get("PASSWORD", "")
except Exception:
    _PASSWORD = ""  # 로컬: secrets 파일 없으면 비밀번호 없이 통과

if "authenticated" not in st.session_state:
    st.session_state.authenticated = not _PASSWORD  # 비밀번호 미설정 시 자동 통과

if not st.session_state.authenticated:
    st.set_page_config(page_title="StockIQ", page_icon="📈", layout="centered")
    st.title("📈 StockIQ")
    pw = st.text_input("비밀번호를 입력하세요", type="password", placeholder="Password")
    if st.button("로그인", use_container_width=True) or pw:
        if pw == _PASSWORD:
            st.session_state.authenticated = True
            st.rerun()
        elif pw:
            st.error("비밀번호가 틀렸습니다.")
    st.stop()

CORP_CODE = "00139719"


@st.cache_resource
def _ai_client():
    return anthropic.Anthropic()


ANALYSIS_SYSTEM = """당신은 한국 상장사 재무 애널리스트입니다. 주어진 두 해치 연간 재무제표와 사업보고서 원문 발췌(사업의 내용, 이사의 경영진단 및 분석의견)를 분석하여, 영업이익 변동의 핵심 원인을 **회사가 직접 밝힌 경영 맥락 중심**으로 설명하세요.

출력 형식 (마크다운):

**영업이익 ±X% 변동 원인 (회사 경영 맥락)**

회사가 사업보고서에서 직접 언급한 시장 상황, 전략 변화, 원가 구조 변동, 수요/공급 이슈 등을 2~4문단으로 서술하세요. 핵심 문장은 **원문 그대로 인용**하여 인용문 블록으로 표시:

> 회사 원문: "실제 원문 문장 그대로"

**주요 수치 변동**
- 계정명 A억 → B억 (±X.X%) : 한 줄 의미

**[근거]**
- 인용 출처: DART 사업보고서 (YYYY년) <섹션명>
- 재무 출처: DART 사업보고서 (YYYY년) <재무제표명> - <항목명>

규칙 (엄수):
- 원문 인용은 반드시 제공된 발췌 텍스트 안에 **실제로 존재하는 문장만** 사용하세요. 없으면 인용하지 말고 수치 분석만 제공.
- 제공되지 않은 뉴스·보도자료·추측은 포함 금지.
- 금액은 억원 단위, 천단위 콤마, 소수점 1자리 변동률.
- 한국어로 작성.
- **마크다운 헤딩(`#`, `##`, `###`) 사용 금지**. 섹션 제목은 반드시 `**굵은 글씨**`만 사용하세요. 위 출력 형식에 명시된 3개 제목(`**영업이익 ... 원인 ...**`, `**주요 수치 변동**`, `**[근거]**`) 외에 별도 소제목을 만들지 마세요."""


def compute_yoy_changes(op_by_year, threshold=30):
    """YoY 영업이익 변동률이 threshold% 이상인 (연도, 직전연도, 변동률) 리스트."""
    years = sorted(op_by_year.keys())
    out = []
    for i in range(1, len(years)):
        prior, cur = years[i - 1], years[i]
        p_op, c_op = op_by_year[prior], op_by_year[cur]
        if p_op == 0:
            continue
        pct = (c_op - p_op) / abs(p_op) * 100
        if abs(pct) >= threshold:
            out.append((cur, prior, pct))
    return out


def build_year_context(fin_dir, year, prior_year):
    parts = []
    for fname, label in [
        ("연간_손익계산서.csv", "손익계산서"),
        ("연간_재무상태표.csv", "재무상태표"),
        ("연간_현금흐름표.csv", "현금흐름표"),
    ]:
        path = os.path.join(fin_dir, fname)
        if not os.path.exists(path):
            continue
        df = pd.read_csv(path)
        label_col = df.columns[0]
        cur_col, prior_col = f"{year}/12", f"{prior_year}/12"
        if cur_col not in df.columns or prior_col not in df.columns:
            continue
        parts.append(f"\n[{label}] {prior_year} → {year} (단위: 억원)")
        for _, row in df.iterrows():
            parts.append(f"  {row[label_col]}: {row[prior_col]} → {row[cur_col]}")
    return "\n".join(parts)


def _build_report_excerpt(year):
    sec = dart_report.get_report_sections(CORP_CODE, str(year))
    if not sec:
        return f"\n(※ {year}년 사업보고서 원문을 가져오지 못함)"
    rcept = sec.get("_rcept_no", "N/A")
    out = [f"\n=== {year}년 사업보고서 (rcept_no: {rcept}) ==="]
    for key in ("사업의 내용", "이사의 경영진단 및 분석의견"):
        if key in sec:
            out.append(f"\n## [{key}]\n{sec[key]}")
    return "\n".join(out)


def load_or_generate_analysis(year, prior_year, pct, fin_dir):
    path = os.path.join(ANALYSIS_DIR, f"op_change_{year}.md")
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            return f.read()
    fin_ctx = build_year_context(fin_dir, year, prior_year)
    report_ctx = _build_report_excerpt(year) + _build_report_excerpt(prior_year)
    user_msg = (
        f"{year}년 영업이익이 전년({prior_year}) 대비 {pct:+.1f}% 변동했습니다.\n\n"
        f"# 재무 데이터 (연간, 억원)\n{fin_ctx}\n\n"
        f"# 사업보고서 원문 발췌\n{report_ctx}"
    )
    msg = _ai_client().messages.create(
        model="claude-sonnet-4-5",
        max_tokens=2500,
        system=ANALYSIS_SYSTEM,
        messages=[{"role": "user", "content": user_msg}],
    )
    text = msg.content[0].text.strip()
    with open(path, "w", encoding="utf-8") as f:
        f.write(text)
    return text


st.set_page_config(page_title="StockIQ", page_icon="📈", layout="wide")

# ── 회사 선택 (사이드바) ──────────────────────────────────────────────────────
_companies_path = "companies.csv"
if os.path.exists(_companies_path):
    _co_df = pd.read_csv(_companies_path)
    _co_names = _co_df["name"].tolist()
else:
    _co_df = pd.DataFrame(columns=["name", "stock_code", "corp_code", "sector", "description"])
    _co_names = []

with st.sidebar:
    st.markdown("## 📊 분석 회사")
    if _co_names:
        selected_company = st.selectbox("회사 선택", _co_names, label_visibility="collapsed")
        _row = _co_df[_co_df["name"] == selected_company].iloc[0]
        CORP_CODE   = str(_row["corp_code"]).zfill(8)
        st.caption(f"**{_row['sector']}** · {_row['description']}")
    else:
        selected_company = "미등록"
        CORP_CODE = ""
        st.warning("companies.csv가 없거나 비어 있어요.")
    st.divider()
    st.caption("새 회사 추가: companies.csv에 행 추가 후 재실행")

# ── 회사별 경로 ───────────────────────────────────────────────────────────────
company_dir  = f"data/companies/{selected_company}"
fin_dir      = f"{company_dir}/financials"
briefing_dir = f"{company_dir}/briefings"
telegram_dir = f"{company_dir}/telegram"
ANALYSIS_DIR = f"{company_dir}/analysis"
os.makedirs(ANALYSIS_DIR, exist_ok=True)

st.title("📈 StockIQ 투자 리서치 대시보드")
st.caption(f"**{selected_company}** · 마지막 업데이트: {datetime.now().strftime('%Y-%m-%d %H:%M')}")

# 시세 데이터
st.subheader("보유 관심 종목")
if os.path.exists("data/stocks.csv"):
    df = pd.read_csv("data/stocks.csv")
    col1, col2, col3 = st.columns(3)
    for i, row in df.iterrows():
        with [col1, col2, col3][i % 3]:
            st.metric(
                label=row["종목명"],
                value=row["현재가"] + "원",
                delta=f"PER {row['PER']}배"
            )
    st.dataframe(df, width="stretch")
else:
    st.warning("data/stocks.csv 파일이 없어요. save_data.py 먼저 실행해 주세요.")

st.divider()

# 경쟁사 주가·시가총액 비교 (YG-1 vs OSG vs KMT)
st.subheader("🏁 경쟁사 비교 — 연봉 주가 & 시가총액")

PEERS_CFG = [
    {"ticker": "019210.KS", "name": "YG-1", "currency": "KRW", "fx": "KRW=X", "color": "#d62728", "axis": "y3"},
    {"ticker": "6136.T",    "name": "OSG",  "currency": "JPY", "fx": "JPY=X", "color": "#1f77b4", "axis": "y"},
    {"ticker": "KMT",       "name": "KMT",  "currency": "USD", "fx": None,    "color": "#2ca02c", "axis": "y2"},
]


# ── 공식 소스 기반 발행주식수 시계열 (분기·연말 기준) ───────────────────────
# YG-1: DART stockTotqySttus API — reprt_code 11013/11012/11014/11011 (Q1~Q4)
#   합계 istc_totqy (보통주+우선주, 자사주 제외 후 유통주식)
YG1_SHARES_Q = {
    "2016-03-31": 28_615_520, "2016-06-30": 28_615_520, "2016-09-30": 28_615_520, "2016-12-31": 29_457_513,
    "2017-03-31": 29_519_632, "2017-06-30": 29_640_077, "2017-09-30": 29_842_668, "2017-12-31": 31_448_331,
    "2018-03-31": 31_449_158, "2018-06-30": 31_449_158, "2018-09-30": 31_449_158, "2018-12-31": 31_449_158,
    "2019-03-31": 31_449_158, "2019-06-30": 31_449_158, "2019-09-30": 31_449_158, "2019-12-31": 31_449_239,
    "2020-03-31": 31_449_239, "2020-06-30": 31_449_239, "2020-09-30": 31_449_239, "2020-12-31": 34_193_728,
    "2021-03-31": 34_193_728, "2021-06-30": 34_193_728, "2021-09-30": 34_193_728, "2021-12-31": 34_193_728,
    "2022-03-31": 34_193_728, "2022-06-30": 34_193_728, "2022-09-30": 34_193_728, "2022-12-31": 34_193_728,
    "2023-03-31": 34_193_728, "2023-06-30": 34_193_728, "2023-09-30": 34_193_728, "2023-12-31": 34_193_728,
    "2024-03-31": 37_193_728, "2024-06-30": 37_193_728, "2024-09-30": 37_193_728, "2024-12-31": 37_193_728,
    "2025-03-31": 37_193_728, "2025-06-30": 37_193_728, "2025-09-30": 37_193_728, "2025-12-31": 37_193_728,
}
# KMT: SEC EntityCommonStockSharesOutstanding — 10-Q/10-K 커버일자 기준
KMT_SHARES_Q = {
    "2016-01-29": 79_672_229, "2016-04-29": 79_689_781, "2016-07-29": 79_700_981, "2016-10-31": 79_933_935,
    "2017-01-31": 80_193_977, "2017-04-28": 80_554_198, "2017-07-31": 80_672_938, "2017-10-31": 81_048_153,
    "2018-01-31": 81_573_415, "2018-04-30": 81_628_262, "2018-07-31": 81_647_556, "2018-10-31": 82_102_785,
    "2019-01-31": 82_233_615, "2019-04-30": 82_390_406, "2019-07-31": 82_462_011, "2019-10-31": 82_856_908,
    "2020-01-31": 82_898_074, "2020-04-30": 82_913_959, "2020-07-31": 82_927_634, "2020-10-30": 83_276_032,
    "2021-01-29": 83_533_003, "2021-04-30": 83_598_649, "2021-07-31": 83_615_430, "2021-10-29": 83_645_026,
    "2022-01-31": 83_090_710, "2022-04-29": 82_638_419, "2022-07-29": 81_338_696, "2022-10-31": 80_576_387,
    "2023-01-31": 80_527_022, "2023-04-28": 80_275_367, "2023-07-31": 79_711_220, "2023-10-31": 79_603_305,
    "2024-01-31": 79_269_782, "2024-04-30": 78_665_910, "2024-07-31": 77_900_791, "2024-10-31": 77_725_882,
    "2025-01-31": 77_360_327, "2025-04-30": 76_233_564, "2025-07-31": 76_021_577, "2025-10-31": 76_093_136,
    "2026-01-31": 76_198_792,
}
# OSG: IR FY 결산단신 PDF, 회계연도말 Nov 30 보통주 유통주식수 (발행-자사주)
#   분기 데이터 대신 연 1회 값 사용 → 월별 주가와 결합 시 step function forward-fill
OSG_SHARES_Q = {
    "2015-11-30": 95_046_256, "2016-11-30": 90_025_147, "2017-11-30": 97_184_575,
    "2018-11-30": 97_970_188, "2019-11-30": 97_191_831, "2020-11-30": 97_450_361,
    "2021-11-30": 97_668_266, "2022-11-30": 95_668_994, "2023-11-30": 95_944_700,
    "2024-11-30": 84_921_343, "2025-11-30": 82_150_384,
}


def _shares_series(mapping: dict[str, int]) -> pd.Series:
    """문자열 날짜 dict → 정렬된 pd.Series (index=Timestamp)."""
    s = pd.Series({pd.Timestamp(k): v for k, v in mapping.items()}).sort_index()
    return s


@st.cache_data(ttl=60 * 60 * 6, show_spinner="yfinance에서 주가·환율 수집중…")
def _peer_dataset(start: str = "2017-01-01"):
    import yfinance as yf

    ohlc, monthly, fx_monthly = {}, {}, {}
    for p in PEERS_CFG:
        tk = yf.Ticker(p["ticker"])
        d = tk.history(start=start, interval="1d", auto_adjust=False)
        if not d.empty:
            d.index = d.index.tz_localize(None)
            y = d.resample("YE").agg(
                {"Open": "first", "High": "max", "Low": "min", "Close": "last"}
            ).dropna()
            y.index = y.index.year
            m = d["Close"].resample("ME").last().dropna()
        else:
            y, m = pd.DataFrame(), pd.Series(dtype=float)
        ohlc[p["name"]] = y
        monthly[p["name"]] = m

        if p["fx"]:
            fxd = yf.Ticker(p["fx"]).history(start=start, interval="1d", auto_adjust=False)
            fxd.index = fxd.index.tz_localize(None)
            fx_monthly[p["name"]] = fxd["Close"].resample("ME").last().dropna()
        else:
            fx_monthly[p["name"]] = None
    return ohlc, monthly, fx_monthly


try:
    peer_ohlc, peer_monthly, peer_fx = _peer_dataset()

    # 1) 주가 추이 — 3사 월말 종가, 각자 별도 y축 (OSG 좌, YG-1 우·보조, KMT 우-외곽)
    AXIS_MAP = {"OSG": "y", "YG-1": "y2", "KMT": "y3"}
    fig_price = go.Figure()
    for p in PEERS_CFG:
        s = peer_monthly[p["name"]]
        if s.empty:
            continue
        is_yg1 = p["name"] == "YG-1"
        fig_price.add_trace(go.Scatter(
            x=s.index, y=s.values,
            name=f"{p['name']} ({p['currency']}){' · 보조축' if is_yg1 else ''}",
            mode="lines",
            line=dict(color=p["color"], width=2, dash="dot" if is_yg1 else "solid"),
            yaxis=AXIS_MAP[p["name"]],
        ))
    fig_price.update_layout(
        title="2017–2026 주가 추이 (월말 종가) — YG-1 은 보조축",
        xaxis=dict(title="연도", domain=[0.0, 0.92]),
        yaxis=dict(
            title=dict(text="OSG (JPY)", font=dict(color="#1f77b4")),
            tickfont=dict(color="#1f77b4"),
            side="left", tickformat=",.0f",
        ),
        yaxis2=dict(
            title=dict(text="YG-1 (KRW)", font=dict(color="#d62728")),
            tickfont=dict(color="#d62728"),
            overlaying="y", side="right",
            tickformat=",.0f",
        ),
        yaxis3=dict(
            title=dict(text="KMT (USD)", font=dict(color="#2ca02c")),
            tickfont=dict(color="#2ca02c"),
            overlaying="y", side="right",
            anchor="free", position=1.0,
            tickformat=",.0f",
        ),
        height=520, margin=dict(l=70, r=110, t=60, b=60),
        template="plotly_white",
        legend=dict(orientation="h", y=-0.2),
        hovermode="x unified",
    )
    st.plotly_chart(fig_price, use_container_width=True)

    # 2) 시가총액 월별 선그래프 — 분기별 공식 주식수 × 월말 종가 × 월말 FX → 억원
    shares_series_map = {
        "YG-1": _shares_series(YG1_SHARES_Q),
        "OSG":  _shares_series(OSG_SHARES_Q),
        "KMT":  _shares_series(KMT_SHARES_Q),
    }
    krw_fx_m = peer_fx["YG-1"]   # USD→KRW 월말 종가
    jpy_fx_m = peer_fx["OSG"]    # USD→JPY 월말 종가

    def _to_krw_eok_m(name: str, dt: pd.Timestamp, close: float, shares: float) -> float | None:
        local = close * shares
        if name == "YG-1":
            return local / 1e8
        if name == "OSG":
            if krw_fx_m is None or jpy_fx_m is None:
                return None
            k = krw_fx_m.asof(dt); j = jpy_fx_m.asof(dt)
            if pd.isna(k) or pd.isna(j) or not j:
                return None
            return local * (k / j) / 1e8
        # KMT (USD → KRW)
        if krw_fx_m is None:
            return None
        k = krw_fx_m.asof(dt)
        if pd.isna(k):
            return None
        return local * k / 1e8

    mcap_rows = []
    for p in PEERS_CFG:
        m = peer_monthly[p["name"]]
        sh_series = shares_series_map[p["name"]]
        if m.empty or sh_series.empty:
            continue
        for dt, close in m.items():
            sh_val = sh_series.asof(dt)  # step function: 해당 시점 이전 최신값
            if pd.isna(sh_val):
                continue
            v = _to_krw_eok_m(p["name"], dt, float(close), float(sh_val))
            if v is None:
                continue
            mcap_rows.append({"date": dt, "company": p["name"], "mcap_eok": v})

    mcap_df = pd.DataFrame(mcap_rows)
    if not mcap_df.empty:
        fig_mcap = go.Figure()
        for p in PEERS_CFG:
            sub = mcap_df[mcap_df["company"] == p["name"]].sort_values("date")
            if sub.empty:
                continue
            is_yg1 = p["name"] == "YG-1"
            fig_mcap.add_trace(go.Scatter(
                x=sub["date"], y=sub["mcap_eok"],
                name=f"{p['name']}{' (보조축)' if is_yg1 else ''}",
                mode="lines",
                line=dict(color=p["color"], width=2, dash="dot" if is_yg1 else "solid"),
                yaxis="y2" if is_yg1 else "y",
                hovertemplate=f"{p['name']} · %{{x|%Y-%m}}<br>%{{y:,.0f}} 억원<extra></extra>",
            ))
        fig_mcap.update_layout(
            title="시가총액 추이 (월말, 단위: 억원) — YG-1 보조축",
            xaxis=dict(title="연도"),
            yaxis=dict(title="OSG / KMT (억원)", side="left", tickformat=",.0f"),
            yaxis2=dict(
                title=dict(text="YG-1 (억원)", font=dict(color="#d62728")),
                tickfont=dict(color="#d62728"),
                overlaying="y", side="right", tickformat=",.0f",
            ),
            height=500, template="plotly_white",
            legend=dict(orientation="h", y=-0.15),
            hovermode="x unified",
        )
        st.plotly_chart(fig_mcap, use_container_width=True)

        with st.expander("원데이터 보기 (월말 시가총액, 억원)"):
            pivot = mcap_df.pivot(index="date", columns="company", values="mcap_eok").round(0)
            pivot.index = pivot.index.strftime("%Y-%m")
            st.dataframe(pivot.astype("Int64"), width="stretch")
    else:
        st.info("시가총액 데이터를 만들 수 없습니다 (발행주식수/환율 미확보).")
except Exception as _peer_err:
    st.warning(f"경쟁사 차트 로드 실패: {_peer_err}")

# 경쟁사 재무·밸류에이션 비교표
st.subheader("📊 경쟁사 재무·밸류에이션 비교표 (2015–2025, 단위: 억원)")
_pc_path = "data/companies/와이지-원/peer_compare.csv"
if os.path.exists(_pc_path):
    pc_df = pd.read_csv(_pc_path)
    st.caption("YG-1: DART 사업보고서 · KMT: SEC XBRL · OSG: IR 결산단신 PDF · "
               "외화 → 연평균 환율(yfinance KRW=X, JPY=X)로 원화 환산")

    metrics = [
        "매출액(억원)", "영업이익(억원)", "당기순이익(억원)", "영업이익률(%)",
        "재고자산(억원)", "원재료(억원)", "시가총액(억원)", "PER", "PBR",
    ]
    # 헤더 약식 (억원 표기 생략, 섹션 캡션에 단위 명시)
    SHORT = {
        "매출액(억원)": "매출액", "영업이익(억원)": "영업이익", "당기순이익(억원)": "순이익",
        "영업이익률(%)": "OPM(%)", "재고자산(억원)": "재고", "원재료(억원)": "원재료",
        "시가총액(억원)": "시총", "PER": "PER", "PBR": "PBR",
    }

    def _fmt_company_table(df_company: pd.DataFrame):
        t = df_company[["연도"] + metrics].copy().rename(columns=SHORT)
        def cell(v, m):
            if pd.isna(v): return "-"
            if m in ("OPM(%)", "PER", "PBR"):
                return f"{v:,.1f}"
            return f"{v:,.0f}"
        for c in t.columns:
            if c == "연도":
                t[c] = t[c].astype(int).astype(str)
            else:
                t[c] = t[c].apply(lambda v, c=c: cell(v, c))
        # 연도를 컬럼으로, 지표를 행으로 → 전치
        return t.set_index("연도").T.rename_axis(index="지표", columns=None)

    for comp, emoji in [("YG-1", "🇰🇷"), ("OSG", "🇯🇵"), ("KMT", "🇺🇸")]:
        sub = pc_df[pc_df["회사"] == comp].sort_values("연도")
        if sub.empty:
            continue
        st.markdown(f"#### {emoji} {comp}")
        st.table(_fmt_company_table(sub))

    with st.expander("🔧 데이터 재생성 (peer_compare.py 실행)"):
        st.code("python peer_compare.py", language="bash")
        st.caption("DART/SEC/PDF 원천에서 다시 추출하려면 위 명령 실행 후 페이지 새로고침")
else:
    st.info("`peer_compare.py` 를 먼저 실행해 주세요. → 결과: `data/companies/와이지-원/peer_compare.csv`")

st.divider()

# 공시 데이터
st.subheader("최근 공시")
if os.path.exists("data/disclosures.csv"):
    df_disc = pd.read_csv("data/disclosures.csv")
    # 선택한 회사 공시만 필터
    if "종목명" in df_disc.columns:
        df_disc = df_disc[df_disc["종목명"] == selected_company]
    st.dataframe(df_disc, width="stretch")
else:
    st.warning("data/disclosures.csv 파일이 없어요.")

st.divider()

# AI 브리핑
st.subheader("AI 브리핑")
if os.path.exists(briefing_dir):
    files = sorted(os.listdir(briefing_dir), reverse=True)
    if files:
        for f in files:
            with open(os.path.join(briefing_dir, f), "r", encoding="utf-8") as file:
                content = file.read()
            with st.expander(f"📄 {f.replace('.txt', '')}"):
                st.markdown(content)
    else:
        st.warning("브리핑 파일이 없어요. briefing.py 먼저 실행해 주세요.")
else:
    st.warning("브리핑 폴더가 없어요.")

st.divider()

# 텔레그램 정보
st.subheader("📨 텔레그램 정보")


if os.path.exists(telegram_dir):
    summary_files = sorted(
        f for f in os.listdir(telegram_dir)
        if f.endswith(".md") and "검색결과" not in f
    )
    if not summary_files:
        st.warning("data/telegram 폴더에 요약 마크다운 파일이 없어요.")
    for fname in summary_files:
        with open(os.path.join(telegram_dir, fname), "r", encoding="utf-8") as f:
            content = f.read()
        title = fname.replace(".md", "").replace("_", " ")
        with st.expander(f"📊 {title}", expanded=False):
            st.markdown(content)
else:
    st.warning("텔레그램 폴더가 없어요.")

st.divider()

# 일본 공작기계 수주 (JMTBA)
st.subheader("🇯🇵 일본 공작기계 수주 (JMTBA)")

try:
    mt_df = mt_crawler.fetch_series()
except Exception as e:
    mt_df = None
    st.warning(f"공작기계 수주 데이터 로딩 실패: {e}")

if mt_df is not None and not mt_df.empty:
    mt_src = mt_df.attrs.get("source", "JMTBA")
    mt_unit = mt_df.attrs.get("unit", "JPY Million")
    mt_fetched = mt_df.attrs.get("fetched_at", "")

    # 최신 지표 요약
    latest = mt_df.iloc[-1]
    prev = mt_df.iloc[-2]
    latest_yoy = latest["yoy_pct"]
    prev_yoy = prev["yoy_pct"]
    mom_pct = (latest["value_jpy_mn"] / prev["value_jpy_mn"] - 1) * 100

    # 10년 내 최고치인지 확인
    last_10y_mask = mt_df["date"] >= (latest["date"] - pd.DateOffset(years=10))
    is_10y_high = latest["value_jpy_mn"] >= mt_df.loc[last_10y_mask, "value_jpy_mn"].max()
    highlight = " (10년 내 최고치)" if is_10y_high else ""

    c1, c2, c3, c4 = st.columns(4)
    c1.metric(
        f"{latest['date'].strftime('%Y-%m')} 수주액",
        f"{latest['value_jpy_mn'] / 100:,.0f} 억엔",
        f"전월 대비 {mom_pct:+.1f}%",
    )
    c2.metric(
        "YoY",
        f"{latest_yoy:+.1f}%",
        f"{(latest_yoy - prev_yoy):+.1f}%p (전월 YoY 대비)",
    )
    c3.metric(
        "12개월 평균 YoY",
        f"{mt_df['yoy_pct'].tail(12).mean():+.1f}%",
    )
    max_10y = mt_df.loc[last_10y_mask, "value_jpy_mn"].max()
    c4.metric(
        "최근 10년 최고치",
        f"{max_10y / 100:,.0f} 억엔",
        highlight.strip("()") if highlight else "—",
    )

    # 기간 필터
    min_year = int(mt_df["date"].dt.year.min())
    max_year = int(mt_df["date"].dt.year.max())

    period_labels = ["1년", "3년", "5년", "10년", "전체", "사용자 지정"]
    period = st.radio(
        "기간",
        period_labels,
        horizontal=True,
        index=3,
        key="mt_period",
    )

    if period == "사용자 지정":
        y_from, y_to = st.slider(
            "연도 범위",
            min_value=min_year,
            max_value=max_year,
            value=(max(min_year, max_year - 10), max_year),
            key="mt_year_range",
        )
        mask = (mt_df["date"].dt.year >= y_from) & (mt_df["date"].dt.year <= y_to)
    else:
        years_map = {"1년": 1, "3년": 3, "5년": 5, "10년": 10, "전체": None}
        n = years_map[period]
        if n is None:
            mask = pd.Series(True, index=mt_df.index)
        else:
            cutoff = latest["date"] - pd.DateOffset(years=n)
            mask = mt_df["date"] >= cutoff

    view = mt_df.loc[mask].copy()

    # 수주액 차트
    fig_val = go.Figure()
    fig_val.add_trace(
        go.Bar(
            x=view["date"],
            y=view["value_jpy_mn"],
            marker_color="#4C78A8",
            name="수주액(JPY 백만)",
            hovertemplate="%{x|%Y-%m}<br>%{y:,.0f} JPY 백만<extra></extra>",
        )
    )
    # 10년 내 최고치 표기
    ten_year_ago = latest["date"] - pd.DateOffset(years=10)
    in_view_10y = view[view["date"] >= ten_year_ago]
    if not in_view_10y.empty:
        peak_row = in_view_10y.loc[in_view_10y["value_jpy_mn"].idxmax()]
        fig_val.add_annotation(
            x=peak_row["date"],
            y=peak_row["value_jpy_mn"],
            text=f"10년 최고<br>{peak_row['date'].strftime('%Y-%m')}<br>{peak_row['value_jpy_mn']:,.0f}",
            showarrow=True,
            arrowhead=2,
            ax=0,
            ay=-40,
            font=dict(size=11, color="#D62728"),
            bgcolor="rgba(255,255,255,0.85)",
            bordercolor="#D62728",
        )
    fig_val.update_layout(
        title=f"월별 수주액 ({mt_unit})",
        xaxis_title="",
        yaxis_title="JPY 백만",
        height=320,
        margin=dict(l=10, r=10, t=50, b=20),
        showlegend=False,
    )
    st.plotly_chart(fig_val, width="stretch")

    # YoY 차트
    view_yoy = view.dropna(subset=["yoy_pct"])
    if not view_yoy.empty:
        colors = ["#D62728" if v < 0 else "#2CA02C" for v in view_yoy["yoy_pct"]]
        fig_yoy = go.Figure()
        fig_yoy.add_trace(
            go.Bar(
                x=view_yoy["date"],
                y=view_yoy["yoy_pct"],
                marker_color=colors,
                name="YoY(%)",
                hovertemplate="%{x|%Y-%m}<br>YoY %{y:+.1f}%<extra></extra>",
            )
        )
        fig_yoy.add_hline(y=0, line_width=1, line_color="#888")
        fig_yoy.update_layout(
            title="전년동월대비(YoY) 증감률",
            xaxis_title="",
            yaxis_title="%",
            height=300,
            margin=dict(l=10, r=10, t=50, b=20),
            showlegend=False,
        )
        st.plotly_chart(fig_yoy, width="stretch")

    with st.expander("📋 최근 데이터 (최근 24개월)", expanded=False):
        tbl = mt_df.tail(24).iloc[::-1].copy()
        tbl["월"] = tbl["date"].dt.strftime("%Y-%m")
        tbl["수주액 (JPY 백만)"] = tbl["value_jpy_mn"].map(lambda x: f"{x:,.0f}")
        tbl["YoY (%)"] = tbl["yoy_pct"].map(
            lambda x: f"{x:+.1f}%" if pd.notna(x) else "—"
        )
        st.dataframe(
            tbl[["월", "수주액 (JPY 백만)", "YoY (%)"]],
            width="stretch",
            hide_index=True,
        )

    st.caption(f"출처: {mt_src} · Trading Economics · 최근 수집 {mt_fetched}")

st.divider()

# 텅스텐 APT 가격 (KOMIS)
st.subheader("⚙️ 텅스텐 APT 가격 (KOMIS)")

try:
    w_df_m = w_crawler.fetch_series(resample="M")
except Exception as e:
    w_df_m = None
    st.warning(f"텅스텐 가격 데이터 로딩 실패: {e}")

if w_df_m is not None and not w_df_m.empty:
    w_src = w_df_m.attrs.get("source", "KOMIS")
    w_unit = w_df_m.attrs.get("unit", "USD/kg")
    w_fetched = w_df_m.attrs.get("fetched_at", "")
    w_latest_price = w_df_m.attrs.get("latest_price", 0.0)
    w_latest_yoy = w_df_m.attrs.get("latest_yoy_pct", 0.0)
    w_latest_mom = w_df_m.attrs.get("latest_mom_pct", 0.0)
    w_latest_date = w_df_m.attrs.get("latest_date", "")
    w_latest_label = (
        datetime.strptime(w_latest_date, "%Y%m%d").strftime("%Y-%m-%d")
        if len(w_latest_date) == 8 else w_latest_date
    )

    # 와이지-원 IR 기준 텅스텐 파우더·환봉 6배 기준일 추정
    REF_DATE = "2025-03"
    w_ref_row = w_df_m[w_df_m["date"].dt.to_period("M").astype(str) == REF_DATE]
    ref_price = float(w_ref_row["price_usd_kg"].iloc[0]) if not w_ref_row.empty else None

    c1, c2, c3, c4 = st.columns(4)
    c1.metric(
        f"최신 APT 가격 ({w_latest_label})",
        f"{w_latest_price:.2f} USD/kg",
        f"전월 대비 {w_latest_mom:+.1f}%",
    )
    c2.metric(
        "YoY",
        f"{w_latest_yoy:+.1f}%",
        help="전년 동기 대비 등락률",
    )
    c3.metric(
        f"기준일 ({REF_DATE}) 대비 배율",
        f"{w_latest_price / ref_price:.1f}배" if ref_price else "—",
        help=f"{REF_DATE} 대비 현재 가격 배율 (와이지-원 IR 기준일)",
    )
    w_min10 = w_df_m[w_df_m["date"] >= (w_df_m["date"].max() - pd.DateOffset(years=10))]
    c4.metric(
        "10년 최고가",
        f"{w_min10['price_usd_kg'].max():.2f} USD/kg",
        w_min10.loc[w_min10["price_usd_kg"].idxmax(), "date"].strftime("%Y-%m"),
    )

    # 기간 필터 — 1년 고정
    w_cutoff = w_df_m["date"].max() - pd.DateOffset(years=1)
    w_mask = w_df_m["date"] >= w_cutoff
    w_view = w_df_m.loc[w_mask].copy()

    # 가격 차트 (월별 평균)
    fig_w = go.Figure()
    fig_w.add_trace(go.Scatter(
        x=w_view["date"],
        y=w_view["price_usd_kg"],
        mode="lines",
        line=dict(color="#E377C2", width=2),
        name="APT 가격",
        hovertemplate="%{x|%Y-%m}<br>%{y:.2f} USD/kg<extra></extra>",
    ))
    # 현재 최고가 annotation
    w_peak = w_view.loc[w_view["price_usd_kg"].idxmax()]
    fig_w.add_annotation(
        x=w_peak["date"],
        y=w_peak["price_usd_kg"],
        text=f"최고 {w_peak['price_usd_kg']:.1f}<br>{w_peak['date'].strftime('%Y-%m')}",
        showarrow=True,
        arrowhead=2,
        ax=0, ay=-40,
        font=dict(size=11, color="#D62728"),
        bgcolor="rgba(255,255,255,0.85)",
        bordercolor="#D62728",
    )
    fig_w.update_layout(
        title=f"텅스텐 APT 월평균 가격 ({w_unit})",
        xaxis_title="",
        yaxis_title="USD/kg",
        height=320,
        margin=dict(l=10, r=10, t=50, b=20),
        showlegend=False,
    )
    st.plotly_chart(fig_w, width="stretch")

    # YoY 차트
    w_view_yoy = w_view.dropna(subset=["yoy_pct"])
    if not w_view_yoy.empty:
        w_colors = ["#D62728" if v < 0 else "#2CA02C" for v in w_view_yoy["yoy_pct"]]
        fig_wyoy = go.Figure()
        fig_wyoy.add_trace(go.Bar(
            x=w_view_yoy["date"],
            y=w_view_yoy["yoy_pct"],
            marker_color=w_colors,
            name="YoY(%)",
            hovertemplate="%{x|%Y-%m}<br>YoY %{y:+.1f}%<extra></extra>",
        ))
        fig_wyoy.add_hline(y=0, line_width=1, line_color="#888")
        fig_wyoy.update_layout(
            title="전년동월대비(YoY) 증감률 — APT",
            xaxis_title="",
            yaxis_title="%",
            height=300,
            margin=dict(l=10, r=10, t=50, b=20),
            showlegend=False,
        )
        st.plotly_chart(fig_wyoy, width="stretch")

    with st.expander("📋 최근 데이터 (최근 24개월)", expanded=False):
        w_tbl = w_df_m.tail(24).iloc[::-1].copy()
        w_tbl["월"] = w_tbl["date"].dt.strftime("%Y-%m")
        w_tbl["APT (USD/kg)"] = w_tbl["price_usd_kg"].map(lambda x: f"{x:.2f}")
        w_tbl["YoY (%)"] = w_tbl["yoy_pct"].map(
            lambda x: f"{x:+.1f}%" if pd.notna(x) else "—"
        )
        st.dataframe(
            w_tbl[["월", "APT (USD/kg)", "YoY (%)"]],
            width="stretch",
            hide_index=True,
        )

    # 와이지-원 연계 인사이트 카드
    if ref_price and w_latest_price > 0:
        mult = w_latest_price / ref_price
        st.info(
            f"**와이지-원 투자 thesis 검증** ·  "
            f"APT 기준 {REF_DATE}→최신: **{mult:.1f}배** 상승 "
            f"({ref_price:.1f} → {w_latest_price:.1f} USD/kg) · "
            f"YoY **{w_latest_yoy:+.1f}%** — "
            f"IR 주장 '약 6배, YoY 300~500%'와 "
            f"{'부합' if mult >= 4.0 else '수준 미달'}"
        )

    st.caption(f"출처: {w_src} · 최근 수집 {w_fetched}")

st.divider()

# ── OPM 시뮬레이터 (현재 와이지-원 전용 — 신규 회사 추가 시 별도 설정 필요) ──
st.subheader("🔬 OPM 시뮬레이터")
if selected_company != "와이지-원":
    st.info(f"**{selected_company}** 전용 수치 설정이 필요합니다. 현재는 와이지-원 기준으로 표시됩니다.")
st.caption("2025년 실적을 기반으로 판가·물량·원재료 변수를 조정해 영업이익률을 추정합니다.")

# ── 기준값 (2025 연간 연결, 단위: 억원) ─────────────────────────────────────
_BASE_REV   = 6394.0   # 매출액
_BASE_COGS  = 4179.0   # 매출원가
_BASE_SGNA  = 1547.0   # 판매비와관리비
_BASE_OP    = 665.0    # 영업이익
_BASE_OPM   = _BASE_OP / _BASE_REV * 100  # 10.4%
# 비영업·세금 (2025 실적 고정 가정)
_BASE_NOI   = -286.0   # 비영업손익 합계 (기타수익285 − 기타비용187 + 금융수익110 − 금융비용455 − 지분법21)
_BASE_TAX_R = 0.332    # 실효세율 (법인세126 / 세전이익379)

# 최근 4년 실적 (bridge 참고용)
_hist = {
    "2022": dict(rev=5498, op=726, opm=726/5498*100),
    "2023": dict(rev=5532, op=547, opm=547/5532*100),
    "2024": dict(rev=5750, op=558, opm=558/5750*100),
    "2025": dict(rev=6394, op=665, opm=665/6394*100),
}

col_hist, col_ctrl = st.columns([2, 3], gap="large")

with col_hist:
    st.markdown("**📊 연간 실적 추이 (OPM)**")
    hist_years = list(_hist.keys())
    hist_opms  = [_hist[y]["opm"] for y in hist_years]
    fig_hist = go.Figure()
    fig_hist.add_trace(go.Bar(
        x=hist_years, y=hist_opms,
        marker_color=["#AEC7E8"] * 3 + ["#1F77B4"],
        text=[f"{v:.1f}%" for v in hist_opms], textposition="outside",
        hovertemplate="%{x}<br>OPM %{y:.1f}%<extra></extra>",
    ))
    fig_hist.update_layout(
        height=230, margin=dict(l=5, r=5, t=30, b=5),
        yaxis=dict(title="OPM (%)", range=[0, max(hist_opms) * 1.35]),
        showlegend=False,
    )
    st.plotly_chart(fig_hist, width="stretch")

    st.markdown("**2025 기준값**")
    bcol1, bcol2 = st.columns(2)
    bcol1.metric("매출액", f"{_BASE_REV:,.0f}억")
    bcol2.metric("영업이익", f"{_BASE_OP:,.0f}억")
    bcol1.metric("매출원가율", f"{_BASE_COGS/_BASE_REV*100:.1f}%")
    bcol2.metric("OPM", f"{_BASE_OPM:.1f}%")

with col_ctrl:
    st.markdown("**⚙️ 시뮬레이션 파라미터**")

    st.caption("※ 모든 변화율은 **2025년 연간 실적 대비 전년동기(YoY) 증감률** 기준입니다.")

    sim_price  = st.slider(
        "📈 판가 변화율 — ASP (YoY, %)",
        min_value=-10, max_value=80, value=15, step=1, format="%d%%",
        help=(
            "제품 평균 판매단가(ASP)의 2025년 대비 연간 가중평균 변화율.\n"
            "실제 공문 기반 연간 가중평균 (1~2월 동결, 3~4월 1차, 5~12월 복합):\n"
            "  보수 (3월+10%, 5월+15%) → 연간 +19%\n"
            "  중립 (3월+12%, 5월+30%) → 연간 +32%\n"
            "  적극 (3월+15%, 5월+50%) → 연간 +51%"
        ),
        key="sim_price")

    # 공문 기반 구간 레이블
    _price_label = (
        "📋 공문 이전 수준" if sim_price < 15 else
        "📋 공문 보수 (3월+10%·5월+15%, 연평균+19%)" if sim_price <= 22 else
        "📋 공문 중립 (3월+12%·5월+30%, 연평균+32%)" if sim_price <= 38 else
        "📋 공문 적극 (3월+15%·5월+50%, 연평균+51%)" if sim_price <= 55 else
        "⚡ 공문 상단 초과 (추가 인상 가정)"
    )
    st.caption(f"{_price_label}")
    sim_vol    = st.slider(
        "📦 물량 변화율 — Volume (YoY, %)",
        min_value=-15, max_value=30, value=7, step=1, format="%d%%",
        help="판매 수량의 2025년 대비 변화율. JMTBA 회복 + M/S 확대 가정",
        key="sim_vol")
    sim_w_cost = st.slider(
        "⛏️ 텅스텐 원재료 원가 변화율 (YoY, %)",
        min_value=-10, max_value=300, value=50, step=5, format="%d%%",
        help=(
            "2025 PL 대비 2026 PL에 반영될 원재료 단가 변화율.\n"
            "※ 재고회전 13개월 기준 구간 해석:\n"
            "  +50~60%  → 2026 Base: 2025 매입분 소진 (연평균 44 USD/kg vs 2024 연평균 28)\n"
            "  +100~150% → Bear: 2025 Q3~Q4 고가 매입분 빠르게 소진\n"
            "  +200~300% → Stress: 저가재고 소진 후 시장가 직반영 (2027 리스크 선반영)\n"
            "  시장 APT 변화율(+600%)과 다름 — PL 반영은 재고 래그(13개월) 적용"
        ),
        key="sim_w_cost")

    # 슬라이더 값에 따른 시나리오 레이블 표시
    _w_label = (
        "✅ Base (2025 매입분 소진, 재고 13개월 버퍼)" if sim_w_cost <= 70 else
        "⚠️ Bear (2025 Q3~Q4 고가분 조기 소진)" if sim_w_cost <= 150 else
        "🔴 Stress (저가재고 소진, 시장가 근접 반영)"
    )
    _w_implied_price = 28 * (1 + sim_w_cost / 100)  # 2024 기준가 28 USD/kg 역산
    st.caption(f"{_w_label} — 역산 실효 매입가 약 **{_w_implied_price:.0f} USD/kg** "
               f"(2024 기준가 28 USD/kg × {1 + sim_w_cost/100:.2f}배)")
    sim_w_ratio = st.slider(
        "🧩 텅스텐 원재료 비중 — COGS 내 (수준값, %)",
        min_value=10, max_value=55, value=30, step=1, format="%d%%",
        help="매출원가에서 원재료(텅스텐 등)가 차지하는 비중. IR 언급 '제조원가 중 원재료 약 30%' 기준",
        key="sim_w_ratio")
    sim_sgna   = st.slider(
        "🏢 판관비 변화율 — SG&A (YoY, %)",
        min_value=-5, max_value=15, value=3, step=1, format="%d%%",
        help="판매비와관리비의 2025년(1,547억) 대비 변화율. 물량 독립(고정비성). 2024→2025 실제 +6.0%",
        key="sim_sgna")
    sim_auto   = st.slider(
        "🤖 자동화·효율화 효과 (기타 고정원가 절감, %)",
        min_value=-10, max_value=5, value=-3, step=1, format="%d%%",
        help="기타 매출원가 중 고정비 부분(인건비·감가상각)의 효율화율. 음수=절감. 국내인력 1,582→1,230명 반영",
        key="sim_auto")
    sim_other_var = st.slider(
        "📊 기타 매출원가 변동비 비중 (수준값, %)",
        min_value=0, max_value=70, value=40, step=5, format="%d%%",
        help=(
            "기타 매출원가(인건비·에너지·소모품) 중 물량에 비례하는 변동비 비중.\n"
            "  0%  → 완전 고정 (이전 모델, OPM 과대추정)\n"
            " 40%  → 현실적 추정 (에너지·소모품 변동)\n"
            " 60%  → 보수적 (역사적 피크 13% 수준 재현)\n"
            "역사적 피크 OPM 13.2%(2022)를 역산하면 약 55~60% 수준"
        ),
        key="sim_other_var")

    # ── 계산 ───────────────────────────────────────────────────────────────
    price_mult = (1 + sim_price / 100)
    vol_mult   = (1 + sim_vol   / 100)
    sim_rev    = _BASE_REV * price_mult * vol_mult

    w_frac       = sim_w_ratio / 100
    _BASE_RAW    = _BASE_COGS * w_frac        # 원재료비 — 변동비
    _BASE_OTHER  = _BASE_COGS * (1 - w_frac)  # 기타 매출원가

    # 원재료비: 물량 × 단가 변화 (변동비)
    cogs_raw = _BASE_RAW * vol_mult * (1 + sim_w_cost / 100)

    # 기타 매출원가: 변동비 부분(물량 비례) + 고정비 부분(자동화 효과)
    _var_frac          = sim_other_var / 100
    cogs_other_var     = _BASE_OTHER * _var_frac * vol_mult           # 변동비: 물량 연동
    cogs_other_fixed   = _BASE_OTHER * (1 - _var_frac) * (1 + sim_auto / 100)  # 고정비: 자동화
    cogs_other         = cogs_other_var + cogs_other_fixed

    sim_cogs   = cogs_raw + cogs_other
    sim_sgna_v = _BASE_SGNA * (1 + sim_sgna / 100)
    sim_op     = sim_rev - sim_cogs - sim_sgna_v
    sim_opm    = sim_op / sim_rev * 100 if sim_rev > 0 else 0

    # ── 요인 분해: 순차 귀속 ─────────────────────────────────────────────
    _opm0 = _BASE_OPM
    # ① 판가
    _rev_1  = _BASE_REV * price_mult
    _op_1   = _rev_1 - _BASE_RAW - _BASE_OTHER - _BASE_SGNA
    _opm1   = _op_1 / _rev_1 * 100 if _rev_1 > 0 else 0
    _price_effect = _opm1 - _opm0
    # ② 물량 (원재료 + 기타변동비 함께 증가, 고정비 희석)
    _rev_2          = _rev_1 * vol_mult
    _raw_2          = _BASE_RAW * vol_mult
    _other_var_2    = _BASE_OTHER * _var_frac * vol_mult    # 변동 부분 증가
    _other_fixed_2  = _BASE_OTHER * (1 - _var_frac)         # 고정 부분 불변
    _other_2        = _other_var_2 + _other_fixed_2
    _op_2   = _rev_2 - _raw_2 - _other_2 - _BASE_SGNA
    _opm2   = _op_2 / _rev_2 * 100 if _rev_2 > 0 else 0
    _vol_op_effect = _opm2 - _opm1
    # ③ 원재료 단가
    _raw_3  = _BASE_RAW * vol_mult * (1 + sim_w_cost / 100)
    _op_3   = _rev_2 - _raw_3 - _other_2 - _BASE_SGNA
    _opm3   = _op_3 / _rev_2 * 100 if _rev_2 > 0 else 0
    _w_cost_effect = _opm3 - _opm2
    # ④ 자동화 (고정비 부분만)
    _other_fixed_4  = _BASE_OTHER * (1 - _var_frac) * (1 + sim_auto / 100)
    _other_4        = _other_var_2 + _other_fixed_4
    _op_4   = _rev_2 - _raw_3 - _other_4 - _BASE_SGNA
    _opm4   = _op_4 / _rev_2 * 100 if _rev_2 > 0 else 0
    _auto_effect = _opm4 - _opm3
    # ⑤ 판관비
    _sgna_effect = sim_opm - _opm4

with col_ctrl:
    r1c1, r1c2, r1c3 = st.columns(3)
    r1c1.metric("예상 매출액", f"{sim_rev:,.0f}억",
                f"{(sim_rev/_BASE_REV-1)*100:+.1f}%")
    r1c2.metric("예상 영업이익", f"{sim_op:,.0f}억",
                f"{(sim_op/_BASE_OP-1)*100:+.1f}%")
    r1c3.metric("예상 OPM", f"{sim_opm:.1f}%",
                f"{sim_opm-_BASE_OPM:+.1f}%p vs 2025")

# ── Bridge (Waterfall) 차트 ─────────────────────────────────────────────────
# 순서: 2025 → 원재료↑ → 판가↑ → 물량↑ → 자동화 → 판관비 → 시뮬 (Claude 분석 Step 논리와 정합)
bridge_x = ["2025 OPM", "원재료 원가", "판가 효과", "물량 효과", "자동화", "판관비", "시뮬 OPM"]
bridge_y = [
    _BASE_OPM,
    _w_cost_effect,
    _price_effect,
    _vol_op_effect,
    _auto_effect,
    _sgna_effect,
    sim_opm,
]
bridge_measure = ["absolute", "relative", "relative", "relative", "relative", "relative", "total"]

fig_bridge = go.Figure(go.Waterfall(
    orientation="v",
    measure=bridge_measure,
    x=bridge_x,
    y=bridge_y,
    text=[f"{v:.1f}%p" if i not in (0, 6) else f"{v:.1f}%" for i, v in enumerate(bridge_y)],
    textposition="outside",
    connector={"line": {"color": "rgba(63,63,63,0.5)"}},
    increasing={"marker": {"color": "#2CA02C"}},
    decreasing={"marker": {"color": "#D62728"}},
    totals={"marker": {"color": "#FF7F0E"}},
))
fig_bridge.update_layout(
    title="OPM Bridge (2025 실적 → 시뮬레이션)",
    yaxis=dict(title="OPM (%)", tickformat=".1f"),
    height=370,
    margin=dict(l=10, r=10, t=50, b=10),
    showlegend=False,
)
st.plotly_chart(fig_bridge, width="stretch")

# ── 재고 소진 시점 인디케이터 ─────────────────────────────────────────────────
_INV_2025    = 4434.0   # 2025년말 재고자산 (억원)
_MONTHLY_BASE = _BASE_COGS / 12  # 기본 월 소진액 348억

_inv_monthly  = _MONTHLY_BASE * vol_mult          # 물량 슬라이더 반영 월 소진
_inv_months   = _INV_2025 / _inv_monthly           # 재고 소진 예상 개월수 (2026.1 기준)
_inv_exhaust_month = round(_inv_months)
_inv_exhaust_label = (
    f"2026년 {_inv_exhaust_month}월" if _inv_exhaust_month <= 12
    else f"2027년 {_inv_exhaust_month - 12}월"
)

st.divider()
st.markdown("**🏭 재고 소진 예상 시점 (물량 슬라이더 연동)**")
ic1, ic2, ic3 = st.columns(3)
ic1.metric("현재 재고", f"{_INV_2025:,.0f}억", "2025년말 기준")
ic2.metric("월 소진 속도", f"{_inv_monthly:,.0f}억/월",
           f"{(vol_mult-1)*100:+.0f}% vs 기준({_MONTHLY_BASE:.0f}억)")
ic3.metric("저가재고 소진 예상", _inv_exhaust_label,
           f"{_inv_months:.1f}개월 후",
           delta_color="inverse")

# 소진 시점에 따른 2027 리스크 경고
if _inv_months < 10:
    st.warning(
        f"⚠️ **재고 소진 가속:** 현재 물량 가정 시 약 {_inv_months:.1f}개월 후 저가 재고 소진 "
        f"→ **{_inv_exhaust_label}부터** 2025~2026년 고가 매입분(시장가 164+ USD/kg)이 원가에 반영. "
        f"판가 추가 인상 또는 APT 가격 하락 없으면 **2027 OPM 급락 리스크**."
    )
elif _inv_months < 12:
    st.info(
        f"ℹ️ 재고 소진 예상: {_inv_exhaust_label} — 연말까지 저가 재고 버퍼 유지되나 "
        f"**2027년 초부터 고가 매입분 원가 반영 시작** 가능. 판가 방어선 모니터링 필요."
    )
else:
    st.success(
        f"✅ 저가 재고 {_inv_months:.1f}개월 지속 — {_inv_exhaust_label}까지 원가 버퍼 유지. "
        f"2026년 내 원가 충격 제한적."
    )

# ── 시나리오 요약표 ──────────────────────────────────────────────────────────
with st.expander("📋 시나리오 비교 (가수요 포함 4개)", expanded=False):
    _sc_ov = sim_other_var
    scenarios = {
        # ── 공문 기반 (실제 판가 인상 공문 반영) ──────────────────────────────
        "📋 공문-보수 (3월+10%·5월+15%, 연평균+19%)":  dict(price=19, vol=7,  w_cost=60,  w_ratio=30, sgna=3, auto=-3, ov=_sc_ov),
        "📋 공문-중립 (3월+12%·5월+30%, 연평균+32%)":  dict(price=32, vol=5,  w_cost=80,  w_ratio=30, sgna=4, auto=-3, ov=_sc_ov),
        "📋 공문-적극 (3월+15%·5월+50%, 연평균+51%)":  dict(price=51, vol=3,  w_cost=100, w_ratio=30, sgna=5, auto=-2, ov=_sc_ov),
        # ── 기타 시나리오 ───────────────────────────────────────────────────
        "가수요 🔥 (급등·물량폭증·재고조기소진)":       dict(price=22, vol=25, w_cost=122, w_ratio=30, sgna=5, auto=-2, ov=_sc_ov),
        "전략적지연 🎯 (공문하단+19%·물량폭증·M/S탈취)": dict(price=19, vol=20, w_cost=60, w_ratio=30, sgna=4, auto=-3, ov=_sc_ov),
        "Base ⚖️ (재고 13개월 버퍼)":                  dict(price=15, vol=7,  w_cost=50,  w_ratio=30, sgna=3, auto=-3, ov=_sc_ov),
        "Bear 🐻 (재고조기소진·판가전가실패)":          dict(price=5,  vol=-3, w_cost=150, w_ratio=30, sgna=5, auto=0,  ov=_sc_ov),
    }
    rows = []
    for name, s in scenarios.items():
        p_m = 1 + s["price"]/100; v_m = 1 + s["vol"]/100
        wf = s["w_ratio"]/100; vf = s["ov"]/100
        rev      = _BASE_REV * p_m * v_m
        cg_raw   = _BASE_COGS * wf * v_m * (1 + s["w_cost"]/100)
        cg_ov    = _BASE_COGS * (1-wf) * vf * v_m                       # 변동비: 물량 연동
        cg_fixed = _BASE_COGS * (1-wf) * (1-vf) * (1 + s["auto"]/100)  # 고정비: 자동화
        op  = rev - cg_raw - cg_ov - cg_fixed - _BASE_SGNA * (1 + s["sgna"]/100)
        opm = op / rev * 100
        # 당기순이익: 비영업손익 고정(-286억) + 실효세율 33.2% 적용
        ebt = op + _BASE_NOI
        ni  = ebt * (1 - _BASE_TAX_R) if ebt > 0 else ebt  # 결손 시 세금 없음
        npm = ni / rev * 100
        inv_m = _INV_2025 / (_MONTHLY_BASE * v_m)
        exhaust = f"2026년 {round(inv_m)}월" if round(inv_m) <= 12 else f"2027년 {round(inv_m)-12}월"
        rows.append({
            "시나리오": name,
            "판가": f"{s['price']:+}%", "물량": f"{s['vol']:+}%",
            "원재료원가": f"{s['w_cost']:+}%",
            "예상매출(억)": f"{rev:,.0f}",
            "예상OPM": f"{opm:.1f}%",
            "예상OP(억)": f"{op:,.0f}",
            "예상NI(억)": f"{ni:,.0f}",
            "NPM": f"{npm:.1f}%",
            "재고소진": exhaust,
        })
    st.dataframe(pd.DataFrame(rows), width="stretch", hide_index=True)
    st.caption(
        "📋 **공문 기반**: 2026년 3월 10~15% + 5월 15~50% 인상 공문 기준 연간 가중평균 적용 "
        "(1~2월 동결, 3~4월 1차, 5~12월 복합 누계). "
        "원재료 원가는 보수·중립·적극 순으로 빠른 재고 소진 속도를 반영.\n\n"
        "🎯 **전략적 지연**: 경쟁사보다 판가를 낮게 유지해 M/S 확대. "
        "저가 재고 덕분에 낮은 판가에서도 OPM 유지 — 재고 소진은 1~2개월 빨라짐.\n\n"
        "🔥 **가수요**: 2026 OPM은 Base와 비슷(15%)하나 재고 소진이 2~3개월 앞당겨짐. "
        "진짜 리스크는 **2027년** — 2026년 고가 매입분(164+ USD/kg) 전면 반영 시점.\n\n"
        "💡 **당기순이익(NI) 산출 방법**: 영업이익 + 비영업손익 고정(−286억: 기타수익285·기타비용187·금융수익110·금융비용455·지분법−21) "
        "= 세전이익 → 실효세율 33.2% 적용 (2025 기준: 법인세126/세전379). 결손 시 세금 미반영."
    )

# ── 계산 근거 및 가정 ────────────────────────────────────────────────────────
with st.expander("📐 OPM 시뮬레이터 계산 근거 및 가정", expanded=False):
    st.markdown("""
#### 1️⃣ 기준값 출처 — **2025년 연결 손익 (단위: 억원)**
`data/financials/연간_손익계산서.csv` 2025년 실적 컬럼 기준.

| 항목 | 값 | 비율 |
|---|---:|---:|
| 매출액 | **6,394** | 100.0% |
| 매출원가 | **4,179** | 65.4% |
| 매출총이익 | 2,214 | 34.6% |
| 판관비 | **1,547** | 24.2% |
| **영업이익** | **665** | **OPM 10.4%** |

---

#### 2️⃣ 원가 구조 분해 및 비용 성격 분류

IR 언급 "**제조원가 중 원재료 약 30%**" 기준으로 COGS를 3개 비용 성격으로 분해.
**기타 매출원가**는 완전 고정비가 아니라 **변동비 + 고정비 혼합**으로 모델링 (근거: ⑤ 교차검증 참조).

| 구성 | 산식 | 금액(억) | 성격 | 물량 연동 |
|---|---|---:|---|:---:|
| ① 원재료비 | 4,179 × 30% | **1,254** | 순수 변동비 (텅스텐·초경 소재) | **100% 비례** |
| ② 기타원가-변동 | 2,925 × 변동비% | **1,170** *(기본: 40%)* | 외주·소모품·초과근무 등 | **물량 비례** |
| ③ 기타원가-고정 | 2,925 × (1−변동비%) | **1,755** *(기본: 60%)* | 인건비·감가상각·에너지 기본분 | 자동화 슬라이더 |
| ④ 판관비 | — | **1,547** | 고정비성 | 판관비 슬라이더 |

→ **물량 증가 시**: ① + ② 비례 증가, ③ + ④ 고정 유지 → OPM 레버리지 발생.

---

#### 3️⃣ 계산 공식 (슬라이더 → OPM)

```
예상 매출          = 6,394 × (1 + 판가%) × (1 + 물량%)

원재료비            = 1,254 × (1 + 물량%) × (1 + 원재료원가%)   ← 순수 변동비

기타원가-변동       = 2,925 × 변동비비중% × (1 + 물량%)         ← 물량 비례
기타원가-고정       = 2,925 × (1−변동비비중%) × (1 + 자동화%)   ← 고정비 (자동화 절감)
기타 매출원가 합계  = 기타원가-변동 + 기타원가-고정

예상 판관비         = 1,547 × (1 + 판관비%)                     ← 고정비

예상 영업이익       = 예상매출 − 원재료비 − 기타원가 − 예상판관비
예상 OPM            = 예상영업이익 / 예상매출 × 100
```

**Bridge 차트 효과 분해 (OPM %p, 순차 귀속 — 합산 오차 없음)**

| 단계 | 귀속 효과 | 설명 |
|---|---|---|
| ① 판가 | `+판가% × (1 / (1+판가%))` | 매출만 증가, 원가 불변 → 전액 OP 기여 |
| ② 물량 | 고정비 희석 효과 | 매출+원가 모두 증가하나 ③④ 고정비 희석 |
| ③ 원재료 단가 | `-원재료비증분 / 예상매출` | 텅스텐 단가 상승만의 OP 훼손 |
| ④ 자동화 | `+기타고정비절감 / 예상매출` | 인력 효율화로 고정비 절감 |
| ⑤ 판관비 | `-판관비증분 / 예상매출` | 잔차 = 최종 OPM과 일치 |

---

#### 4️⃣ 슬라이더 기본값 근거

| 슬라이더 | 기본값 | 근거 |
|---|:---:|---|
| 📈 판가 (ASP) | **+15%** | 공문 하단 보수 실행 아래 연평균 (→ 공문 구간 표 참조) |
| 📦 물량 (Volume) | **+7%** | JMTBA 2026.3 전년동월 +10%대 회복 + YG-1 M/S 확대 |
| ⛏️ 텅스텐 원재료 원가 | **+50%** | KOMIS 재고회전 래그 역산: 2026 P/L 반영 매입가 ≈ 2025 연평균(44 USD/kg) / 2025 P/L 반영 매입가 ≈ 2024 연평균(28 USD/kg) → **+57%** → 버퍼 감안 +50% |
| 🧩 원재료 비중 (COGS) | **30%** | IR "제조원가 중 원재료 약 30%" |
| 🏢 판관비 | **+3%** | 2024→2025 실제 +6.0% 대비 보수 가정 |
| 🤖 자동화·효율화 | **−3%** | 국내 인력 1,582(2021)→1,230(2025), 서운1공장 불량률 1%→0%대 |
| 📊 기타원가 변동비 비중 | **40%** | 역사적 피크 OPM 13.2%(2022) 역산 근거 (→ ⑤ 교차검증 참조) |

**텅스텐 원재료 슬라이더 시나리오 구간**

| 슬라이더 값 | 의미 | 역산 실효 매입가 |
|:---:|---|---|
| ~70% | ✅ Base — 2025 매입분(저가 재고) 충분히 남음 | ~48 USD/kg |
| 71~150% | ⚠️ Bear — 2025 Q3~Q4 고가 매입분 조기 반영 시작 | ~49~84 USD/kg |
| 151~300% | 🔴 Stress — 저가 재고 소진, 2026 시장가(164+) 근접 | ~84~112 USD/kg |

---

#### 5️⃣ 판가 인상 공문 구간 해설

2026년 공문 기준 연간 가중평균 판가 = **(1~2월: 0%) + (3~4월: 1차 인상%) + (5~12월: 누적 복합%)** / 12개월

| 시나리오 | 3월 1차 | 5월 2차 | 연평균 가중값 | 전략 해석 |
|---|:---:|:---:|:---:|---|
| 공문-보수 | +10% | +15% | **+19%** | 공문 **하단** 실행 — "한 걸음 느리게", 물량으로 M/S |
| 공문-중립 | +12% | +30% | **+32%** | 공문 중간값 — 균형 전략 |
| 공문-적극 | +15% | +50% | **+51%** | 공문 **상단** 실행 — 경쟁사와 동조 |

> 공식 인상 전(1~2월)은 0%, 3~4월 1차 적용, 5월부터 연말까지 복합 누적 계산.
> **전략적 지연 시나리오** = 공문 하단(+19%) 실행 + 물량 +20% (판가가 경쟁사보다 낮아 고객 이탈 방어 및 M/S 탈취).

---

#### 6️⃣ 시나리오별 핵심 논리

| 시나리오 | 핵심 드라이버 | 2026 OPM | 2027 리스크 |
|---|---|:---:|---|
| 공문-보수 | 판가+19%, 물량 방어 | ~18% | 낮음 |
| 공문-중립 | 판가+32%, 물량 소폭 희생 | ~22% | 낮음 |
| 공문-적극 | 판가+51%, 물량 감소 | ~28% | 낮음 |
| 전략적 지연 🎯 | **판가+19% 억제 + 물량+20% M/S 탈취** | ~22% | **중간** (재고 10.6개월로 조기 소진) |
| 가수요 🔥 | 판가+22%, 물량+25% (투기 수요) | ~15% | **높음** — 2026 고가 매입분(164+ USD/kg)이 2027 원가에 반영 |
| Bear 🐻 | 판가+5% 전가 실패, 재고 조기 소진 | ~−16% | 최악 |

> **가수요 메커니즘**: 급등기 고객 사전 재고 축적 → 단기 물량↑ → 2025~2026년 고가 매입분이 13개월 뒤 P/L 반영.
> 2026 OPM은 Base와 유사하지만 **2027년에 고가 재고 전면 소진 → 원가 급등** 시점이 핵심 리스크.

---

#### 7️⃣ 교차 검증

| 검증 항목 | 수치 | 판단 |
|---|---|---|
| 역사적 OPM 피크 | **2022년 13.2%** | Base 시나리오 OPM 13~17% = 타당한 회복 구간 |
| 기타원가 변동비 40% 근거 | 완전 고정비 가정 시 OPM 18~28% (과대추정) → 역사 피크 13.2% 교차 시 변동비 약 40%가 합리적 | ✅ 수정 반영 |
| KOMIS APT | 2025.3(28.58) → 2026.3(201.07) = **7.04배** | IR "약 6배 상승" 부합 |
| 재고회전 | 2025 재고(4,434억) / COGS(4,179억) × 12 = **12.9개월** | 저가 재고 버퍼 2026년 내내 유지 근거 |
| JMTBA 수주 | 2026.3 전년동월 +10%대 | 물량 +7% 가정의 외부 근거 |

---

#### 8️⃣ 모델 한계 및 유의사항

- **변동비 비중 불확실성**: 기타원가 내 변동비 40% 가정은 역사 OPM 역산 추정치 — 제품 믹스·공장 가동률에 따라 30~60% 범위 변동 가능
- **판가-물량 상호작용**: 단순 곱셈 모델 (수요 탄력성·경쟁사 반응 미반영)
- **원재료 비중 고정**: 제품 믹스 변화 미반영 (CVD·NCM 등 고부가 제품 비중↑ 시 원재료비율↓)
- **환율 미반영**: 해외 매출 70%+ → 원/달러 변동이 실제 OPM에 추가 영향
- **래그 단순화**: 원재료 가격 변동의 P/L 반영은 정확히 13개월이 아니라 재고 소진 패턴에 따라 분산
- **일회성 항목 제외**: 스톡옵션·충당금·PPA 상각 미반영
    """)

st.divider()

# 재무 차트
st.subheader("재무 차트")


def _to_num(v):
    try:
        return float(str(v).replace(",", ""))
    except (ValueError, TypeError):
        return 0.0


def render_chart_section(path, months, key_prefix, fin_dir):
    if not os.path.exists(path):
        st.warning(f"{os.path.basename(path)} 파일이 없어요.")
        return
    is_annual = months == ["12"]

    df = pd.read_csv(path)
    label_col = df.columns[0]
    df.index = df[label_col].astype(str).str.strip()

    required = ["매출액", "매출총이익", "영업이익"]
    missing = [r for r in required if r not in df.index]
    if missing:
        st.warning(f"손익계산서에 항목이 없어요: {', '.join(missing)}")
        return

    years = sorted({c.split("/")[0] for c in df.columns if "/" in c})
    if not years:
        st.warning("기간 열이 없어요.")
        return

    if len(years) > 1:
        start, end = st.select_slider(
            "기간 선택",
            options=years,
            value=(years[0], years[-1]),
            key=f"{key_prefix}_chart_range",
        )
    else:
        start = end = years[0]

    selected = [
        f"{y}/{m}" for y in years if start <= y <= end for m in months
        if f"{y}/{m}" in df.columns
    ]
    if not selected:
        st.warning("선택된 기간이 없어요.")
        return

    rev = [_to_num(df.at["매출액", c]) for c in selected]
    gp = [_to_num(df.at["매출총이익", c]) for c in selected]
    op = [_to_num(df.at["영업이익", c]) for c in selected]
    gpm = [g / r * 100 if r else 0 for g, r in zip(gp, rev)]
    opm = [o / r * 100 if r else 0 for o, r in zip(op, rev)]

    max_rev = max(rev) if rev else 0
    max_op = max(op) if op else 0
    min_op = min(op) if op else 0
    pad = 1.1
    L_max = max(max_rev * pad, 1)
    if max_op > 0 and min_op < 0:
        R_max, R_min = max_op * pad, min_op * pad
    elif max_op > 0:
        R_max, R_min = max_op * pad, 0
    elif min_op < 0:
        R_max, R_min = abs(min_op) * 0.1, min_op * pad
    else:
        R_max, R_min = 1, 0
    L_min = L_max * R_min / R_max if R_max > 0 else 0

    st.caption("매출액 / 영업이익 (억원)")
    fig = make_subplots(specs=[[{"secondary_y": True}]])
    fig.add_trace(
        go.Bar(
            x=selected, y=rev, name="매출액",
            marker_color="#4C78A8",
            text=[f"{v:,.0f}" for v in rev],
            textposition="inside",
            textfont=dict(size=12, color="white", family="Arial Black"),
        ),
        secondary_y=False,
    )
    fig.add_trace(
        go.Scatter(
            x=selected, y=op, name="영업이익",
            mode="lines+markers+text",
            line=dict(color="#F58518", width=2.5),
            marker=dict(size=8, symbol="circle"),
            text=[f"{v:,.0f}" for v in op],
            textposition="top right",
            textfont=dict(size=11, color="#F58518", family="Arial Black"),
        ),
        secondary_y=True,
    )
    fig.update_yaxes(title_text="매출액 (억원)", range=[L_min, L_max], secondary_y=False)
    fig.update_yaxes(title_text="영업이익 (억원)", range=[R_min, R_max], secondary_y=True)

    yoy_changes = []
    if is_annual:
        op_by_year = {selected[i].split("/")[0]: op[i] for i in range(len(selected))}
        yoy_changes = compute_yoy_changes(op_by_year, threshold=30)
        for cy, _, pct in yoy_changes:
            period = f"{cy}/12"
            if period in selected:
                y_val = op[selected.index(period)]
                fig.add_annotation(
                    x=period, y=y_val,
                    yref="y2",
                    text=f"{pct:+.0f}%",
                    showarrow=True,
                    arrowhead=2,
                    arrowsize=1.2,
                    arrowcolor="#D62728" if pct < 0 else "#2CA02C",
                    ax=0, ay=-40,
                    font=dict(size=12, color="#D62728" if pct < 0 else "#2CA02C", family="Arial Black"),
                    bordercolor="#D62728" if pct < 0 else "#2CA02C",
                    borderwidth=1,
                    borderpad=3,
                    bgcolor="white",
                )

    fig.update_layout(
        height=400,
        margin=dict(l=0, r=0, t=20, b=0),
        legend=dict(
            orientation="h",
            yanchor="bottom", y=1.02,
            xanchor="right", x=1,
        ),
        hovermode="x unified",
    )
    st.plotly_chart(fig, width="stretch")

    if is_annual and yoy_changes:
        st.markdown("**전년 대비 영업이익 ±30% 이상 변동 연도**")
        for cy, py, pct in yoy_changes:
            label = f"🔺 {cy}년 영업이익 {pct:+.1f}% (전년 {py} 대비)" if pct > 0 else f"🔻 {cy}년 영업이익 {pct:+.1f}% (전년 {py} 대비)"
            cached_path = os.path.join(ANALYSIS_DIR, f"op_change_{cy}.md")
            note_path = os.path.join(ANALYSIS_DIR, f"op_change_{cy}_note.md")
            with st.expander(label):
                # AI 분석 영역
                if os.path.exists(cached_path):
                    with open(cached_path, "r", encoding="utf-8") as f:
                        st.markdown(f.read())
                    cols = st.columns([5, 1])
                    cols[0].caption(f"캐시: {cached_path}")
                    if cols[1].button("🔄 재생성", key=f"regen_{key_prefix}_{cy}"):
                        os.remove(cached_path)
                        with st.spinner("Claude 재분석 중..."):
                            load_or_generate_analysis(cy, py, pct, fin_dir)
                        st.rerun()
                else:
                    if st.button(f"{cy}년 분석 생성 (Claude API 호출)", key=f"gen_{key_prefix}_{cy}"):
                        with st.spinner("분석 생성 중..."):
                            load_or_generate_analysis(cy, py, pct, fin_dir)
                        st.rerun()

                # 내 메모 영역
                st.divider()
                st.markdown("**📝 내 메모**")
                existing_note = ""
                if os.path.exists(note_path):
                    with open(note_path, "r", encoding="utf-8") as f:
                        existing_note = f.read()
                note = st.text_area(
                    "메모",
                    value=existing_note,
                    key=f"note_{key_prefix}_{cy}",
                    height=100,
                    label_visibility="collapsed",
                    placeholder="이 연도의 변동에 대한 개인 주석/추가 근거 등을 기록하세요.",
                )
                ncols = st.columns([1, 5])
                if ncols[0].button("💾 저장", key=f"save_{key_prefix}_{cy}"):
                    with open(note_path, "w", encoding="utf-8") as f:
                        f.write(note)
                    ncols[1].success(f"저장됨: {note_path}")

    st.caption("GPM / OPM (%)")
    fig_margin = go.Figure()
    fig_margin.add_trace(go.Scatter(
        x=selected, y=gpm,
        name="GPM (%)",
        mode="lines+markers+text",
        line=dict(color="#4C78A8", width=2),
        marker=dict(color="#4C78A8", size=8),
        text=[f"{v:.1f}%" for v in gpm],
        textposition="top center",
        textfont=dict(size=10, color="#4C78A8"),
        hovertemplate="%{x}<br>GPM: %{y:.1f}%<extra></extra>",
    ))
    fig_margin.add_trace(go.Scatter(
        x=selected, y=opm,
        name="OPM (%)",
        mode="lines+markers+text",
        line=dict(color="#F58518", width=2),
        marker=dict(color="#F58518", size=8),
        text=[f"{v:.1f}%" for v in opm],
        textposition="top center",
        textfont=dict(size=10, color="#F58518"),
        hovertemplate="%{x}<br>OPM: %{y:.1f}%<extra></extra>",
    ))
    fig_margin.update_layout(
        height=260,
        margin=dict(l=0, r=0, t=10, b=0),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        hovermode="x unified",
        yaxis=dict(ticksuffix="%"),
    )
    st.plotly_chart(fig_margin, width="stretch")


if os.path.exists(fin_dir):
    chart_a, chart_q = st.tabs(["연간", "분기"])
    with chart_a:
        render_chart_section(
            os.path.join(fin_dir, "연간_손익계산서.csv"),
            ["12"],
            "annual",
            fin_dir,
        )
    with chart_q:
        render_chart_section(
            os.path.join(fin_dir, "분기_손익계산서.csv"),
            ["03", "06", "09", "12"],
            "quarter",
            fin_dir,
        )
else:
    st.warning("data/financials 폴더가 없어요.")

st.divider()

# 재무제표
st.subheader("재무제표")


def render_financial_table(path, selected_cols):
    if not os.path.exists(path):
        st.warning(f"{os.path.basename(path)} 파일이 없어요.")
        return
    df = pd.read_csv(path)
    label_col = df.columns[0]
    keep = [label_col] + [c for c in selected_cols if c in df.columns]
    df = df[keep]
    period_cols = df.columns[1:]
    col_config = {label_col: st.column_config.TextColumn(label_col, width="medium")}
    for c in period_cols:
        col_config[c] = st.column_config.TextColumn(c, width="small")
    height = 38 * (len(df) + 1) + 3
    use_container = len(period_cols) <= 6
    st.dataframe(
        df,
        width="stretch" if use_container else "content",
        hide_index=True,
        height=height,
        column_config=col_config,
    )


def available_years(path):
    if not os.path.exists(path):
        return []
    header = pd.read_csv(path, nrows=0)
    return sorted({c.split("/")[0] for c in header.columns if "/" in c})


def render_section(title, file_map, months, key_prefix):
    st.markdown(f"### {title}")
    sample_path = os.path.join(fin_dir, next(iter(file_map.values())))
    years = available_years(sample_path)
    if not years:
        st.warning(f"{title} 재무제표 파일이 없어요.")
        return

    if len(years) > 1:
        start, end = st.select_slider(
            f"{title} 기간 선택",
            options=years,
            value=(years[0], years[-1]),
            key=f"{key_prefix}_range",
        )
    else:
        start = end = years[0]

    selected = [f"{y}/{m}" for y in years if start <= y <= end for m in months]

    tabs = st.tabs(list(file_map.keys()))
    for tab, (_, fname) in zip(tabs, file_map.items()):
        with tab:
            render_financial_table(os.path.join(fin_dir, fname), selected)


if os.path.exists(fin_dir):
    annual_files = {
        "손익계산서": "연간_손익계산서.csv",
        "재무상태표": "연간_재무상태표.csv",
        "현금흐름표": "연간_현금흐름표.csv",
    }
    quarter_files = {
        "손익계산서": "분기_손익계산서.csv",
        "재무상태표": "분기_재무상태표.csv",
        "현금흐름표": "분기_현금흐름표.csv",
    }

    render_section("연간", annual_files, ["12"], "annual")
    st.divider()
    render_section("분기", quarter_files, ["03", "06", "09", "12"], "quarter")
else:
    st.warning("data/financials 폴더가 없어요. financials.py 먼저 실행해 주세요.")