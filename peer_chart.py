"""YG-1 vs OSG vs KMT 주가/시가총액 연봉 차트 생성.

출력: data/companies/와이지-원/peer_price_annual.html
      data/companies/와이지-원/peer_marketcap_annual.html
"""
import os
import pandas as pd
import yfinance as yf
import plotly.graph_objects as go
from plotly.subplots import make_subplots

OUT_DIR = "data/companies/와이지-원"
os.makedirs(OUT_DIR, exist_ok=True)

PEERS = [
    {"ticker": "019210.KS", "name": "YG-1", "currency": "KRW", "fx": "KRW=X"},
    {"ticker": "6136.T",    "name": "OSG",  "currency": "JPY", "fx": "JPY=X"},
    {"ticker": "KMT",       "name": "KMT",  "currency": "USD", "fx": None},
]

START = "2017-01-01"


def annual_ohlc(ticker: str) -> pd.DataFrame:
    daily = yf.Ticker(ticker).history(start=START, interval="1d", auto_adjust=False)
    if daily.empty:
        return pd.DataFrame()
    daily.index = daily.index.tz_localize(None)
    yearly = daily.resample("YE").agg(
        {"Open": "first", "High": "max", "Low": "min", "Close": "last"}
    )
    yearly.index = yearly.index.year
    return yearly.dropna()


def annual_fx_close(fx_ticker: str) -> pd.Series:
    """USD 기준 환율(로컬통화 per 1 USD)의 연말 종가."""
    if fx_ticker is None:
        return None
    d = yf.Ticker(fx_ticker).history(start=START, interval="1d", auto_adjust=False)
    d.index = d.index.tz_localize(None)
    y = d["Close"].resample("YE").last()
    y.index = y.index.year
    return y


def annual_shares(ticker: str) -> pd.Series:
    """balance_sheet 기반 연말 발행주식수(부족 연도는 최초 보유값으로 앞단 채움)."""
    tk = yf.Ticker(ticker)
    bs = tk.balance_sheet
    row = None
    for key in ("Ordinary Shares Number", "Share Issued"):
        if key in bs.index:
            row = bs.loc[key].dropna()
            break
    if row is None or row.empty:
        return pd.Series(dtype=float)
    s = pd.Series({d.year: v for d, v in row.items()})
    s = s.sort_index()
    return s


# ── 1) 데이터 수집 ────────────────────────────────────────────────────────────
ohlc_by_peer = {}
shares_by_peer = {}
fx_by_peer = {}

for p in PEERS:
    ohlc_by_peer[p["name"]] = annual_ohlc(p["ticker"])
    shares_by_peer[p["name"]] = annual_shares(p["ticker"])
    fx_by_peer[p["name"]] = annual_fx_close(p["fx"]) if p["fx"] else None
    print(f"[{p['name']}] OHLC {len(ohlc_by_peer[p['name']])}행, shares {len(shares_by_peer[p['name']])}행")

all_years = sorted({y for df in ohlc_by_peer.values() for y in df.index})
print("대상 연도:", all_years)


# ── 2) 주가 연봉 캔들 (3 y-axis) ──────────────────────────────────────────────
fig = go.Figure()

color_ok = {"OSG": "#1f77b4", "KMT": "#2ca02c", "YG-1": "#d62728"}
axis_map = {"OSG": "y", "KMT": "y2", "YG-1": "y3"}

for p in PEERS:
    df = ohlc_by_peer[p["name"]]
    if df.empty:
        continue
    fig.add_trace(
        go.Candlestick(
            x=df.index,
            open=df["Open"], high=df["High"], low=df["Low"], close=df["Close"],
            name=f"{p['name']} ({p['currency']})",
            yaxis=axis_map[p["name"]],
            increasing_line_color=color_ok[p["name"]],
            decreasing_line_color=color_ok[p["name"]],
            increasing_fillcolor=color_ok[p["name"]],
            decreasing_fillcolor="rgba(255,255,255,0)",
            line=dict(width=1.5),
        )
    )

fig.update_layout(
    title="YG-1 vs OSG vs KMT 연봉 주가 추이 (2017–2026)",
    xaxis=dict(title="연도", rangeslider=dict(visible=False), dtick=1),
    yaxis=dict(
        title=dict(text="OSG (JPY)", font=dict(color=color_ok["OSG"])),
        tickfont=dict(color=color_ok["OSG"]),
        side="left",
    ),
    yaxis2=dict(
        title=dict(text="KMT (USD)", font=dict(color=color_ok["KMT"])),
        tickfont=dict(color=color_ok["KMT"]),
        overlaying="y", side="right",
    ),
    yaxis3=dict(
        title=dict(text="YG-1 (KRW)", font=dict(color=color_ok["YG-1"])),
        tickfont=dict(color=color_ok["YG-1"]),
        overlaying="y", side="right", position=0.97, anchor="free",
    ),
    legend=dict(orientation="h", y=-0.18),
    height=620,
    margin=dict(r=110, l=70, t=70, b=80),
    template="plotly_white",
)

price_out = os.path.join(OUT_DIR, "peer_price_annual.html")
fig.write_html(price_out, include_plotlyjs="cdn")
print("saved:", price_out)


# ── 3) 시가총액 (USD 환산) 연봉 바 ────────────────────────────────────────────
rows = []
for p in PEERS:
    name = p["name"]
    df = ohlc_by_peer[name]
    shares = shares_by_peer[name]
    fx = fx_by_peer[name]
    if df.empty or shares.empty:
        continue
    shares_filled = shares.reindex(df.index).ffill().bfill()
    for year, r in df.iterrows():
        sh = shares_filled.get(year)
        if sh is None or pd.isna(sh):
            continue
        local_mcap = r["Close"] * sh
        if fx is None:
            usd_mcap = local_mcap
        else:
            fx_rate = fx.get(year)
            if fx_rate is None or pd.isna(fx_rate) or fx_rate == 0:
                continue
            usd_mcap = local_mcap / fx_rate
        rows.append({
            "year": year, "company": name,
            "mcap_usd_b": usd_mcap / 1e9,
            "shares": sh, "close": r["Close"],
        })

mcap_df = pd.DataFrame(rows)
print(mcap_df.pivot(index="year", columns="company", values="mcap_usd_b").round(2))

fig2 = go.Figure()
for p in PEERS:
    d = mcap_df[mcap_df["company"] == p["name"]]
    if d.empty:
        continue
    fig2.add_trace(go.Bar(
        x=d["year"], y=d["mcap_usd_b"], name=p["name"],
        marker_color=color_ok[p["name"]],
        text=[f"{v:.2f}" for v in d["mcap_usd_b"]],
        textposition="outside",
    ))

fig2.update_layout(
    title="YG-1 vs OSG vs KMT 연말 시가총액 (USD 환산, 십억$)",
    xaxis=dict(title="연도", dtick=1),
    yaxis=dict(title="시가총액 (USD Billion)"),
    barmode="group",
    height=560,
    template="plotly_white",
    legend=dict(orientation="h", y=-0.15),
)

mcap_out = os.path.join(OUT_DIR, "peer_marketcap_annual.html")
fig2.write_html(mcap_out, include_plotlyjs="cdn")
print("saved:", mcap_out)

# CSV 원데이터도 저장
mcap_df.to_csv(os.path.join(OUT_DIR, "peer_marketcap_annual.csv"), index=False, encoding="utf-8-sig")

ohlc_rows = []
for p in PEERS:
    df = ohlc_by_peer[p["name"]]
    for y, r in df.iterrows():
        ohlc_rows.append({
            "year": y, "company": p["name"], "currency": p["currency"],
            "open": r["Open"], "high": r["High"], "low": r["Low"], "close": r["Close"],
        })
pd.DataFrame(ohlc_rows).to_csv(
    os.path.join(OUT_DIR, "peer_price_annual.csv"), index=False, encoding="utf-8-sig"
)
print("CSV도 저장 완료.")
