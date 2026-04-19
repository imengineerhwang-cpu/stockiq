"""일본 공작기계 수주(JMTBA) 월별 데이터 수집.

소스: https://ko.tradingeconomics.com/japan/machine-tool-orders
   - 차트 데이터 API: https://d3ii0wo49og5mi.cloudfront.net/economics/japanmactooord
   - 응답은 base64 → XOR(tradingeconomics-charts-core-api-key) → gzip → JSON
   - 1983-01부터 월별, JPY Million(수주액 합계) 제공.

캐시: data/machine_tool/te_japanmactooord.json (24h TTL)
"""
from __future__ import annotations

import base64
import json
import os
import time
import zlib
from datetime import datetime

import pandas as pd
from curl_cffi import requests as cfr

CACHE_DIR = "data/machine_tool"
CACHE_FILE = os.path.join(CACHE_DIR, "te_japanmactooord.json")
CACHE_TTL_SEC = 24 * 60 * 60  # 24시간

_API_URL = "https://d3ii0wo49og5mi.cloudfront.net/economics/japanmactooord"
_API_KEY = "20260324:loboantunes"
_OBF_KEY = b"tradingeconomics-charts-core-api-key"
_REFERER = "https://ko.tradingeconomics.com/japan/machine-tool-orders"


def _is_cache_fresh(path: str, ttl: int) -> bool:
    if not os.path.exists(path):
        return False
    return (time.time() - os.path.getmtime(path)) < ttl


def _decode_payload(text: str) -> dict:
    """TE 암호화 응답 → dict.

    text는 JSON 문자열(큰따옴표로 감싸진 base64 문자열)이다.
    """
    b64 = json.loads(text) if text.lstrip().startswith('"') else text
    raw = base64.b64decode(b64)
    xored = bytes(b ^ _OBF_KEY[i % len(_OBF_KEY)] for i, b in enumerate(raw))
    inflated = zlib.decompress(xored, 31)  # gzip (wbits=31)
    return json.loads(inflated.decode("utf-8"))


def _fetch_raw(span: str = "max") -> dict:
    """TE 차트 데이터 원문 fetch.

    span: 1y/3y/5y/10y/25y/max
    """
    version = datetime.now().strftime("%Y%m%d%H%M00")
    r = cfr.get(
        _API_URL,
        params={"span": span, "v": version},
        headers={
            "x-api-key": _API_KEY,
            "Referer": _REFERER,
            "Origin": "https://ko.tradingeconomics.com",
            "Accept": "application/json, text/plain, */*",
        },
        impersonate="chrome120",
        timeout=30,
    )
    r.raise_for_status()
    return _decode_payload(r.text)


def fetch_series(force: bool = False) -> pd.DataFrame:
    """월별 공작기계 수주 시계열 반환 (JPY Million, YoY % 포함).

    컬럼: date(datetime64), value_jpy_mn(float), yoy_pct(float, %).
    가장 오래된 연도(1983) 이후 전체 기간.
    """
    os.makedirs(CACHE_DIR, exist_ok=True)

    if not force and _is_cache_fresh(CACHE_FILE, CACHE_TTL_SEC):
        with open(CACHE_FILE, "r", encoding="utf-8") as f:
            cached = json.load(f)
    else:
        data = _fetch_raw(span="max")
        series = data[0]["series"][0]["serie"]
        # data 포인트: [value, unix_ms, ?, 'YYYY-MM-DD']
        points = [
            {"date": p[3], "value_jpy_mn": float(p[0])}
            for p in series["data"]
            if p[0] is not None and p[3]
        ]
        cached = {
            "fetched_at": datetime.now().isoformat(timespec="seconds"),
            "source": series.get("source", ""),
            "unit": series.get("unit", "JPY Million"),
            "frequency": series.get("frequency", "monthly"),
            "name": series.get("name", "JP Machine Tool Orders"),
            "data": points,
        }
        with open(CACHE_FILE, "w", encoding="utf-8") as f:
            json.dump(cached, f, ensure_ascii=False, indent=2)

    df = pd.DataFrame(cached["data"])
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date").reset_index(drop=True)
    # YoY(%): 12개월 전 대비 변화율
    df["yoy_pct"] = (df["value_jpy_mn"] / df["value_jpy_mn"].shift(12) - 1) * 100
    df.attrs["source"] = cached.get("source", "")
    df.attrs["unit"] = cached.get("unit", "JPY Million")
    df.attrs["fetched_at"] = cached.get("fetched_at", "")
    return df


if __name__ == "__main__":
    df = fetch_series(force=True)
    print("source:", df.attrs.get("source"))
    print("unit:", df.attrs.get("unit"))
    print("fetched_at:", df.attrs.get("fetched_at"))
    print("rows:", len(df), "range:", df["date"].min().date(), "~", df["date"].max().date())
    print(df.tail(5).to_string(index=False))
