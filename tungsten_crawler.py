"""KOMIS 텅스텐 가격 데이터 수집.

소스: https://www.komis.or.kr/Komis/RsrcPrice/MinorMetals
   - getMnrlPrcByMnrkndUnqCd AJAX (HP002, MNRL0018, 798-APT 88.5% EXW China)
   - 일별 데이터, 2010-07-02 ~ 현재, USD/kg

캐시: data/tungsten/komis_apt.json (12h TTL)

가격기준 코드 (참고):
  796 = Ferro-tungsten 75%
  798 = Tungsten APT 88.5% min EXW China  ← 사용
  799 = Tungsten Oxide 99.95%
  800 = Tungsten Carbide 99.8%
  816 = Tungsten Ore 55%
"""
from __future__ import annotations

import json
import os
import time
from datetime import datetime

import pandas as pd
from curl_cffi import requests as cfr

CACHE_DIR = "data/tungsten"
CACHE_FILE = os.path.join(CACHE_DIR, "komis_apt.json")
CACHE_TTL_SEC = 12 * 60 * 60  # 12시간

_PAGE_URL = "https://www.komis.or.kr/Komis/RsrcPrice/MinorMetals"
_DATA_URL = "https://www.komis.or.kr/Komis/RsrcPrice/ajax/getMnrlPrcByMnrkndUnqCd"
_FORM_DATA = "HP000=HP002&srchMnrkndUnqCd=MNRL0018&srchPrcCrtr=798"


def _is_cache_fresh(path: str, ttl: int) -> bool:
    if not os.path.exists(path):
        return False
    return (time.time() - os.path.getmtime(path)) < ttl


def _fetch_raw() -> dict:
    """KOMIS AJAX 호출 → 텅스텐 APT 원시 응답."""
    session = cfr.Session()
    # 세션 쿠키(JSESSIONID) 획득
    session.get(_PAGE_URL, impersonate="chrome120", timeout=20)
    headers = {
        "X-Requested-With": "XMLHttpRequest",
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "Referer": _PAGE_URL,
        "Accept": "application/json, text/javascript, */*; q=0.01",
    }
    r = session.post(_DATA_URL, data=_FORM_DATA, headers=headers, timeout=45)
    r.raise_for_status()
    return r.json()


def fetch_series(force: bool = False, resample: str = "M") -> pd.DataFrame:
    """텅스텐 APT 가격 시계열 반환.

    Parameters
    ----------
    force : bool
        캐시 무시하고 재수집.
    resample : str
        'D' = 일별(원본), 'W' = 주별, 'M' = 월별 평균(기본).

    컬럼: date(datetime64), price_usd_kg(float), yoy_pct(float %)
    attrs: source, unit, fetched_at, latest_price, latest_yoy_pct, latest_mom_pct
    """
    os.makedirs(CACHE_DIR, exist_ok=True)

    if not force and _is_cache_fresh(CACHE_FILE, CACHE_TTL_SEC):
        with open(CACHE_FILE, "r", encoding="utf-8") as f:
            cached = json.load(f)
    else:
        raw = _fetch_raw()
        std = raw.get("dataAvg", {}).get("stdMap", {})
        points = raw.get("data", {}).get("defaultMnrl", [])
        info = std.get("INFO", {})
        crtrymd = std.get("CRTRYMD", {})

        # 역순(최신→과거) 정렬된 항목 → 유효한 가격만 추출
        records = [
            {"date": p["crtrYmd"], "price_usd_kg": float(p["cmercPrc"])}
            for p in points
            if p.get("cmercPrc") and float(p["cmercPrc"]) > 0
        ]
        cached = {
            "fetched_at": datetime.now().isoformat(timespec="seconds"),
            "source": "KOMIS · " + info.get("prcCrtr", "APT 88.5% EXW China"),
            "unit": info.get("prcUnitCdNm", "USD") + "/" + info.get("weigUnitCd", "kg"),
            "latest_date": crtrymd.get("crtrYmd", ""),
            "latest_price": float(crtrymd.get("cmercPrc", 0)),
            "latest_yoy_pct": float(std.get("YEAR", {}).get("flctnPrcnt", 0)),
            "latest_mom_pct": float(std.get("MONTH", {}).get("flctnPrcnt", 0)),
            "data": records,
        }
        with open(CACHE_FILE, "w", encoding="utf-8") as f:
            json.dump(cached, f, ensure_ascii=False, indent=2)

    df = pd.DataFrame(cached["data"])
    df["date"] = pd.to_datetime(df["date"], format="%Y%m%d")
    df = df.sort_values("date").reset_index(drop=True)

    # 리샘플링
    if resample != "D":
        freq = {"W": "W", "M": "ME"}.get(resample, "ME")
        df = df.set_index("date")["price_usd_kg"].resample(freq).mean().dropna().reset_index()
        df.columns = ["date", "price_usd_kg"]

    # YoY(%) 계산 (월별: 12개월 전, 일별/주별: 365일 전 근사치)
    if resample == "M":
        df["yoy_pct"] = (df["price_usd_kg"] / df["price_usd_kg"].shift(12) - 1) * 100
    else:
        df["yoy_pct"] = (df["price_usd_kg"] / df["price_usd_kg"].shift(52 if resample == "W" else 365) - 1) * 100

    # attrs
    df.attrs.update({
        "source": cached["source"],
        "unit": cached["unit"],
        "fetched_at": cached["fetched_at"],
        "latest_price": cached["latest_price"],
        "latest_yoy_pct": cached["latest_yoy_pct"],
        "latest_mom_pct": cached["latest_mom_pct"],
        "latest_date": cached["latest_date"],
    })
    return df


if __name__ == "__main__":
    df = fetch_series(force=True, resample="M")
    print("source:", df.attrs["source"])
    print("unit:", df.attrs["unit"])
    print("rows:", len(df), "range:", df["date"].min().date(), "~", df["date"].max().date())
    print("latest:", df.attrs["latest_date"], df.attrs["latest_price"], "USD/kg",
          f"YoY {df.attrs['latest_yoy_pct']:+.1f}%")
    print(df.tail(8).to_string(index=False))
