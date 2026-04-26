"""Almonty 텅스텐 (APT) 가격 히스토리 수집.

소스: https://almonty.com/tungsten-history/
  - 페이지가 Ninja Tables 플러그인의 AJAX 엔드포인트로 동적 로드
  - admin-ajax.php?action=wp_ajax_ninja_tables_public_action&table_id=1768
  - FastMarkets 주간 APT(미국) 가격 (USD/MTU, low/high/average)

저장: data/macro/tungsten/almonty_apt.json
"""
import os
import re
import json
import requests
from datetime import datetime

URL = (
    "https://almonty.com/wp-admin/admin-ajax.php"
    "?action=wp_ajax_ninja_tables_public_action"
    "&table_id=1768"
    "&target_action=get-all-data"
    "&default_sorting=old_first"
    "&skip_rows=0&limit_rows=0"
)
OUT = "data/macro/tungsten/almonty_apt.json"


def _parse_price(s: str) -> float | None:
    """'$2,800.00' → 2800.0 / '-' or '' → None"""
    if not s:
        return None
    s = s.strip().replace("$", "").replace(",", "")
    if not s or s in ("-", "—"):
        return None
    try:
        return float(s)
    except ValueError:
        return None


def _parse_date(s: str) -> str | None:
    """'17-Apr-26' → '2026-04-17' ISO 8601."""
    s = s.strip()
    for fmt in ("%d-%b-%y", "%d-%B-%y", "%Y-%m-%d"):
        try:
            return datetime.strptime(s, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None


def fetch() -> list[dict]:
    """Almonty 텅스텐 가격 row 리스트 반환 (오래된 → 최신 순).

    각 row 형식:
    {
      "date": "2025-06-06", "low": 410.0, "high": 450.0,
      "average": 430.0, "currency": "USD", "unit": "MTU APT",
      "source": "FastMarkets via Almonty",
    }
    """
    hdrs = {
        "User-Agent": "Mozilla/5.0",
        "Referer": "https://almonty.com/tungsten-history/",
    }
    raw = requests.get(URL, headers=hdrs, timeout=30).json()
    rows = []
    for item in raw:
        v = item.get("value", {})
        d = _parse_date(v.get("date", ""))
        if not d:
            continue
        rows.append({
            "date": d,
            "low":     _parse_price(v.get("low", "")),
            "high":    _parse_price(v.get("high", "")),
            "average": _parse_price(v.get("average", "")),
            "currency": "USD",
            "unit": "MTU APT",
            "source": "FastMarkets via Almonty",
        })
    rows.sort(key=lambda r: r["date"])
    return rows


def save(rows: list[dict], path: str = OUT) -> str:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    payload = {
        "fetched_at": datetime.now().isoformat(timespec="seconds"),
        "source_url": "https://almonty.com/tungsten-history/",
        "rows": rows,
    }
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    return path


if __name__ == "__main__":
    rows = fetch()
    p = save(rows)
    print(f"saved: {p}, rows={len(rows)}")
    if rows:
        print(f"  earliest: {rows[0]['date']}  avg=${rows[0]['average']}/MTU")
        print(f"  latest:   {rows[-1]['date']}  avg=${rows[-1]['average']}/MTU")
        # 25년 6월 이후만 출력
        cutoff = "2025-06-01"
        recent = [r for r in rows if r["date"] >= cutoff]
        print(f"\n=== 2025-06 이후 ({len(recent)}개) ===")
        print(f"{'date':<12} {'low':>10} {'high':>10} {'avg':>10}")
        for r in recent:
            print(f"{r['date']:<12} ${r['low']:>9,.2f} ${r['high']:>9,.2f} ${r['average']:>9,.2f}")
