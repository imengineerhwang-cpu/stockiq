import requests
from bs4 import BeautifulSoup

def get_stock_info(stock_code):
    url = f"https://finance.naver.com/item/main.naver?code={stock_code}"
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"}
    r = requests.get(url, headers=headers)
    soup = BeautifulSoup(r.text, "html.parser")

    # 현재가
    price = soup.select_one("p.no_today span.blind")

    # 텍스트 줄 목록
    lines = [l.strip() for l in soup.get_text().splitlines() if l.strip()]

    # PER — "PER(배)" 다음 줄
    per = "N/A"
    for i, line in enumerate(lines):
        if line == "PER(배)" and i+1 < len(lines):
            per = lines[i+1]
            break

    # 시가총액 — "시가총액" → "5,326" → "억원" 패턴
    marcap = "N/A"
    for i, line in enumerate(lines):
        if line == "시가총액" and i+2 < len(lines) and lines[i+2] == "억원":
            marcap = lines[i+1] + "억원"
            break

    return {
        "종목코드": stock_code,
        "현재가": price.text.strip() if price else "N/A",
        "PER": per,
        "시가총액": marcap,
    }

result = get_stock_info("019210")
for k, v in result.items():
    print(f"{k}: {v}")