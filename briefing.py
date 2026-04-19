import anthropic
import pandas as pd
from dotenv import load_dotenv
from datetime import datetime
import os

load_dotenv()
client = anthropic.Anthropic()

def generate_briefing(stock_name, disclosures, stock_info):
    # 공시 목록 텍스트로 변환
    disc_text = "\n".join([f"- [{d['공시일']}] {d['제목']}" for _, d in disclosures.iterrows()]) if len(disclosures) > 0 else "최근 공시 없음"

    prompt = f"""
당신은 전문 주식 투자 분석가입니다.
아래 데이터를 바탕으로 {stock_name}의 오늘 투자 브리핑을 작성해주세요.

[시세 정보]
- 현재가: {stock_info.get('현재가', 'N/A')}
- PER: {stock_info.get('PER', 'N/A')}
- 시가총액: {stock_info.get('시가총액', 'N/A')}

[최근 공시]
{disc_text}

브리핑 형식:
1. 한줄 요약 (1문장)
2. 주요 이슈 (2~3개 bullet)
3. 투자 참고 포인트 (1~2문장)

※ 이 분석은 참고용이며 투자 결정은 본인 판단이 중요합니다.
"""

    message = client.messages.create(
        model="claude-sonnet-4-5",
        max_tokens=1000,
        messages=[{"role": "user", "content": prompt}]
    )
    return message.content[0].text

# CSV 파일 읽기
df_stocks = pd.read_csv("data/stocks.csv")
df_disc = pd.read_csv("data/disclosures.csv")

# 종목별 브리핑 생성
os.makedirs("data/briefings", exist_ok=True)
today = datetime.now().strftime("%Y-%m-%d")

for _, stock in df_stocks.iterrows():
    name = stock["종목명"]
    print(f"\n=== {name} 브리핑 생성 중 ===")

    # 해당 종목 공시만 필터
    disc = df_disc[df_disc["종목명"] == name]

    # 브리핑 생성
    briefing = generate_briefing(name, disc, stock)
    print(briefing)

    # 파일 저장
    filename = f"data/briefings/{today}_{name}.txt"
    with open(filename, "w", encoding="utf-8") as f:
        f.write(f"=== {name} 투자 브리핑 ({today}) ===\n\n")
        f.write(briefing)

    print(f"\n→ {filename} 저장 완료")