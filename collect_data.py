import yfinance as yf
import pandas as pd
from datetime import datetime

# 한국 주요 지수
indices = {
    'KOSPI': '^KS11',      # 코스피
    'KOSDAQ': '^KQ11',     # 코스닥
    'KOSPI200': '^KS200',  # 코스피200
}

print("=" * 50)
print("한국 주식 지수 데이터 수집")
print("=" * 50)

for name, ticker in indices.items():
    try:
        # 지수 데이터 다운로드
        data = yf.download(ticker, period='1d', progress=False, auto_adjust=True)
        
        # 최신 데이터 추출
        latest = data.iloc[-1]
        current_price = float(latest['Close'].iloc[0]) if hasattr(latest['Close'], 'iloc') else float(latest['Close'])
        
        print(f"\n{name} ({ticker})")
        print(f"  현재가: {current_price:,.2f}")
        print(f"  수집 시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
    except Exception as e:
        print(f"\n{name} 데이터 수집 실패: {str(e)}")

print("\n" + "=" * 50)