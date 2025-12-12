import yfinance as yf
import sqlite3
from datetime import datetime

# 한국 주요 지수
indices = {
    'KOSPI': '^KS11',      # 코스피
    'KOSDAQ': '^KQ11',     # 코스닥
    'KOSPI200': '^KS200',  # 코스피200
}

# SQLite 데이터베이스 연결
conn = sqlite3.connect('stock_data.db')
cursor = conn.cursor()

# 테이블 생성 (첫 실행 시)
cursor.execute('''
    CREATE TABLE IF NOT EXISTS indices (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL,
        ticker TEXT NOT NULL,
        price REAL NOT NULL,
        collected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
''')

print("=" * 60)
print("한국 주식 지수 데이터 수집 및 저장")
print("=" * 60)

for name, ticker in indices.items():
    try:
        # 지수 데이터 다운로드
        data = yf.download(ticker, period='1d', progress=False, auto_adjust=True)
        
        # 최신 데이터 추출
        latest = data.iloc[-1]
        current_price = float(latest['Close'].iloc[0]) if hasattr(latest['Close'], 'iloc') else float(latest['Close'])
        
        # 데이터베이스에 저장
        cursor.execute('''
            INSERT INTO indices (name, ticker, price, collected_at)
            VALUES (?, ?, ?, ?)
        ''', (name, ticker, current_price, datetime.now()))
        
        print(f"\n✓ {name} ({ticker})")
        print(f"  현재가: {current_price:,.2f}")
        print(f"  저장 시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
    except Exception as e:
        print(f"\n✗ {name} 데이터 수집 실패: {str(e)}")

# 데이터베이스 저장
conn.commit()

# 저장된 데이터 확인
print("\n" + "=" * 60)
print("데이터베이스에 저장된 최근 10개 기록")
print("=" * 60)

cursor.execute('''
    SELECT id, name, ticker, price, collected_at 
    FROM indices 
    ORDER BY collected_at DESC 
    LIMIT 10
''')

for row in cursor.fetchall():
    print(f"ID: {row[0]} | {row[1]:10} | {row[2]:10} | 가격: {row[3]:>10,.2f} | {row[4]}")

# 연결 종료
conn.close()

print("\n" + "=" * 60)
print("✓ 데이터 수집 및 저장 완료!")
print("  데이터베이스 파일: stock_data.db")
print("=" * 60)