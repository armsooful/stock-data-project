import yfinance as yf
import sqlite3
from datetime import datetime
import schedule
import time

# 한국 주요 지수
indices = {
    'KOSPI': '^KS11',      # 코스피
    'KOSDAQ': '^KQ11',     # 코스닥
    'KOSPI200': '^KS200',  # 코스피200
}

def collect_and_save():
    """데이터를 수집하고 데이터베이스에 저장하는 함수"""
    
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
    
    print(f"\n[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 데이터 수집 시작...")
    
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
            
            print(f"  ✓ {name}: {current_price:,.2f}")
            
        except Exception as e:
            print(f"  ✗ {name} 실패: {str(e)}")
    
    # 데이터베이스 저장
    conn.commit()
    conn.close()
    
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 데이터 수집 완료!")


# 스케줄 설정
print("=" * 60)
print("주식 지수 데이터 자동 수집 스케줄러")
print("=" * 60)
print("설정: 매시간 정각에 데이터 수집")
print("중지하려면 Ctrl+C 를 누르세요.")
print("=" * 60)

# 매시간 정각에 실행
schedule.every().hour.at(":00").do(collect_and_save)

# 스케줄러 실행
try:
    while True:
        schedule.run_pending()
        time.sleep(60)  # 60초마다 확인
except KeyboardInterrupt:
    print("\n\n스케줄러 중지됨.")