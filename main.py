import yfinance as yf
import sqlite3
from datetime import datetime
import schedule
import time
import threading
from flask import Flask, jsonify, render_template

# Flask 앱 생성
app = Flask(__name__)

# 한국 주요 지수
indices = {
    'KOSPI': '^KS11',      # 코스피
    'KOSDAQ': '^KQ11',     # 코스닥
    'KOSPI200': '^KS200',  # 코스피200
}

def get_db_connection():
    """데이터베이스 연결"""
    conn = sqlite3.connect('stock_data.db')
    conn.row_factory = sqlite3.Row
    return conn

def print_status():
    """현재 상태 출력"""
    conn = sqlite3.connect('stock_data.db')
    cursor = conn.cursor()
    
    cursor.execute('''
        SELECT name, price, collected_at
        FROM indices
        WHERE (name, collected_at) IN (
            SELECT name, MAX(collected_at) FROM indices GROUP BY name
        )
        ORDER BY name
    ''')
    
    rows = cursor.fetchall()
    conn.close()
    
    print(f"\n[{datetime.now().strftime('%H:%M:%S')}] 현재 지수 상태:")
    for row in rows:
        print(f"  {row[0]:10} | {row[1]:>10,.2f} | 수집: {row[2]}")

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
    
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 데이터 수집 시작...")
    
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


def run_scheduler():
    """백그라운드에서 스케줄러 실행"""
    print("\n" + "=" * 60)
    print("📅 스케줄러 시작")
    print("   - 매 1분마다 데이터 수집")
    print("   - 매 10초마다 현재 상태 출력")
    print("=" * 60)
    
    # 매 1분마다 데이터 수집
    schedule.every(1).minutes.do(collect_and_save)
    
    # 매 10초마다 상태 출력
    schedule.every(10).seconds.do(print_status)
    
    # 앱 시작 시 바로 한 번 수집
    print("\n초기 데이터 수집...")
    collect_and_save()
    print_status()
    
    # 스케줄러 루프
    while True:
        schedule.run_pending()
        time.sleep(1)  # 1초마다 확인


# ============================================================
# Flask API 엔드포인트
# ============================================================

@app.route('/')
def index():
    """메인 페이지"""
    return render_template('index.html')

@app.route('/api/indices')
def get_indices():
    """모든 지수의 최신 데이터 반환"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # 각 지수별 최신 데이터만 가져오기
    cursor.execute('''
        SELECT name, ticker, price, collected_at
        FROM indices
        WHERE (name, collected_at) IN (
            SELECT name, MAX(collected_at) FROM indices GROUP BY name
        )
        ORDER BY name
    ''')
    
    rows = cursor.fetchall()
    conn.close()
    
    data = []
    for row in rows:
        data.append({
            'name': row['name'],
            'ticker': row['ticker'],
            'price': row['price'],
            'collected_at': row['collected_at']
        })
    
    return jsonify(data)

@app.route('/api/index/<index_name>')
def get_index_history(index_name):
    """특정 지수의 최근 데이터 반환 (최대 100개)"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute('''
        SELECT id, name, ticker, price, collected_at
        FROM indices
        WHERE name = ?
        ORDER BY collected_at DESC
        LIMIT 100
    ''', (index_name,))
    
    rows = cursor.fetchall()
    conn.close()
    
    if not rows:
        return jsonify({'error': '데이터를 찾을 수 없습니다.'}), 404
    
    data = []
    for row in rows:
        data.append({
            'id': row['id'],
            'name': row['name'],
            'ticker': row['ticker'],
            'price': row['price'],
            'collected_at': row['collected_at']
        })
    
    return jsonify(data)

@app.route('/api/stats')
def get_stats():
    """데이터베이스 통계 반환"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # 총 레코드 수
    cursor.execute('SELECT COUNT(*) as count FROM indices')
    total_records = cursor.fetchone()['count']
    
    # 지수별 레코드 수
    cursor.execute('''
        SELECT name, COUNT(*) as count
        FROM indices
        GROUP BY name
        ORDER BY name
    ''')
    
    stats = {}
    for row in cursor.fetchall():
        stats[row['name']] = row['count']
    
    conn.close()
    
    return jsonify({
        'total_records': total_records,
        'by_index': stats
    })


if __name__ == '__main__':
    print("=" * 60)
    print("🚀 통합 주식 지수 모니터링 시스템 시작")
    print("=" * 60)
    
    # 스케줄러를 백그라운드 스레드에서 실행
    scheduler_thread = threading.Thread(target=run_scheduler, daemon=True)
    scheduler_thread.start()
    
    # Flask 앱 실행
    print("\n" + "=" * 60)
    print("🌐 Flask 웹 서버 시작")
    print("=" * 60)
    print("📍 주소: http://localhost:5000")
    print("🛑 중지하려면 Ctrl+C 를 누르세요.")
    print("=" * 60)
    
    app.run(debug=False, host='0.0.0.0', port=5000)