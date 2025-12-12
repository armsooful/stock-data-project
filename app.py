from flask import Flask, jsonify, render_template
import sqlite3
from datetime import datetime

app = Flask(__name__)

def get_db_connection():
    """데이터베이스 연결"""
    conn = sqlite3.connect('stock_data.db')
    conn.row_factory = sqlite3.Row
    return conn

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
    print("Flask 웹 서버 시작")
    print("=" * 60)
    print("주소: http://localhost:5000")
    print("중지하려면 Ctrl+C 를 누르세요.")
    print("=" * 60)
    app.run(debug=True, host='0.0.0.0', port=5000)