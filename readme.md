# 주식 지수 모니터링 시스템

실시간으로 KOSPI, KOSDAQ, KOSPI200 지수를 수집하고 웹 대시보드에서 확인할 수 있는 시스템입니다.

## 기능

- 📊 실시간 지수 데이터 수집 (1분 단위)
- 💾 SQLite 데이터베이스에 모든 데이터 저장
- 🌐 웹 대시보드 (Flask)
- 📈 실시간 차트 표시 (Chart.js)
- 📱 반응형 디자인

## 설치 및 실행

### 로컬 실행

```bash
# 가상환경 생성
python -m venv venv

# 가상환경 활성화 (Mac)
source venv/bin/activate

# 가상환경 활성화 (Windows)
venv\Scripts\activate

# 패키지 설치
pip install -r requirements.txt

# 앱 실행
python main.py
```

### 웹 접속

브라우저에서 `http://localhost:5000` 접속

## 배포 (Render)

1. GitHub에 푸시
2. Render.com에서 New Web Service 생성
3. GitHub 저장소 연결
4. 자동 배포

## 프로젝트 구조

```
stock-data-project/
├── main.py                 # 통합 앱 (Flask + Scheduler)
├── requirements.txt        # 패키지 의존성
├── Procfile               # 배포 설정
├── runtime.txt            # Python 버전
├── stock_data.db          # SQLite 데이터베이스
└── templates/
    └── index.html         # 웹 대시보드
```

## 기술 스택

- **Backend**: Python, Flask
- **Database**: SQLite
- **Frontend**: HTML, CSS, JavaScript, Chart.js
- **Scheduler**: Schedule
- **Hosting**: Render.com

## 라이선스

MIT License

## 개발자

주식 지수 모니터링 팀