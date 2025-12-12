@echo off
echo.
echo ================================================
echo 주식 지수 모니터링 시스템 시작
echo ================================================
echo.

cd /d %~dp0

REM 가상환경 활성화
call venv\Scripts\activate.bat

REM 통합 앱 실행
python main.py

pause