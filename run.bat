@echo off
REM Navigate to project folder
cd /d F:\Markets\data-ingestion-tool

REM Activate the virtual environment
call .venv\Scripts\activate.bat

REM Run main.py
python main.py

REM Keep the window open (optional)
pause
