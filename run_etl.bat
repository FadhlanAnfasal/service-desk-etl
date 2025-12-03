@echo off
cd /d "C:\Users\ACER\Documents\service-desk-etl"

"%cd%\venv\Scripts\python.exe" -m src.main >> logs\task_log.txt 2>&1

exit /b 0
