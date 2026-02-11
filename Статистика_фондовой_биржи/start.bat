@echo off
cd /d "C:\Users\admin\source\repos\Python-API-Postgres"
python generate-sales-data.py
python run.py
pause

