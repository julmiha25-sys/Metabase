#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Проверка сущствования файла sales-date.csv
import os
import pandas as pd
import configparser
import yfinance as yf
from datetime import datetime, timedelta
from pgdb import PGDatabase

config = configparser.ConfigParser()
dirname = os.path.dirname(__file__)
config.read(os.path.join(dirname, 'config.ini'))

COMPANIES=eval(config['Companies']['COMPANIES'])
SALES_PATH=config["Files"]["SALES_PATH"]
DATABASE_CREDS=config['Database']

# Создание пустого датафрейма на случай пустой выгрузки - не было курсов в выходные
sales_df=pd.DataFrame()
# Датафрейм с информацией по торгам 
if os.path.exists(SALES_PATH):
    sales_df=pd.read_csv(SALES_PATH)
    #print(sales_df)
    os.remove(SALES_PATH)

# Словарь с исторической информацией по торгам
historical_d={}    

for company in COMPANIES:
    df = yf.download(
        company,
        start=(datetime.today()-timedelta(days=3)).strftime("%Y-%m-%d"),
        end=datetime.today().strftime("%Y-%m-%d"),
        progress=False
    )
    # "Схлопываем" multi-level columns
    df.columns=df.columns.droplevel(1)  # Убираем уровень с тикерами
    df=df.reset_index()
    df['Ticker']=company  # Добавляем отдельный столбец
    historical_d[company]=df
    
# Вызываем класс PGDatabase из файла pgdb.py
database=PGDatabase(
    host=DATABASE_CREDS['HOST'],
    database=DATABASE_CREDS['DATABASE'],
    user=DATABASE_CREDS['USER'],
    password=DATABASE_CREDS['PASSWORD'],
    )

for i, row in sales_df.iterrows():
    query=f"insert into sales values ('{row['dt']}', '{row['company']}', '{row['transaction_type']}', '{row['amount']}')"
    database.post(query)

for company, data in historical_d.items():
    for i,row in data.iterrows():
        query=f"insert into stock values ('{row['Date']}', '{row['Ticker']}', '{row['Open']}', '{row['Close']}')"
        database.post(query)
