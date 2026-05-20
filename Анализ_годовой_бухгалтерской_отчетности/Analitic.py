# -*- coding: utf-8 -*-
# Импорт библиотеки для выполнения HTTP-запросов к API
import requests
import pandas as pd
from datetime import datetime, timedelta
# Импорт библиотеки для работы с переменными окружения
from dotenv import load_dotenv
# Импорт библиотеки для подключения к PostgreSQL
from pgdb import PGDatabase
import logging
import os
import glob
import pandas as pd

# Настройки логирования
os.makedirs("/root/temz/logs", exist_ok=True)
log_filename = f"/root/temz/logs/balance.log"
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_filename, encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)
logger.info("ЗАПУСК ETL ПРОЦЕССА")
logger.info(f"Лог файл: {log_filename}")

# Чтение параметров подключения из переменных окружения
DATABASE_CREDS = {
    'HOST': os.getenv('DB_HOST', 'localhost'),
    'DATABASE': os.getenv('DB_NAME', 'postgres'),
    'USER': os.getenv('DB_USER', 'postgres'),
    'PASSWORD': os.getenv('DB_PASSWORD', '')
}

# Функция для единоразового накопления истории
def find_date():
    df=pd.read_excel('Годовая_бухгалтерская_отчетность.xlsx', engine='openpyxl')
    df.columns = ['Отчетный период','Нематериальные активы','Нематериальные активы в организации','Основные средства',
              'ОС пригодные к использованию', 'Незаконченное строительство','Отложенные налоговые активы','Запасы',
              'Сырье и материалы','Готовая продукция','Товары','Незавершенное производство','Прочие запасы',
              'НДС по приобретенным ценностям','Дебиторская задолженность', 'Дебиторские расчеты с покупателями и заказчиками',
              'Дебиторские расчеты с поставщиками и подрадчиками','Дебиторские расчеты по налогам и сборам',
              'Дебиторские расчеты по социальному страхованию и обеспечению','Прочая дебиторская задолженность',
              'Денежные средства и денежные эквиваленты','Касса','Расчетные счета','Валютные счета','Депозитные счета',
              'Прочие оборотные активы','Расходы будущих периодов','Уставной капитал','Добавочный капитал','Резервный капитал',
              'Нераспределенная прибыль','Отложенные налоговые обязательства','Кредиторская задолженность',
              'Кредиторские расчеты с поставщиками и подрядчиками','Кредиторские расчеты с покупателями и заказчиками',
              'Кредиторские расчеты по налогам и сборам','Расчеты по социальному страхованию и обеспечению',
              'Расчеты с персоналом по оплате труда','Расчеты с разными дебиторами и кредиторами','Оценочные обязательства']
    print(f"\nВсего собрано записей: {len(df)}")
    logger.info(f"Всего собрано записей: {len(df)}")
    return df

# Функция для загрузки данных в БД
def load_dataframe_to_db(df, database, table_name='sales'):
    # Проверка датафрейма на пустоту
    if df.empty:
        print("DataFrame пуст, загрузка отменена")
        logger.warning("DataFrame пуст, загрузка отменена")
        return
    # Получение списка с названиями колонок
    columns = df.columns.tolist()
    # Соединение их в строку с ,
    columns_str = ', '.join([f'"{col}"' for col in columns]) 
    # Создание плейсхолдера - заполнителя для ячейки
    placeholders = ', '.join(['%s'] * len(columns))
    query = f"INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders})"
    # Преобразование датафрейма в список кортежей (каждый — одна строка данных)
    data_tuples = [tuple(row) for row in df.to_numpy()]
    # Вставка всех данных одним запросом - вызов функции массовой вставки данных из pgdb.py
    print(f"Загрузка {len(data_tuples)} строк в таблицу {table_name}...")
    logger.info(f"Загрузка {len(data_tuples)} строк в таблицу {table_name}...")
    try:
        database.post_many(query, data_tuples)
        print(f"Загрузка завершена")
        logger.info(f"Загрузка завершена")
    except Exception as e:
        logger.error(f"Ошибка при загрузке данных: {e}")
        raise

# Функция для первоначального создания таблицы 
def create_table(database, df, table_name='balance'):
    # Маппинг типов данных pandas
    dtype_mapping = {
        'int64': 'INTEGER',
        'float64': 'FLOAT',
        'object': 'TEXT',
        'datetime64[ns]': 'TIMESTAMP',
        'bool': 'BOOLEAN'
    }
    # Создание колонок из DataFrame
    columns_def = []
    for col in df.columns:
        # Получение типа данных колонки в виде строки
        col_type = str(df[col].dtype)
        # Нахождение соотвествия этому типу - типа БД
        pg_type = dtype_mapping.get(col_type, 'TEXT')
        # Добавление в список колонок новой колонки (название + тип)
        columns_def.append(f'"{col}" {pg_type}')
    # Создание таблицы
    create_query = f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join(columns_def)})"
    try:
        database.post(create_query)
        print(f"Таблица {table_name} создана с колонками:")
        logger.info(f"Таблица {table_name} создана с колонками:")
        for col in df.columns:
            print(f"  - {col}")
            logger.info(f"  - {col}")
    except Exception as e:
        logger.error(f"Ошибка при создании таблицы: {e}")
        raise

# Подключение к БД
logger.info("Подключение к базе данных")
database = PGDatabase(
    host=DATABASE_CREDS['HOST'],
    database=DATABASE_CREDS['DATABASE'],
    user=DATABASE_CREDS['USER'],
    password=DATABASE_CREDS['PASSWORD'],
)
logger.info("Подключение к БД установлено")

# Сбор данных
print("Сбор данных")
logger.info("Начало сбора данных из API")
# Вызов функции для сбора данных
df = find_date()
# Создание таблицы и загрузка данных
if not df.empty:
    logger.info(f"Получено {len(df)} строк данных. Начинаем загрузку в БД...")
    # Вызов функции создания таблицы balance в БД
    create_table(database, df, 'balance')
    # Вызов функции загрузки данных из датафрейма в БД
    load_dataframe_to_db(df, database, 'balance')
    logger.info("ETL процесс завершен успешно")
else:
    print("Нет данных для загрузки")
    logger.warning("Нет данных для загрузки")