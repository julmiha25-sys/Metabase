**Проект для загрузки данных с API Фондовой биржи (https://yahoo.com/) и загрузки в БД Postgres.**

Конфигурационный файл config.ini (путь к файлу с выгрузкой, список компаний, настройки БД).

Класс-коннектор pgdb.py  для работы с БД.

Скрипт для генерации датасета generate-sales-data.py 

Файл со списком установленных библиотек requirements.txt

Файл run.py - считывание данных с датасета в датафрейм,  обработка и загрузка в БД.

__**1. На удаленном сервере (я делала на виртуальном сервере Ubuntu - Oracle Virtual Box) клонирование удаленного репозитория:**__

sudo apt update

sudo apt install git

git –version

git clone https://github.com/julmiha25-sys/Python.git ~/Python

__**2. Содание и активация виртуального окружения:**__
   
sudo apt install python3.12-venv

python3 -m venv venv 

source venv/bin/activate  

cd Python_API_Postgres 

pip install -r requirements.txt – устанвка необходимых зависимостей

python3 generate-sales-data.py  - запускаем генератор датасета (если выходит ошибка подключения к БД, то нужно: 

su - postgres

psql -U postgres

postgres=# ALTER USER postgres WITH PASSWORD '123456';   - зададим пароль как в config.ini

exit

ls -l – появился файл sales-data.csv

git status  -  показывает изменения в репозитории (появился новый файл)

теперь возникает ошибка – отсутствие БД finance

__**3.Подключение к удаленному серверу через локальный DBeaver и создание БД finance**__ 

<img width="844" height="300" alt="image" src="https://github.com/user-attachments/assets/577d15a3-c276-4619-8da1-53e819508fd0" />

Генерация SQL-скрипта для создания таблиц sales, stock и запуск их в SQL-редакторе для новой БД finance на удаленном сервере.

<img width="793" height="699" alt="image" src="https://github.com/user-attachments/assets/d72b26e4-679c-4d94-a623-51e5a613b831" />

Запускаем  на уделенном сервере Ubuntu  python3 run.py

Скрипт отработал – данные в таблицах появились.

git status  - просмотр изменений

git stash –include-untracker   - сохраение всех изменений

git add .    – добавление всех изменений в текущей директории в staging area

git commit –m “содержимое_комментария»   - добавление комментария

git push origin master - отправка изменений на удаленный репозиторий

git pull – скачивание изменений с Git-а

__**4.Настройка crontab на удаленном сервере**__

EDITOR=nano crontab -e

*/1 * * * * /home/vboxuser/Python/venv/bin/python  /home/vboxuser/Python/Python_API_Postgres/generate-sales-data.py >> /home/vbo> -  Генерация датасета - каждую минуту

01 7 * * * /home/vboxuser/Python/venv/bin/python  /home/vboxuser/Python/Python_API_Postgres/run.py - Загрузка данных в БД - ежедневно в 7.01

<img width="844" height="437" alt="image" src="https://github.com/user-attachments/assets/4c178ba2-d11f-4ed0-a5ad-f4c0bbc3c87d" />

<img width="916" height="384" alt="image" src="https://github.com/user-attachments/assets/ff7a6220-8310-47a0-8082-e98e015e7b68" />

__**5.Настройка веб-сервера и Metabase**__

1.Установка Docker (предварительно установить необходимые зависимости, добавить GPG ключ Docker, добавить официальный репозиторий Docker в список источников пакетов Ubuntu).

sudo apt install docker.io

2.Установка, разворачивание, запуск Metabase.

sudo docker pull metabase/metabase:latest

sudo docker run -d -p 3000:3000 --name metabase metabase/metabase

udo docker ps –a

3.Установка и запуск веб-сервера Nginx.

udo apt install nginx

sudo systemctl start nginx


