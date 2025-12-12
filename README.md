# Service Desk ETL Pipeline

## 1. Overview
Project ETL pipeline ini dibangun untuk memroses data yang telah diambil serta dibersihkan menjadi data warehouse yang dapat dijalankan baik manual maupun otomatis menggunakan windows task scheduler dan airflow. Adapun data diambil dari hasil scrapping hasil ulasan dari aplikasi MyMRTJ. Tujuan dilakukannya project ini adalah untuk mengetahui bagaimana respon pengguna aplikasi baik respon positif, netral, maupun negatif. Adapun data warehouse utama yaitu:

- Semua ulasan MyMRTJ 2019-2025
- Daftar rating
- Daftar sentimen

Serta 1 data warehouse cron yang menampilkan status jalannya aplikasi tiap dijalankan secara manual. Dan juga data warehouse untuk memonitor ulasan untuk KPI meliputi:

- Distribusi rating per tahun
- Distribusi rating per hari
- Distribusi masing masing rating per hari
- Distribusi sentimen berdasarkan rating
- Distribusi jumlah rating.

Folder dari operasi ETL Pipeline akan diunggah ke github serta di cek kualitas kode nya.

## 2. Architechture

Melalui link 

## 3. Tech Stack

Melalui link

## 4. Project Repository

### A. Localhost

- logs
-- cron_etl.log
-- etl_cron.log
-- task_log.txt
- notebook
-- analytics.ipynb
-- exploration.ipynb
-- oscar_age_male.csv
- sql
-- 01_schema.sql
-- 02_indexes.sql (opsional)
-- views_kpi.sql
- src
-- __init__.py
-- config.py
-- dq.py
-- extract.py
-- load.py
-- logger.py
-- main.py
-- test_etl_functions.py
-- transform.py
- venv
- .env
- .gitignore
- docker-compose.yml
- requirements.txt
- run_etl.bat

### B. DB WSL




## 5. Airflow Structure



## 6. Setup Dan Bagaimana Cara Menjalankan ETL

### A. Install 'requirements.txt'

- # pip freeze > requirements.txt

### B. Konfigurasi PostgreSQL dengan 'docker compose'

- # docker compose up -d

### C. Aplikasikan Data Warehouse Schema

Agar lebih mudah, koneksikan postgresql ke dbeaver dan masukkan semua kode 01_schema.sql melalui dbeaver.

### D. Konfigurasi Virtual Environment

Digunakan untuk menyimpan semua library python yang dibutuhkan pada project ini dan library yang digunakan tidak bertabrakan dengan projcet lain.
Gunakan '.\venv\Scripts\activate' di windows dan 'Source venv/bin/activate' di wsl

### E. Run ETL Secara Manual

# python -m src.main

## 7. Menjalankan ETL Otomatis dengan Cron dan Task Scheduler

### A. Cron

Melalui 'crontab -e' dan atur waktu nya sesuai keinginan pengguna baik per menit maupun per jam.

### B. Task Scheduler

Hubungkan project ini ke task scheduler, atur waktu penggunaannya, gunakan run_etl.bat untuk mengintegrasikan project dengan task scheduler. Project akan berjalan setelah berhasil diintegrasikan.

## 8. Diagram KPI

Integrasikan seluruh kode views_kpi.sql ke dbeaver untuk menambahkan data tambahan ke database 

### A. Jupyter

Install jupyter dan kernel terlebih dahulu sebelum menjalankannya. Gunakan di Analytics.ipynb untuk membuat diagram dari setiap database yang dibuat. Integrasikan langsung dengan 'postgresql+psycopg2://etl_user:etl_pass@localhost:5432/servicedesk_dw' agar terhubung dengan database. Gunakan kernel sesuai yang telah dibuat dan gunakan kode python untuk mengatur diagramnya.

### B. Microsoft Power BI

Integrasikan dengan 'postgresql+psycopg2://etl_user:etl_pass@localhost:5432/servicedesk_dw' untuk mendapatkan setiap database nya. Ini lebih mudah untuk digunakan untuk mengatur database seperti apa yang ingin dibuat karena banyak opsi diagram serta pewarnaannya. Atur dan sesuaikan dengan komposisi pada kordinat x dan y sesuai yang pengguna inginkan.

## 9. Airflow

Unduh library airflow di wsl. Gunakan db sebagai host karena airflow tidakbisa digunakan di localhost.

## 10. Input Folder ETL ke Github

### A. WSL



### B. Git Bash

- git add .
- git commit -m " "
- git push origin main

## 11. Diagram db


# Modul ini untuk developer yang ingin menggunakan ETL ini sebagai referensi