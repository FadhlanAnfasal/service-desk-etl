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

## 2. Architecture

Melalui link 

## 3. Tech Stack

| Layer | Tools |
|------|------|
| Source | API |
| ETL | Python (pandas, requests, SQLAlchemy, dotenv, tenacity) |
| Database | PostgreSQL + Adminer |
| Scheduling | Cron (Week 7), Airflow (Week 10) |
| BI | Power BI, Jupyter Notebook|

---

## 4. Project Repository

### A. Localhost

- logs
-- cron_etl.log;
-- etl_cron.log;
-- task_log.txt;
- notebook
-- analytics.ipynb;
-- exploration.ipynb;
-- oscar_age_male.csv;
- sql
-- 01_schema.sql;
-- 02_indexes.sql (opsional);
-- views_kpi.sql;
- src
-- __init__.py;
-- config.py;
-- dq.py;
-- extract.py;
-- load.py;
-- logger.py;
-- main.py;
-- test_etl_functions.py;
-- transform.py;
- venv
- .env
- .gitignore
- docker-compose.yml
- requirements.txt
- run_etl.bat

### B. DB WSL

- .pre-commit-config.yaml
- requirements.txt            
- dags
-- service_desk_etl_dag.py
- logs
-- cron_etl.log;
-- 'dag_id=service_desk_etl';   
-- dag_processor_manager;   
-- etl_cron.log;   
-- scheduler;   
-- task_log.txt;
- run_etl.bat
- .env         
- notebook         
- sql
- .git
- README.md
- docker-compose.yml          
- src
-- __init__.py;
-- config.py;
-- extract.py;  
-- logger.py;
-- test_etl_functions.py;
-- __pycache__;
-- dq.py;
-- load.py;
-- main.py;
-- transform.py;
- .github      
- RUNBOOK.md                
- etl_cron.log         
- pyproject.toml   
- venv
- .gitignore       
- install_log.txt      
- pytest.ini       

## 5. Airflow Structure

- airflow.cfg  
- airflow.db  
- docker-compose-airflow.yml  
- logs  
- plugins

## 6. Setup Dan Bagaimana Cara Menjalankan ETL

### A. Install 'requirements.txt'

- 'pip freeze > requirements.txt'

### B. Konfigurasi PostgreSQL dengan 'docker compose'

- 'docker compose up -d'

### C. Aplikasikan Data Warehouse Schema

Agar lebih mudah, koneksikan postgresql ke dbeaver dan masukkan semua kode '01_schema.sql' melalui dbeaver.

### D. Konfigurasi Virtual Environment

Digunakan untuk menyimpan semua library python yang dibutuhkan pada project ini dan library yang digunakan tidak bertabrakan dengan projcet lain.
Gunakan '.\venv\Scripts\activate' di windows dan 'Source venv/bin/activate' di wsl

### E. Run ETL Secara Manual

'python -m src.main'

## 7. Menjalankan ETL Otomatis dengan Cron dan Task Scheduler

### A. Cron

Melalui 'crontab -e' dan atur waktu nya sesuai keinginan pengguna baik per menit maupun per jam.

### B. Task Scheduler

Hubungkan project ini ke task scheduler, atur waktu penggunaannya, gunakan 'run_etl.bat' untuk mengintegrasikan project dengan task scheduler. Project akan berjalan setelah berhasil diintegrasikan.

## 8. Diagram KPI

Integrasikan seluruh kode views_kpi.sql ke dbeaver untuk menambahkan data tambahan ke database 

### A. Jupyter

Install jupyter dan kernel terlebih dahulu sebelum menjalankannya. Gunakan di Analytics.ipynb untuk membuat diagram dari setiap database yang dibuat. Integrasikan langsung dengan 'postgresql+psycopg2://etl_user:etl_pass@localhost:5432/servicedesk_dw' agar terhubung dengan database. Gunakan kernel sesuai yang telah dibuat dan gunakan kode python untuk mengatur diagramnya.

### B. Microsoft Power BI

Integrasikan dengan 'postgresql+psycopg2://etl_user:etl_pass@localhost:5432/servicedesk_dw' untuk mendapatkan setiap database nya. Ini lebih mudah untuk digunakan untuk mengatur database seperti apa yang ingin dibuat karena banyak opsi diagram serta pewarnaannya. Atur dan sesuaikan dengan komposisi pada kordinat x dan y sesuai yang pengguna inginkan.

## 9. Airflow

- Unduh library airflow di wsl. Gunakan db sebagai host karena airflow tidak bisa digunakan di localhost. 
- Buat database airflow dengan kode 'airflow db init'.
- Airflow hanya bisa digunakan di wsl.
- Gunakan service_desk_etl_dag.py untuk mendata setiap langkah yang akan dilakukan oleh airflow serta urutannya.
- Urutan dari proses melalui airflow yaitu menjalankan etl (run_etl) >> email smtp >> notifikasi group microsoft teams.
- Atur airflow.cfg untuk mengaktifkan email smtp.
- Pada 'airflow.cfg', masuk ke bagian [email] dan [smtp] untuk mengatur pengiriman notifikasi berhasil ke email user dari email smtp.
- Akun yang digunakan user harus mendapatkan password dari google dan untuk mendapatkan password tersebut akun harus terautentikasi dua faktor.
- Urutannya untuk mendaftarkan password yaitu Setelah autentikasi 2 faktor masuk ke App Password >> masukkan [App : Mail]; [Device : Other].
- Password akan didapatkan setelah memasukkan data terkait di App Password dan password tersebut berisi 16 karakter dan tidak bisa dibuat manual.
- Masukkan password tersebut pada bagian [smtp] di airflow.cfg.
- Password yang didapatkan kurang lebih sebagai contoh 'xxxx xxxx xxxx xxxx' namun yang akan diisi di airflow.cfg tidak boleh diberi spasi seperti ini 'xxxxxxxxxxxxxxxx'.
- Untuk microsoft teams harus menggunakan email kampus/kantor dan tidak bisa menggunakan email pribadi.
- Buat webhook di teams dengan masuk ke channel teams dan buat incoming webhook. 
- Beri nama sesuai yang user inginkan [etl_notifications] dan user akan mendapatkan url webhook.
- Unduh workflows dan atur agar workflow alert terkirim ke channel yang telah dibuat.
- Masukkan url ke 'service_desk_etl_dag.py' pada bagian [teams_notify], usahakan url dispasi menjadi beberapa bagian.
- Atur workflow nya melalui power automate agar terkirim ke teams dengan membuat flowchart nya dan integrasikan notifikasi nya dengan flowchart nya.
- Jangan lupa buat docker khusus airflow yang terdapat airflow scheduler dan airflow webserver.
- Jalankan airflow melalui docker khusus airflow dan user akan mendapatkan notifikasi status etl yang telah dijalankan.

## 10. Input Folder ETL ke Github

### A. WSL

- Install git library [pre commit, black, flake8].
- 'pre_commit_config.yaml' untuk menjalankan semua commit ke github.
- #noqa untuk menghindari pengecekan dari flake8.
- 'test_etl_functions.py' untuk mengecek apakah masing masing kode dari etl berjalan dan diuji dengan 'pytest'.
- 'pyproject.toml' untuk mengurutkan masing - masing library yang akan dijalankan.
- 'ci.yml' untuk proses continuous integration di github.
- Ambil ssh key dari github dan masukkan ke project agar terhubung tanpa harus login dengan username dan password.
- Kode yang akan dijalankan sebagai berikut:
-- pre-commit run --all-files;
-- git add .;
-- git commit -m " ";
-- git push;

### B. Git Bash

- git add .
- git commit -m " "
- git push origin main

## 11. Diagram db

Kode dari 01_schema.sql dan views_kpi.sql yang diubah dari sql menjadi dbms lalu dihubungkan satu per satu.

# Modul ini untuk developer yang ingin menggunakan ETL ini sebagai referensi

# Saran Update fitur selanjutnya

- Penambahan etl scrapping data pada apache airflow supaya bisa ditampilkan langsung ke dashboard power bi
- Penghapusan layer API node.js
- Penggunaan etl sepenuhnya dalam apache airflow sehingga tidak menggunakan google colab secara manual
