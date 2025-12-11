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



## 3. Tech Stack



## 4. Project Repository



## 5. Airflow Structure



## 6. Setup Dan Bagaimana Cara Menjalankan ETL

### A. Install 'requirements.txt'



### B. Konfigurasi PostgreSQL dengan 'docker compose'



### C. Aplikasikan Data Warehouse Schema



### D. Konfigurasi Virtual Environment



### E. Run ETL Secara Manual



## 7. Menjalankan ETL Otomatis dengan Cron dan Task Scheduler

### A. Cron



### B. Task Scheduler



## 8. Diagram KPI

### A. Jupyter



### B. Microsoft Power BI



## 9. Airflow



## 10. Input Folder ETL ke Github

### A. WSL



### B. Git Bash

- git add .
- git commit -m " "
- git push origin main

# Modul ini untuk developer yang ingin menggunakan ETL ini sebagai referensi