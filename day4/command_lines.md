# Airflow Installation with Docker

## Instrallation Steps

✅ ดาวโหลดไฟล์ docker-compose.yaml ตามลิงค์
```
https://drive.google.com/drive/folders/1P3W4Nx-9C1M0u6roFMUWGNdAeNIx_C3_
```
✅ บันทึกที่ Desktop\ETL\airflow

✅ ไปที่ Docker Terminal

✅ ไปยัง folder บน Desktop\ETL\airflow

✅ พิมพ์คำสั่ง mkdir dags logs plugins
```
mkdir dags logs plugins
```
✅ สั่ง docker-compose up airflow-init
```
docker-compose up airflow-init
```
✅ รอจนเสร็จสิ้น สังเกตข้อความ exited with code 0

✅ เริ่ม airflow สั่ง docker-compose up -d
```
docker-compose up -d
```
✅ หยุด airflow สั่ง docker-compose down
```
docker-compose down
```
✅ ถอนติดตั้ง docker-compose down –volumes –rmi all
```
docker-compose down –volumes –rmi all
```
