# n8n Installation with Docker

## Instrallation Steps
✅ ไปที่ Docker terminal
cd ไปที่ Desktop\ETL\
```
cd D:\DataEngineerTraining\day3\data_engineer_training_psu\day4\etl\n8n
```
สั่ง mkdir n8n
✅ เริ่มติดตั้ง n8n สั่ง
```
 docker run -it --rm --name n8n -p 5678:5678 -v D:\DataEngineerTraining\day3\data_engineer_training_psu\day4\etl\n8n:/home/node/.n8n docker.n8n.io/n8nio/n8n

```
✅ รอดำเนินการจนแล้วเสร็จ
เข้าใช้งานที่ browser ลิงค์ localhost:5678
```
localhost:5678
```
✅ กำหนด login แนะนำให้ใช้ gmail

✅ ดำเนินการจนสำเร็จ ยินดีด้วย :)
หากต้องการใช้งานผ่าน internet ได้ติดตั้ง ngrok
