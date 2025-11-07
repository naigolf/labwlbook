# ใช้ Python เวอร์ชัน 3.11
FROM python:3.11-slim

# ตั้ง working directory
WORKDIR /app

# คัดลอก requirements.txt และติดตั้ง dependencies
COPY requirements.txt .
RUN pip install -r requirements.txt

# คัดลอกโค้ดทั้งหมดเข้า container
COPY . .

# สั่งให้รัน app.py ตอนเริ่ม container
CMD ["python", "app.py"]
