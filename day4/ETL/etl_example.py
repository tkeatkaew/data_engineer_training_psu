from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator

# กำหนดค่า default arguments สำหรับ DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# สร้าง DAG
dag = DAG(
    'simple_etl_example',
    default_args=default_args,
    description='ตัวอย่าง ETL Pipeline อย่างง่าย',
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags=['example', 'etl'],
)

# ฟังก์ชัน Python สำหรับ task
def extract_data():
    """ฟังก์ชันสำหรับดึงข้อมูล"""
    print("กำลังดึงข้อมูลจากแหล่งข้อมูล...")
    # จำลองการดึงข้อมูล
    data = {"records": 100, "source": "database"}
    print(f"ดึงข้อมูลสำเร็จ: {data}")
    return data

def transform_data(**context):
    """ฟังก์ชันสำหรับแปลงข้อมูล"""
    # รับข้อมูลจาก task ก่อนหน้า
    ti = context['ti']
    extracted_data = ti.xcom_pull(task_ids='extract_task')
    
    print("กำลังแปลงข้อมูล...")
    # จำลองการแปลงข้อมูล
    transformed_data = {
        "processed_records": extracted_data["records"] * 2,
        "status": "transformed"
    }
    print(f"แปลงข้อมูลสำเร็จ: {transformed_data}")
    return transformed_data

def load_data(**context):
    """ฟังก์ชันสำหรับโหลดข้อมูล"""
    # รับข้อมูลจาก task ก่อนหน้า
    ti = context['ti']
    transformed_data = ti.xcom_pull(task_ids='transform_task')
    
    print("กำลังโหลดข้อมูล...")
    # จำลองการโหลดข้อมูล
    print(f"โหลดข้อมูลสำเร็จ: {transformed_data}")
    return "โหลดข้อมูลเสร็จสิ้น"

# สร้าง Task
start_task = DummyOperator(
    task_id='start',
    dag=dag,
)

extract_task = PythonOperator(
    task_id='extract_task',
    python_callable=extract_data,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_task',
    python_callable=transform_data,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_task',
    python_callable=load_data,
    dag=dag,
)

# Task สำหรับแสดงข้อมูลระบบ
system_info_task = BashOperator(
    task_id='system_info',
    bash_command='echo "วันที่: $(date)" && echo "ผู้ใช้: $(whoami)" && echo "ไดเรกทอรีปัจจุบัน: $(pwd)"',
    dag=dag,
)

end_task = DummyOperator(
    task_id='end',
    dag=dag,
)

# กำหนดลำดับการทำงานของ Task
start_task >> extract_task >> transform_task >> load_task >> end_task
start_task >> system_info_task >> end_task
