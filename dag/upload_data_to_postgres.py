from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from datetime import date, datetime, timedelta
import pendulum

local_tz = pendulum.timezone("Asia/Bangkok")

# ฟังก์ชันสำหรับการโหลดข้อมูลเข้า PostgreSQL
def upload_data_to_postgres(**kwargs):
    pg_hook = PostgresHook(postgres_conn_id='my_postgres_conn')
    connection = pg_hook.get_conn()
    cursor = connection.cursor()
    
    # สร้างตารางถ้ายังไม่มีอยู่
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS my_table (
            id SERIAL PRIMARY KEY,
            name VARCHAR(50),
            age INT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    # ใส่ข้อมูลตัวอย่างลงในตาราง
    cursor.execute("""
        INSERT INTO my_table (name, age) VALUES ('John Doe', 29), ('Jane Doe', 25)
    """)
    
    # บันทึกการเปลี่ยนแปลง
    connection.commit()
    cursor.close()

# กำหนดค่า default arguments สำหรับ DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime.strptime(datetime.now().strftime('%Y-%m-%d 00:00'),'%Y-%m-%d 00:00').replace(tzinfo=local_tz),
    'retries': 1
}

# สร้าง DAG
with DAG(
    dag_id='upload_data_to_postgres',
    default_args=default_args,
    schedule_interval=None,  # กำหนดว่า DAG นี้ไม่ต้องรันอัตโนมัติ
    catchup=False,
) as dag:

    # สร้าง Task สำหรับการอัพโหลดข้อมูลเข้า PostgreSQL
    upload_task = PythonOperator(
        task_id='upload_data',
        python_callable=upload_data_to_postgres,
        provide_context=True,
    )

    # กำหนดลำดับการรัน Task
    upload_task