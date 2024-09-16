import requests
import pandas as pd
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime
import pendulum
from bs4 import BeautifulSoup

local_tz = pendulum.timezone("Asia/Bangkok")

# ฟังก์ชันสำหรับดึงข้อมูลจาก API
def fetch_data_from_api(**kwargs):
    url = 'https://www.worldometers.info/coronavirus/'
    response = requests.get(url)

    # ตรวจสอบว่า request สำเร็จหรือไม่
    if response.status_code != 200:
        raise Exception(f"Failed to fetch data. Status code: {response.status_code}")

    # Parse ข้อมูล HTML
    soup = BeautifulSoup(response.text, 'lxml')

    # ดึงข้อมูลจากตารางที่มี id = 'main_table_countries_today'
    table_data = soup.find('table', id='main_table_countries_today')

    # ดึงหัวตาราง
    headers = []
    for th in table_data.find_all('th'):
        title = th.text.strip()
        headers.append(title)

    # แปลงข้อมูลเป็น DataFrame เพื่อความสะดวกในการจัดการข้อมูล
    rows = []
    for tr in table_data.find_all('tr')[1:]:
        row = [td.text.strip() for td in tr.find_all('td')]
        rows.append(row)

    df = pd.DataFrame(rows, columns=headers)

    # ส่งข้อมูลไปให้ Task ถัดไป
    return df.to_dict()

# ฟังก์ชันสำหรับ Cleansing ข้อมูล
def clean_data(**kwargs):
    # ดึงข้อมูลจาก Task ก่อนหน้า
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='fetch_data')
    df = pd.DataFrame(data)

    # ทำการเลือกเฉพาะคอลัมน์ที่ต้องการ
    df2 = df[['#', 'Country,Other', 'TotalCases', 'NewCases', 'TotalDeaths', 'NewDeaths',
              'TotalRecovered', 'NewRecovered', 'ActiveCases', 'Serious,Critical',
              'Tot\xa0Cases/1M pop', 'Deaths/1M pop', 'TotalTests', 'Tests/\n1M pop\n', 'Population']]

    # การทำ Cleansing ข้อมูล ด้วยการลบแถวที่เป็นข้อมูลรวม เช่น Total
    df2 = df2[(df2['Country,Other'] != 'Total:') & (df2['#'] != '')]

    # ส่งข้อมูลไปให้ Task ถัดไป
    return df2.to_dict()

# ฟังก์ชันสำหรับอัพโหลดข้อมูลเข้า PostgreSQL
def upload_data_to_postgres(**kwargs):
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='clean_data')
    df = pd.DataFrame(data)

    pg_hook = PostgresHook(postgres_conn_id='my_postgres_conn')
    connection = pg_hook.get_conn()
    cursor = connection.cursor()

    # สร้างตารางถ้ายังไม่มี
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS my_cleaned_table (
            id SERIAL PRIMARY KEY,
            country VARCHAR(50),
            total_cases BIGINT,
            new_cases BIGINT,
            total_deaths BIGINT,
            new_deaths BIGINT,
            total_recovered BIGINT,
            active_cases BIGINT,
            serious_critical BIGINT,
            total_tests BIGINT,
            population BIGINT
        )
    """)

    # ใส่ข้อมูลลงในตาราง
    for _, row in df.iterrows():
        cursor.execute("""
            INSERT INTO my_cleaned_table (country, total_cases, new_cases, total_deaths, new_deaths, total_recovered,
                                          active_cases, serious_critical, total_tests, population)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (row['Country,Other'], row['TotalCases'], row['NewCases'], row['TotalDeaths'], row['NewDeaths'],
              row['TotalRecovered'], row['ActiveCases'], row['Serious,Critical'], row['TotalTests'], row['Population']))

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
    dag_id='api_to_postgres_dag',
    default_args=default_args,
    schedule_interval=None,  # กำหนดว่า DAG นี้ไม่ต้องรันอัตโนมัติ ในกรณีที่อยากให้รันอัตโนมัติทุกๆ 1 ชม ให้ใส่เป็น '@hourly'
    catchup=False,
) as dag:

    # Task สำหรับดึงข้อมูลจาก API
    fetch_data = PythonOperator(
        task_id='fetch_data',
        python_callable=fetch_data_from_api,
        provide_context=True,
    )

    # Task สำหรับ Cleansing ข้อมูล
    clean_data = PythonOperator(
        task_id='clean_data',
        python_callable=clean_data,
        provide_context=True,
    )

    # Task สำหรับอัพโหลดข้อมูลเข้า PostgreSQL
    upload_data = PythonOperator(
        task_id='upload_data',
        python_callable=upload_data_to_postgres,
        provide_context=True,
    )

    # กำหนดลำดับการรัน Task
    fetch_data >> clean_data >> upload_data