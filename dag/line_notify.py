from airflow import DAG
from airflow.operators.python_operator import PythonOperator

import requests
import json
from datetime import date, datetime, timedelta
import pendulum

local_tz = pendulum.timezone("Asia/Bangkok")

## ดึงข้อมูลจาก API
def get_covid19_report_today():
    url = 'https://covid19.th-stat.com/api/open/today'
    response = requests.get(url)
    data = response.json()

    with open('data.json', 'w') as f:
        json.dump(data, f)

    return data

## ส่งข้อมูลเข้า Line notify ผ่าน API 
def send_line_notify():
    url = 'https://notify-api.line.me/api/notify'
    token = 'your_token'
    headers = {
        'content-type':
        'application/x-www-form-urlencoded',
        'Authorization': 'Bearer '+token
    }

    with open('data.json') as f:
        data = json.load(f)

    msg = "Covid-19 report today \n"
    for (k,v) in data.items():
        msg += str(k) + ":" + str(v) + "\n"

    r = requests.post(url, headers=headers, data={'message': msg})
    print(r.text)

## สร้าง DAG Object โดยกำหนดเวลาในการทำงานทุกๆ 8 โมงของทุกวัน
default_args = {
    'owner': 'airflow',
    'start_date': datetime.strptime(datetime.now().strftime('%Y-%m-%d 00:00'),'%Y-%m-%d 00:00').replace(tzinfo=local_tz),
}
with DAG('line-notify',
         schedule_interval='0 8 * * *',
         default_args=default_args,
         description='A simple data pipeline for line-notify',
         catchup=False) as dag:

    t1 = PythonOperator(
        task_id='get_covid19_report_today',
        python_callable=get_covid19_report_today
    )

    t2 = PythonOperator(
        task_id='send_line_notify',
        python_callable=send_line_notify
    )

    t1 >> t2