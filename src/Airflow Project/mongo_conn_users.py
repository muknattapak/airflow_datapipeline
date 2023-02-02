from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import pytz
from pymongo import MongoClient

import pendulum

from sqlalchemy import create_engine
from dataclasses import replace
import pandas as pd
import os
import sys

local_tz = pendulum.timezone("Asia/Bangkok")

# mongoexport -h ddb.thailand-smartliving.com:38200 --authenticationDatabase=aidery-health -u airflow -p 98ZaEVYxq758h2tk -d aidery-health -c survey-answers --fields=_id,userId,ts,response,crt --type=json --out=/Users/muk/symtoms.json


def get_data(**kwargs):
    ti = kwargs['ti']
    client = MongoClient(
        "mongodb://airflow:98ZaEVYxq758h2tk@ddb.thailand-smartliving.com:38200/aidery-health")

    mydb = client['aidery-auth']
    collect2 = mydb['users']
    cursor = collect2.find(
        {'role': 1, 'status': {'$gt': -1}}).limit(25000)
    list_cur = list(cursor)
    temp_data = pd.DataFrame(list_cur)
    df = temp_data.copy().reset_index(drop=True)
    df = df[['_id', 'domainId', 'firstName', 'lastName', 'role', 'age', 'birthday',
             'gender', 'nationalId', 'phone', 'address',
             'diseases', 'allergy', 'features', 'height',  'weight',
             'status', 'crt', 'mdt', 'lastActive', 'families', 'caregivers', 'surgeries',
             'covidTest', 'swabDate', 'badgeColor']]

    df = pd.concat([df.drop('allergy', axis=1),
                    df['allergy'].apply(pd.Series)], axis=1)

    df.columns = df.columns.map(str)
    df = df.drop(['0'], axis=1)

    def utc_to_local(utc_dt):
        local = 'Asia/Bangkok'
        try:
            local_tz = pytz.timezone(local)
            local_dt = utc_dt.replace(tzinfo=pytz.utc).astimezone(local_tz)
            # .normalize might be unnecessary
            return local_tz.normalize(local_dt)
        except:
            return pd.NaT

    def convert_tz(data):
        data_con = pd.to_datetime(data.apply(
            lambda x: utc_to_local(pd.to_datetime(x, errors='coerce'))))
        data_con = pd.to_datetime((data_con.dt.strftime('%Y-%m-%d %H:%M:%S')))
        return data_con

    df['crtTH'] = convert_tz(df['crt'])
    df['mdtTH'] = convert_tz(df['mdt'])

    path = os.path.join(os.getcwd(), "dags/temp/users.csv")
    data = df.to_csv(path, index=False)
    ti.xcom_push('extract_data', data)


def read_data(**kwargs):
    ti = kwargs['ti']
    order_data = ti.xcom_pull(task_ids='t1_get_mongo', key="extract_data")
    path = os.path.join(os.getcwd(), "dags/temp/users.csv")
    data_csv = pd.read_csv(path)
    mongo_df = data_csv.to_dict()
    ti.xcom_push('all_data', mongo_df)


def load_data(**kwargs):
    ti = kwargs['ti']
    df_load = ti.xcom_pull(task_ids='t2_read_csv', key="all_data")
    data_dict = pd.DataFrame.from_dict(df_load)
    data_dict[['crtTH','mdtTH']] = data_dict[['crtTH','mdtTH']].astype('datetime64[ns]')
    data_dict.columns = [c.lower() for c in data_dict.columns]
    # engine = create_engine('postgresql://username:password@localhost:5432/dbname')
    engine = create_engine(
        'postgresql://airflow:airflow@airflow_postgres_1:5432/postgres')
    data_dict.to_sql(name="users_mt", if_exists='replace',
                     con=engine, chunksize=100)


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2022, 7, 26,  tzinfo=local_tz),
    # 'email': ['@gmail.com'],
    # 'email_on_failure': True,
    # 'email_on_retry': True,
    # 'retries': 3,

    'schedule_interval': '@daily',
    'retry_delay': timedelta(seconds=5),
}

with DAG('elt_users',
         default_args=default_args,
         schedule_interval='@daily') as dag:

    t1 = PythonOperator(
        task_id='t1_get_mongo',
        provide_context=True,
        python_callable=get_data
    )

    t2 = PythonOperator(
        task_id='t2_read_csv',
        provide_context=True,
        python_callable=read_data
    )

    t3 = PythonOperator(
        task_id='t3_load_postgres',
        provide_context=True,
        python_callable=load_data
    )

    t1 >> t2 >> t3
