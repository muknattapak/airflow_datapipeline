from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from bson import ObjectId
import pytz
from pymongo import MongoClient
from elasticsearch import Elasticsearch, helpers
from elasticsearch.client import Elasticsearch
import csv
from base64 import b64encode

import pendulum

from sqlalchemy import create_engine
from dataclasses import replace
import pandas as pd
import os

import redis

local_tz = pendulum.timezone("Asia/Bangkok")

# mongoexport -h ddb.thailand-smartliving.com:38200 --authenticationDatabase=aidery-health -u airflow -p 98ZaEVYxq758h2tk -d aidery-health -c survey-answers --fields=_id,userId,ts,response,crt --type=json --out=/Users/muk/symtoms.json


def get_data():
    r = redis.Redis(host='airflow_redis_1', port=6379, db=1)

    client = MongoClient(
        "mongodb://airflow:98ZaEVYxq758h2tk@ddb.thailand-smartliving.com:38200/aidery-health")

    # print(client.server_info())
    mydb = client['aidery-auth']
    collect2 = mydb['users']
    latestId = r.get('latestOfUserPipeline')
    print('LatestId:', latestId)
    # cursor = collect2.find(
    #     {'role': 1, 'status': {'$gt': -1}}).sort('_id',1).limit(250)
    
    if(latestId):
        cursor = collect2.find(
            {'role': 1, 'status': {'$gt': -1},'_id':{'$gt':ObjectId(latestId.decode())}}).sort('_id',1).limit(10000)
            # {'role': 1, 'status': {'$gt': -1},'_id':{'$gt':ObjectId("6165414387405b0018e9e302")}}).sort('_id',1).skip(10600).limit(100)
    else:
        cursor = collect2.find(
            {'role': 1, 'status': {'$gt': -1}}).sort('_id',1).limit(10000)

    print(cursor)
    list_cur = list(cursor)
    temp_data = pd.DataFrame(list_cur)
    df = temp_data.copy().reset_index(drop=True)
    df = df[['_id', 'domainId', 'firstName', 'lastName', 'role', 'birthday',
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

    df['crt'] = convert_tz(df['crt'])
    df['mdt'] = convert_tz(df['mdt'])
    df['lastActive'] = convert_tz(df['lastActive'])

    path = os.path.join(os.getcwd(), "dags/temp/users-test.csv")
    data = df.to_csv(path, index=False)


def load_data():
    path = os.path.join(os.getcwd(), "dags/temp/users-test.csv")
    data_csv = pd.read_csv(path)
    data_csv[['crt','mdt','lastActive']] = data_csv[['crt','mdt','lastActive']].astype('datetime64[ns]')
    # data_csv = data_csv.sort('_id', ascending=False)

    es = Elasticsearch(hosts="https://elastic:qAMhwPV4yT3dKxK2@es.aidery.io:9200",
                        use_ssl = True,
                        verify_certs=False)
    index_name='healthnotes_user'
    reader = data_csv
    helpers.bulk(es, reader, index=index_name)

    r = redis.Redis(host='airflow_redis_1', port=6379, db=1)
    r.set('latestOfUserPipeline',data_csv['_id'].tail(1).to_string(index=False))
    # for key,val in data_redis.items():
    #     r.set(key,val)
        # value = r.get(key)
        # print('redis:',value)


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2022, 9, 6,  tzinfo=local_tz),
    # 'email': ['@gmail.com'],
    # 'email_on_failure': True,
    # 'email_on_retry': True,
    # 'retries': 3,

    'schedule_interval': '@hourly',
    'retry_delay': timedelta(seconds=5),
}

with DAG('elt_users_test',
         default_args=default_args,
         schedule_interval=timedelta(minutes=5))as dag:
        #  schedule_interval='@hourly'

    t1 = PythonOperator(
        task_id='t1_get_mongo',
        provide_context=True,
        python_callable=get_data
    )

    t2 = PythonOperator(
        task_id='t2_load_elastic',
        provide_context=True,
        python_callable=load_data
    )

    t1 >> t2 
