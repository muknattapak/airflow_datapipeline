import logging
from time import time
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import pymongo
import pytz
from pymongo import MongoClient

import pendulum

from sqlalchemy import create_engine
from dataclasses import replace
import pandas as pd
import os
import sys
import redis
from bson import ObjectId
import numpy as np
from dateutil import tz

local_tz = pendulum.timezone("Asia/Bangkok")

# mongoexport -h ddb.thailand-smartliving.com:38200 --authenticationDatabase=aidery-health -u airflow -p 98ZaEVYxq758h2tk -d aidery-health -c survey-answers --fields=_id,userId,ts,response,crt --type=json --out=/Users/muk/symtoms.json

def get_data():
    r = redis.Redis(host='airflow_redis_1', port=6379, db=1)

    client = MongoClient(
        "mongodb://airflow:98ZaEVYxq758h2tk@ddb.thailand-smartliving.com:38200/aidery-health")

    mydb = client['aidery-health']
    mydb2 = client['aidery-auth']
    collect = mydb['health-records']
    collect2 = mydb2['users']
    latestId = r.get('latestOfStepPipeline')
    print('LatestId:', latestId)
    
    start = datetime(2020,5,25,00,00,00)
    # end = datetime(2020,5,25,23,59,59)

    # def dt_time_min(dt):
    #     """converts any datetime/date to new datetime with same date and time=0:00:00"""
    #     return datetime.datetime.combine(dt, datetime.time.min)

    # def dt_time_max(dt):
    #     """converts any datetime/date to new datetime with same date and time=23:59:59.999999"""
    #     return datetime.datetime.combine(dt, datetime.time.max)
    
    for x in range(0,30):
        if(latestId):
            x = collect.find(
                {'userId': '5eaa7561079a460011315d0d', 'type': 4,'_id': {'$gt':ObjectId(latestId.decode())},"from": {"$gte": start}})
        else:
            x = collect.find(
                {'userId': '5eaa7561079a460011315d0d', 'type': 4,"from": {"$gt": start}})
    
    list_cur = list(x)
    temp_data = pd.DataFrame(list_cur)
    df = temp_data.copy().reset_index(drop=True)
    df = df[['_id', 'userId', 'type', 'value', 'from', 'to']]

    # result = collect2.find({'_id': ObjectId('5eaa7561079a460011315d0d')}, {'_id', 'domainId'})
    # list_cur2 = list(result)
    # for i in range (len(df)):
    #     list_cur2.append(list_cur2)
    # temp = pd.DataFrame(user_df)
    # user_df = temp.copy().reset_index(drop=True)        
    # user_df = user_df[['_id','domainId']]
    # print(user_df)

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

    df['from'] = convert_tz(df['from'])
    df['to'] = convert_tz(df['to'])
    # df["sum"] = df.apply(lambda x: df.loc[x.name:,"value"].sum(),axis=1) 
    # df.groupby(df.index).agg({'value':'sum','from':'last'})

    df['domainId'] = "5d9556f191ea5500101d3eb2"
    df = df[['_id', 'userId', 'domainId', 'from', 'to', 'type', 'value']]
    path = os.path.join(os.getcwd(), "dags/temp/step_ta_new.csv")
    data = df.to_csv(path, index=False, 
                   encoding='utf-8', date_format="%Y-%m-%d %H:%M:%S")

    # df.index=pd.to_datetime(df.index)
    # df.set_index('from',inplace=True) 
    # df = df.resample('24H').sum() 
    # df.reset_index('from',inplace=True) 
    # df.drop(['type'], axis=1, inplace=True)
    # df['userid'] = "5eaa7561079a460011315d0d"
    # df['domainid'] = "5d9556f191ea5500101d3eb2"
    # df = df[['userid', 'domainid', 'from', 'value']]
    # # print(df)

    # engine = create_engine(
    #     'postgresql://airflow:airflow@airflow_postgres_1:5432/postgres')
    # df.to_sql(name="step_test_sum1d", if_exists='replace',
    #                  con=engine, chunksize=100)


def load_history():
    path = os.path.join(os.getcwd(), "dags/temp/step_ta_new.csv")
    data_dict = pd.read_csv(path)
    data_dict[['from','to']] = data_dict[['from','to']].astype('datetime64[ns]')
    data_dict.columns = [c.lower() for c in data_dict.columns]
    # engine = create_engine('postgresql://username:password@localhost:5432/dbname')
    engine = create_engine(
        'postgresql://airflow:airflow@airflow_postgres_1:5432/postgres')
    data_dict.to_sql(name="step_test", if_exists='replace',
                     con=engine, chunksize=100)

    r = redis.Redis(host='airflow_redis_1', port=6379, db=1)
    r.set('latestOfStep',data_dict['_id'].to_string(index=False))


def load_summary():
    path = os.path.join(os.getcwd(), "dags/temp/step_ta_new.csv")
    data_dict = pd.read_csv(path,usecols=['from','value'])
    data_dict['from'] = data_dict['from'].astype('datetime64[ns]')
    data_dict.index=pd.to_datetime(data_dict.index)
    data_dict.set_index('from',inplace=True) 
    data_dict = data_dict.resample('1H').sum() 
    data_dict.reset_index('from',inplace=True) 
    # print(data_dict)
    index = pd.DatetimeIndex(data_dict['from'])
    day_time = data_dict.iloc[index.indexer_between_time('00:00','12:00')]
    atn_time = data_dict.iloc[index.indexer_between_time('00:00','18:00')]    
    first_noti = datetime.strptime('5:00am', '%I:%M%p').strftime("%H:%M:%S")
    second_noti = datetime.strptime('11:00am', '%I:%M%p').strftime("%H:%M:%S")

    day_index = day_time.groupby(pd.to_datetime(day_time['from']).dt.date).agg({'value': 'sum'}).reset_index()
    # day_index.rename(columns={"from":"Date","value":"day_sum"},inplace=True)
    day_index["noti"] = first_noti  
    day_index["from"] = pd.to_datetime(day_index["from"])
    day_index['ts_noti'] = pd.to_datetime(day_index['from'].dt.strftime("%Y-%m-%d") + " " + day_index['noti'])
    day_index.drop(['noti', 'from'], axis=1, inplace=True)
    # print(day_index)

    atn_index = atn_time.groupby(pd.to_datetime(atn_time['from']).dt.date).agg({'value': 'sum'}).reset_index()
    # atn_index.rename(columns={"from":"Date","value":"atn_sum"},inplace=True)
    atn_index["noti"] = second_noti  
    atn_index["from"] = pd.to_datetime(atn_index["from"])
    atn_index['ts_noti'] = pd.to_datetime(atn_index['from'].dt.strftime("%Y-%m-%d") + " " + atn_index['noti'])
    atn_index.drop(['noti', 'from'], axis=1, inplace=True)
    
    newdata = pd.concat([day_index,atn_index],axis=0)
    newdata['msg'] = newdata['value'].apply(lambda x: 'ก้าวเดินครบตามเกณฑ์สุขภาพที่กำหนดค่ะ' 
                        if x >= 10000 else 'ก้าวเดินต่ำกว่าเกณฑ์สุขภาพที่กำหนดค่ะ')
    newdata['channel'] = "0"
    newdata['app'] = "0"    
    newdata['userid'] = "5eaa7561079a460011315d0d"
    newdata['domainid'] = "5d9556f191ea5500101d3eb2"
    newdata['title'] = "แจ้งเตือนค่าสุขภาพเกิน"
    newdata['readed'] = "0"
    newdata['status'] = "0"
    newdata['sentAt'] = np.nan
    newdata['type'] = "4"
    newdata = newdata.sort_values(by='ts_noti', ascending=True)
    # newdata['ts_noti'] = pd.to_datetime(newdata['ts_noti']).dt.tz_localize(tz.tzlocal())
    newdata['ts_noti'] = pd.to_datetime(newdata['ts_noti'], utc=True).dt.tz_convert('UTC') #Etc/GMT-7
    newdata['payload'] = newdata[['type','value']].apply(lambda x: x.to_json(), axis=1)
    newdata = newdata[['userid', 'domainid', 'ts_noti', 'title', 'msg', 'payload', 'channel', 'app', 'readed', 'sentAt', 'status']]
    newdata.reset_index(inplace=True, drop=True)
    newdata['ts_noti'] = newdata['ts_noti'].astype('datetime64[ns]')
    newdata['sentAt'] = pd.to_datetime(newdata['sentAt'], errors='coerce')
    newdata[['channel','app','readed','status']] = newdata[['channel','app','readed','status']].astype('int')
    engine = create_engine(
        'postgresql://airflow:airflow@airflow_postgres_1:5432/postgres')
    newdata.to_sql(name="step_test_noti", if_exists='replace',
                     con=engine, chunksize=100)


default_args = {
    'owner': 'zg',
    'depends_on_past': False,
    'start_date': datetime(2022, 7, 26,  tzinfo=local_tz),
    # 'email': ['@gmail.com'],
    # 'email_on_failure': True,
    # 'email_on_retry': True,
    # 'retries': 3,

    'schedule_interval': '@daily',
    'retry_delay': timedelta(seconds=5),
}

with DAG('elt_step_new',
         default_args=default_args,
         schedule_interval='@daily') as dag:

    t1 = PythonOperator(
        task_id='t1_get_mongo',
        provide_context=True,
        python_callable=get_data
    )

    t2 = PythonOperator(
        task_id='t2_load_postgres',
        provide_context=True,
        python_callable=load_history
    )

    t3 = PythonOperator(
        task_id='t3_summary_load_postgres',
        provide_context=True,
        python_callable=load_summary
    )

    t1 >> t2 >> t3