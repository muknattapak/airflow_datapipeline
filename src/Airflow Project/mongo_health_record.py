
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import pymongo
import pytz
from datetime import datetime, timedelta
from pymongo import MongoClient
from bson import ObjectId
import pendulum
from sqlalchemy import create_engine
from dataclasses import replace
import pandas as pd
import numpy as np
import os
import sys
import redis

import constant

# sys.path.insert(0, r'C:\Users\thawaree\Documents\zg-analytics-python')
import sys
sys.path.insert(
    0, '/Users/thawaree/Documents/GitHub/zg-analytics-engine-python/src')

local_tz = pendulum.timezone("Asia/Bangkok")

# mongoexport -h ddb.thailand-smartliving.com:38200 --authenticationDatabase=aidery-health -u airflow -p 98ZaEVYxq758h2tk -d aidery-health -c survey-answers --fields=_id,userId,ts,response,crt --type=json --out=/Users/muk/symtoms.json


def is_latest_id(**kwargs):
    r = redis.Redis(host='airflow_redis_1', port=6379, db=1)

    client = MongoClient(
        "mongodb://airflow:98ZaEVYxq758h2tk@ddb.thailand-smartliving.com:38200/aidery-health")

    health_db = client['aidery-health']
    healthColl = health_db['health-records']
    latestId = r.get('latestOfStepPipeline')
    print('LatestId:', latestId)

    latest_record = pd.DataFrame(list(healthColl.find({'type': {'$in': [
                                 4, 6, 7, 8, 9, 10, 13, 14, 19, 21]}}, {'_id': 1}).sort('_id', -1).limit(1)))

    if latest_record['_id'][0] > ObjectId(latestId.decode()):
        return True

    return False


def cal_threshold(data, thresh_level):
    # print(type(data))
    option = int(data.loc[0, 'type'])

    def HEART_RATE():
        print('Option: ', option)
        print(data[['value']])
        print(thresh_level[thresh_level['type'] == option])

        if (data[['value']] in (get_key('min', thresh_level['low_abnormal'].loc[0]), get_key('min', thresh_level['low_abnormal'].loc[0]))) | (data[['value']] in (get_key('min', thresh_level['high_abnormal'].loc[0]), get_key('min', thresh_level['high_abnormal'].loc[0]))):
            return 'abnormal'
        elif (data[['value']] in (get_key('min', thresh_level['low_urgent'].loc[0]), get_key('min', thresh_level['low_urgent'].loc[0]))) | (data[['value']] in (get_key('min', thresh_level['high_urgent'].loc[0]), get_key('min', thresh_level['high_urgent'].loc[0]))):
            return 'urgent'
        elif (data[['value']] in (get_key('min', thresh_level['low_emergency'].loc[0]), get_key('min', thresh_level['low_emergency'].loc[0]))) | (data[['value']] in (get_key('min', thresh_level['high_emergency'].loc[0]), get_key('min', thresh_level['high_emergency'].loc[0]))):
            return 'emergency'
        else:
            return 'normal'
        # return label

    # def BODY_TEMPERATURE():
    #     print('Option: ',option)
    #     print(thresh_level)
    #     pprint(data[['value']])
    #     # return data

    # def BLOOD_PRESSURE_SYSTOLIC():
    #     print('Option: ',option)
    #     print(thresh_level)
    #     print(data[['value']])
    #     # return data

    # def BLOOD_PRESSURE_DIASTOLIC():
    #     print('Option: ',option)
    #     print(thresh_level)
    #     print(data[['value']])
    #     # return data

    # def BLOOD_OXYGEN():
    #     print('Option: ',option)
    #     print(thresh_level[thresh_level['type']==option])
    #     print(data[['value']])
    #     # return data

    # def BLOOD_GLUCOSE():
    #     print('Option: ',option)
    #     print(thresh_level)
    #     print(data[['value']])
    #     # return data

    # def ELECTRODERMAL_ACTIVITY():
    #     print('Option: ',option)
    #     print(thresh_level)
    #     print(data[['value']])
    #     # return data

    # def HIGH_HEART_RATE_EVENT():
    #     print('Option: ',option)
    #     print(thresh_level)
    #     print(data[['value']])
    #     # return data

    # def LOW_HEART_RATE_EVENT():
    #     print('Option: ',option)
    #     print(thresh_level)
    #     print(data[['value']])
    #     # return data

    # def IRREGULAR_HEART_RATE_EVENT():
    #     print('Option: ',option)
    #     print(thresh_level)
    #     print(data[['value']])
    #     # return data

    # def BLOOD_PRESSURE():
    #     print('Option: ',option)
    #     print(thresh_level)
    #     print(data[['value']])
    #     # return data

    def default():
        print("Incorrect option")

# Dictionary Mapping
    dict = {
        7: HEART_RATE,
        # 8 : BODY_TEMPERATURE,
        # 9 : BLOOD_PRESSURE_SYSTOLIC,
        # 10 : BLOOD_PRESSURE_DIASTOLIC,
        # 13 : BLOOD_OXYGEN,
        # 14 : BLOOD_GLUCOSE,
        # 15 : ELECTRODERMAL_ACTIVITY,
        # 16 : HIGH_HEART_RATE_EVENT,
        # 17 : LOW_HEART_RATE_EVENT,
        # 18 : IRREGULAR_HEART_RATE_EVENT,
        # 19 : BLOOD_PRESSURE
    }
    # get() method returns the function matching the argument
    dict.get(option, default)()


def get_key(keyin, data):
    for key, value in data.items():
        if keyin == key:
            return value
    return print("key does not exist")


def get_data():
    r = redis.Redis(host='airflow_redis_1', port=6379, db=1)

    client = MongoClient(
        "mongodb://airflow:98ZaEVYxq758h2tk@ddb.thailand-smartliving.com:38200/aidery-health")

    mydb = client['aidery-health']

    healthColl = mydb['health-records']
    userColl = mydb['users']
    settingColl = mydb['settings']
    mastersettingColl = mydb['master-setting']

    latestId = r.get('latestOfHealthRecords')
    # print('LatestId:', latestId)

    limit_row = 50
    # read health record
    if (latestId):
        cursor = healthColl.find(
            {'_id': {'$gt': ObjectId(latestId.decode())}}).sort('_id', 1).limit(limit_row)

        cursor = healthColl.find().sort('_id', 1).limit(limit_row)
        # {'role': 1, 'status': {'$gt': -1},'_id':{'$gt':ObjectId("6165414387405b0018e9e302")}}).sort('_id',1).skip(10600).limit(100)
    else:
        cursor = healthColl.find().sort('_id', 1).limit(limit_row)

    temp_data = pd.DataFrame(list(cursor))
    # temp_data = pd.DataFrame(list_cur)
    healthData = temp_data.copy().reset_index(drop=True)
    healthData['label'] = np.nan

    # read user record
    user_list = healthData['userId'].copy().drop_duplicates(
    ).reset_index(drop=True)  # get list of userid

    # ==== # calculate by user id ==========

    # read threshold
    exist_setting = settingColl.find(
        {'userId': ObjectId(user_list[0])}).count()
    if (exist_setting):
        # print('settings')
        cursor = settingColl.find({'userId': ObjectId(user_list[0])}).sort(
            '_id', 1).limit(limit_row)
    else:
        # print('mastersetting')
        cursor = mastersettingColl.find(
            {'userId': ObjectId(user_list[0])}).sort('_id', 1).limit(1000)

    temp_threshold = pd.DataFrame(list(cursor))

    # condition setting

    data = healthData.copy()

    for i in range(len(data)):
        thresh_temp = temp_thres.iloc[temp_thres['type']
                                      == data.iloc[i, 'type']]
        thresh_temp = pd.concat([thresh_temp.drop(
            'low', axis=1), thresh_temp['low'].apply(pd.Series).add_prefix('low_')], axis=1)
        thresh_temp = pd.concat([thresh_temp.drop(
            'high', axis=1), thresh_temp['high'].apply(pd.Series).add_prefix('high_')], axis=1)

        test = cal_threshold(data.loc[[i]], thresh_temp)

    get_key('min', thresh_temp['low_urgent'].ioc[0])

    user_dataset = pd.DataFrame()
    for i in range(len(user_list)):
        cursor2 = userColl.find(
            {'_id': ObjectId(user_list[i]), 'status': {'$gt': -1}}, {'meta': 0}).sort('_id', 1).limit(limit_row)
        u_cur = list(cursor2)
        col_list = ['_id', 'domainId', 'birthday', 'gender']
        if u_cur:
            user_temp = pd.DataFrame(u_cur)
            lost_col = pd.Series(col_list).isin(user_temp.columns) == False
            lost_col_idx = [i for i, x in enumerate(lost_col) if x]

            if (lost_col.any()):  # add lost column name and fill with nan value
                for j in range(len(lost_col_idx)):
                    s_idx = [i for i in lost_col.index if lost_col[i]].pop(j)
                    user_temp[col_list[s_idx]] = user_temp.get(
                        col_list[s_idx], np.nan)

            user_temp = user_temp[col_list]

        user_dataset = pd.concat(
            [user_dataset, user_temp], axis=0, sort=False, igNote_index=True)

    if 'context' in data.columns:
        data = pd.concat([data.drop('context', axis=1),
                         data['context'].apply(pd.Series)], axis=1)
        data.columns = data.columns.map(str)
        # data = data.drop(['0','glucoseConcentration','symptoms'], axis=1)
        data = pd.merge(data, user_dataset, left_on=['userId'], right_on=[
                        '_id'], how='left', suffixes=('', '_y'))
        data.drop(data.filter(regex='_y$').columns, axis=1, inplace=True)

    cur_setting = set_level.find({'domainId': '5a6c3e11ea53c82175ea2217', 'category': 'health',
                                  'type': {'$in': [4, 6, 7, 8, 9, 10, 13, 14, 19]}},
                                 {'crt': 0, 'mdt': 0, 'userId': 0})
    setting_ls = list(cur_setting)
    setting_data = pd.DataFrame(setting_ls)
    pd.concat([setting_data.drop('low', axis=1),
              setting_data['low'].apply(pd.Series)], axis=1)
    setting_data

    path = os.path.join(os.getcwd(), "dags/temp/blood_g.csv")
    data = df.to_csv(path, index=False)


def load_data():
    path = os.path.join(os.getcwd(), "dags/temp/blood_g.csv")
    data_csv = pd.read_csv(path)
    data_dict = pd.DataFrame.from_dict(data_csv)
    data_dict[['from', 'to', 'crt', 'timeStamp']] = data_dict[[
        'from', 'to', 'crt', 'timeStamp']].astype('datetime64[ns]')
    data_dict.columns = [c.lower() for c in data_dict.columns]
    # engine = create_engine('postgresql://username:password@localhost:5432/dbname')
    engine = create_engine(
        'postgresql://airflow:airflow@airflow_postgres_1:5432/postgres')
    data_dict.to_sql(name="health_records", if_exists='replace',
                     con=engine, chunksize=100)

    r = redis.Redis(host='airflow_redis_1', port=6379, db=1)
    r.set('latestOfHealthRecords', data_csv['_id'].tail(
        1).to_string(index=False))


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

with DAG('elt_bloodg',
         default_args=default_args,
         schedule_interval='@daily') as dag:

    t1 = PythonOperator(
        task_id='t1_get_mongo',
        provide_context=True,
        python_callable=get_data
    )

    t2 = PythonOperator(
        task_id='t3_load_postgres',
        provide_context=True,
        python_callable=load_data
    )

    t1 >> t2
