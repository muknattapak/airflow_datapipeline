
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


def retrieve_nested_value(mapping, key_of_interest):
    mappings = [mapping]
    while mappings:
        mapping = mappings.pop()
        try:
            items = mapping.items()
        except AttributeError:
            # we didn't store a mapping earlier on so just skip that value
            continue

        for key, value in items:
            if key == key_of_interest:
                yield value
            else:
                # type of the value will be checked in the next loop
                mappings.append(value)


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


def cal_threshold(values, thrsh):
    key_l = ['abnormal', 'urgent', 'emergency']

    for item in key_l:
        # item = 'emergency'
        grp_level = 'none'
        for level_l in retrieve_nested_value(thrsh['low'], item):
            if ('min' in level_l) & ('max' in level_l):
                if (level_l['min'] <= values <= level_l['max']):
                    grp_level = item
            elif ('max' in level_l):
                if (level_l['max'] >= values):
                    grp_level = item

        for level_l in retrieve_nested_value(thrsh['high'], item):
            if ('min' in level_l) & ('max' in level_l):
                if (level_l['min'] <= values <= level_l['max']):
                    grp_level = item
            elif ('min' in level_l):
                if (level_l['min'] <= values):
                    grp_level = item
        if (grp_level != 'none'):
            break

    if (grp_level == 'none'):
        grp_level = 'normal'

    return grp_level


def new_nest_dict(m):
    for val in m.values():
        if isinstance(val, dict):
            yield from new_nest_dict(val)
        else:
            yield val


def get_data():
    r = redis.Redis(host='airflow_redis_1', port=6379, db=1)

    client = MongoClient(
        "mongodb://airflow:98ZaEVYxq758h2tk@ddb.thailand-smartliving.com:38200/aidery-health")

    aid_health = client['aidery-health']
    aid_auth = client['aidery-auth']

    healthColl = aid_health['health-records']
    settingColl = aid_health['settings']
    mastersettingColl = aid_health['master-setting']
    # aidery-auth
    userColl = aid_auth['users']

    latestId = r.get('latestOfHealthRecords')
    # print('LatestId:', latestId)

    limit_row = 10000
    # read health record
    if (latestId):
        cursor = healthColl.find(
            {'_id': {'$gt': ObjectId(latestId.decode())}}).sort('_id', 1).limit(limit_row)
        # cursor = healthColl.find().sort('_id', 1).limit(limit_row)
    else:
        cursor = healthColl.find().sort('_id', 1).limit(limit_row)

    temp_data = pd.DataFrame(list(cursor))
    healthData = temp_data.copy().reset_index(drop=True)
    healthData['label'] = np.nan

    # read user record
    user_list = healthData['userId'].copy().drop_duplicates(
    ).reset_index(drop=True)  # get list of userid

    # ==== # calculate by user id ==========

    health_label = pd.DataFrame()
    for uid in range(len(user_list)):
        # read threshold
        if settingColl.count_documents({'userId': ObjectId(user_list[uid])}) > 0:
            print('settings')
            cursor = settingColl.find({'userId': ObjectId(user_list[uid])}).sort(
                '_id', 1).limit(1000)
        else:
            print('mastersetting')
            cursorU = userColl.find(
                {'_id': ObjectId(user_list[uid])}, {'_id', 'domainId'})
            users_ls = pd.DataFrame(list(cursorU))
            cursor = mastersettingColl.find(
                {'domainId': str(users_ls['domainId'][0])}).sort('_id', 1).limit(1000)
            if (mastersettingColl.count_documents({'domainId': str(users_ls['domainId'][0])}) <= 0):
                cursor = mastersettingColl.find(
                    {'domainId': {'$exists': False}, 'category': 'health'}).sort('_id', 1).limit(1000)

        temp_thresh = pd.DataFrame(list(cursor))
        thresh = temp_thresh[['_id', 'type', 'low', 'high']].copy()
        # condition setting
        data = healthData[healthData['userId'] == user_list[uid]].copy()
        type_list = data['type'].drop_duplicates(
        ).reset_index(drop=True).values.tolist()

        for type_l in type_list:
            thr_ls = thresh[thresh['type'] == type_l]
            if len(thr_ls) > 0:
                # check None all threshold
                listOfStrings = list(new_nest_dict(
                    dict(enumerate(thr_ls['low'].values.flatten(), 1))))
                result = False
                if len(listOfStrings) > 0:
                    result = all(elem == None for elem in listOfStrings)
                if not result:
                    print("All Elements in List are Equal")
                    data['label'] = data['value'].apply(
                        cal_threshold, thrsh=thr_ls)

        health_label = health_label.append(data, ignore_index=True)

    path = os.path.join(os.getcwd(), "dags/temp/health_labeld.csv")
    data = health_label.to_csv(path, index=False)


def load_data():
    path = os.path.join(os.getcwd(), "dags/temp/health_labeld.csv")
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

with DAG('ETL_health-labeld',
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
