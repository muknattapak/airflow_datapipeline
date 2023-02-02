from dateutil.relativedelta import relativedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python import ShortCircuitOperator
from pymongo import MongoClient
from bson import ObjectId
from datetime import datetime, timedelta, date
import pendulum
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine, Table, Column, MetaData, Integer, String, Float, DateTime, Sequence, Text
from dataclasses import replace
import pandas as pd
import numpy as np
import os
import redis


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
    latestId = r.get('latestOfHealthRecords')
    print('LatestId:', latestId)

    latest_record = pd.DataFrame(list(healthColl.find(
        {'type': {'$in': [7]}}, {'_id': 1}).sort('_id', -1).limit(1)))

    if latestId is not None:
        if latest_record['_id'][0] > ObjectId(latestId.decode()):
            return True
        else:
            return False
    else:
        return True


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

    personalColl = aid_auth['personal-records']

    healthColl = aid_health['health-records']
    # aidery-auth
    userColl = aid_auth['users']

    latestId = r.get('latestOfHealthRecords')
    # print('LatestId:', latestId)

    limit_row = 100
    # read health record
    if (latestId):
        cursor = healthColl.find(
            {'_id': {'$gt': ObjectId(latestId.decode())}, 'type': {'$in': [7]}}).sort('_id', 1).limit(limit_row)
        # cursor = healthColl.find(
        #     {'_id': {'$gt': ObjectId(latestId)}, 'type': {'$in': [4, 7]}}).sort('_id', 1).limit(limit_row)
    else:
        cursor = healthColl.find({'type': {'$in': [7]}}).sort(
            '_id', 1).sort('_id', 1).limit(limit_row)

    temp_data = pd.DataFrame(list(cursor))
    if 'id' not in temp_data.columns:
        temp_data['id'] = np.nan
    if 'source' not in temp_data.columns:
        temp_data['source'] = np.nan
    temp_data.rename(columns={'from': 'start', 'to': 'end'}, inplace=True)
    healthData = temp_data.copy().reset_index(drop=True)

    # read user record
    user_list = healthData['userId'].copy().drop_duplicates(
    ).reset_index(drop=True)  # get list of userid

    # ==== # calculate by user id ==========
    health_clust = pd.DataFrame()
    # ------> split user
    for uid in range(len(user_list)):
        # read threshold
        # print('settings')
        data = pd.DataFrame()
        cursorU = userColl.find(
            {'_id': ObjectId(user_list[uid])}, {'_id', 'domainId', 'birthday'})
        users_ls = pd.DataFrame(list(cursorU))
        # condition setting
        data = healthData[healthData['userId'] == user_list[uid]].copy()
        data = data.merge(users_ls, how='left', left_on='userId',
                          right_on='_id', suffixes=('', '_y'))
        type_list = data['type'].drop_duplicates(
        ).reset_index(drop=True).values.tolist()

        thr_ls = thresh[thresh['type'] == type_l]
        if len(thr_ls) > 0:
            # check None all threshold dataset
            listOfStrings = list(new_nest_dict(
                dict(enumerate(thr_ls['low'].values.flatten(), 1))))
            result = False
            if len(listOfStrings) > 0:
                result = all(elem == None for elem in listOfStrings)
            if not result:
                # print("All Elements in List are Equal")
                data['label'] = data['value'].apply(
                    cal_threshold, thrsh=thr_ls)

        # read users dataset
        cursorU = userColl.find({'_id': ObjectId('608d0cd71fba9a0011baec8d')}, {
                                '_id', 'domainId', 'birthday'})
        users_ls = pd.DataFrame(list(cursorU))
        if not users_ls['birthday'].isna().any():
            users_ls['birth_year'] = users_ls['birthday'].apply(
                lambda x: x.year)
            users_ls.drop(['birthday'], axis=1, inplace=True)
        else:
            users_ls['birth_year'] = None

        data = data.merge(users_ls, how='left', left_on='userId',
                          right_on='_id', suffixes=('', '_y'))
        data.drop(data.filter(regex='_y$').columns, axis=1, inplace=True)
        health_clust = pd.concat((health_clust, data),
                                 axis=0, ignore_index=True)
        health_clust = health_clust.where(pd.notnull(health_clust), None)

    path = os.path.join(os.getcwd(), "dags/temp/health_clust.csv")
    data = health_clust.to_csv(path, index=False)


def check_dateOutOfBound(values):
    if pd.notna(values):
        if (pd.Timestamp.max < values) | (values < pd.Timestamp.min):
            return pd.to_datetime(pd.to_datetime(values.replace(year=(values.year-543)), errors='coerce').isoformat(sep=' ', timespec='seconds'))
        else:
            return pd.to_datetime(pd.to_datetime(values, errors='coerce').isoformat(sep=' ', timespec='seconds'))


col_date_list = list(df.select_dtypes(include=['datetime']).columns)
col_date_list.append('ts')
for type_l in col_date_list:
    df[type_l] = df[type_l].apply(check_dateOutOfBound)
