
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


def is_latest_id(**kwargs):
    r = redis.Redis(host='airflow_redis_1', port=6379, db=1)

    client = MongoClient(
        "mongodb://airflow:98ZaEVYxq758h2tk@ddb.thailand-smartliving.com:38200/aidery-health")

    health_db = client['aidery-health']
    healthColl = health_db['health-records']
    latestId = r.get('latestOfHealthRecords')
    print('LatestId:', latestId)

    latest_record = pd.DataFrame(list(healthColl.find({'type': {'$in': [
                                 4, 7]}}, {'_id': 1}).sort('_id', -1).limit(1)))

    if latestId is not None:
        # have new data in mongo
        if latest_record['_id'][0] > ObjectId(latestId.decode()):
            return True  # read new data
        else:
            return False  # skip read data
    else:
        return True


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

def cal_hr_zone(user_prf):
    if 'age' in user_prf.columns:
        if 'gender' in user_prf.columns:
            if (user_prf['gender'] == 'male').all():
                maxHR = 214 - (0.8*user_prf['age'])
            else:
                maxHR = 209 - (0.7*user_prf['age'])
        else:
            maxHR = 220 - user_prf['age']
    else:       
        maxHR = None
    data = {'zone': [1,2,3,4,5],
        'z_name': ['very light', 'light', 'moderate', 'hard', 'maximum']}
    hr_table = pd.DataFrame(data)
    hr_table['hrUpper'] = np.nan
    hr_table['hrLower'] = np.nan
    Hlower= [50,60,70,80,90]
    Hupper= [60,70,80,90,100]
    for i in range(len(hr_table)):
        hr_table.loc[i, 'hrUpper'] = maxHR[0] * Hupper[i]/100
        hr_table.loc[i, 'hrLower'] = maxHR[0] * Hlower[i]/100
    
    return hr_table

def get_data():
    r = redis.Redis(host='airflow_redis_1', port=6379, db=1)

    client = MongoClient(
        "mongodb://airflow:98ZaEVYxq758h2tk@ddb.thailand-smartliving.com:38200/aidery-health")

    aid_health = client['aidery-health']
    aid_auth = client['aidery-auth']

    healthColl = aid_health['health-records']

    # aidery-auth
    userColl = aid_auth['users']

    latestId = r.get('latestOfHealthRecords')
    # print('LatestId:', latestId)
    # latestId = '5eddc0e08b040a001862c3b7'

    limit_row = 10000
    # read health record
    if (latestId):
        cursor = healthColl.find(
            {'_id': {'$gt': ObjectId(latestId.decode())}, 'type': {'$in': [
                                 4, 7]}}).sort('_id', 1).limit(limit_row)
        # cursor = healthColl.find(
        #     {'_id': {'$gt': ObjectId(latestId)}}).sort('_id', 1).limit(limit_row)
    else:
        cursor = healthColl.find({'type': {'$in': [4, 7]}}).sort('_id', 1).limit(limit_row)

    temp_data = pd.DataFrame(list(cursor))

    if 'id' not in temp_data.columns:
        temp_data['id'] = np.nan

    if 'source' not in temp_data.columns:
        temp_data['source'] = np.nan

    healthData = temp_data.copy().reset_index(drop=True)
    # unique users
    user_list = healthData['userId'].copy().drop_duplicates(
    ).reset_index(drop=True)  # get list of userid

    # ==== # calculate by user id ==========
    health_grp = pd.DataFrame()
    for uid in range(len(user_list)):
        # read threshold
        # ==== read users dataset ======
        cursorU = userColl.find({'_id': ObjectId(user_list[uid])}, {
                                '_id', 'domainId', 'birthday', 'gender'})
        users_ls = pd.DataFrame(list(cursorU))
        if 'birthday' in users_ls.columns:
            users_ls['birth_year'] = users_ls['birthday'].apply(
                lambda x: x.year)
            users_ls.drop(['birthday'], axis=1, inplace=True)
        else:
            users_ls['birth_year'] = None

        if 'gender' not in users_ls.columns:
            users_ls['gender'] = None

        users_ls['_id'] = users_ls['_id'].astype(str)
        users_ls['age'] = int(date.today().year - int(users_ls['birth_year']))
      
        # split user 
        data = healthData[healthData['userId'] == user_list[uid]].copy()
        type_list = data['type'].drop_duplicates(
        ).reset_index(drop=True).values.tolist()

        hr_zone = cal_hr_zone(users_ls)

        data = 

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
                    # print("All Elements in List are Equal")
                    data['label'] = data['value'].apply(
                        cal_threshold, thrsh=thr_ls)

        # ==== read users dataset ======
        cursorU = userColl.find({'_id': ObjectId(user_list[uid])}, {
                                '_id', 'domainId', 'birthday'})
        users_ls = pd.DataFrame(list(cursorU))
        if 'birthday' in users_ls.columns:
            users_ls['birth_year'] = users_ls['birthday'].apply(
                lambda x: x.year)
            users_ls.drop(['birthday'], axis=1, inplace=True)
        else:
            users_ls['birth_year'] = None

        users_ls['_id'] = users_ls['_id'].astype(str)
        data = data.merge(users_ls, how='left', left_on='userId',
                          right_on='_id', suffixes=('', '_y'))
        data.drop(data.filter(regex='_y$').columns, axis=1, inplace=True)
        health_label = health_label.append(data, ignore_index=True)

    health_label = health_label.where(pd.notnull(health_label), None)

    path = os.path.join(os.getcwd(), "dags/temp/health_labeld.csv")
    data = health_label.to_csv(path, index=False)

    
    
