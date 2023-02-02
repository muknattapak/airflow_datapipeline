from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.providers.redis.hooks.redis import RedisHook
from airflow.operators.python import ShortCircuitOperator
from datetime import datetime, timedelta
from bson import ObjectId
from pymongo import MongoClient
import numpy as np

import pendulum

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine, Table, Column, MetaData, Integer, String, Float, DateTime, Sequence, Text
from dataclasses import replace
import pandas as pd
import os
# import sys
# import redis

local_tz = pendulum.timezone("Asia/Bangkok")

# mongoexport -h ddb.thailand-smartliving.com:38200 --authenticationDatabase=aidery-health -u airflow -p 98ZaEVYxq758h2tk -d aidery-health -c survey-answers --fields=_id,userId,ts,response,crt --type=json --out=/Users/muk/symtoms.json


def is_latest_id(**kwargs):
    SOURCES_REDIS_CONN_ID = "redisCon"
    redis_conn = RedisHook(redis_conn_id=SOURCES_REDIS_CONN_ID)
    red_db = redis_conn.get_conn()
    latestId = red_db.get('latestOfUsers')
    print('LatestId:', latestId)

    SOURCES_MONGO_DB_CONN_ID = "mongoAuth"
    SOURCES_MONGO_DB = "aidery-auth"
    SOURCES_MONGO_COLLECTION = "users"

    mongoAuth = MongoHook(conn_id=SOURCES_MONGO_DB_CONN_ID)
    usersColl = mongoAuth.get_collection(
        SOURCES_MONGO_COLLECTION, SOURCES_MONGO_DB)

    # client = MongoClient(
    #     "mongodb://airflow:98ZaEVYxq758h2tk@ddb.thailand-smartliving.com:38200/aidery-health")

    # authDbH = client['aidery-auth']
    # usersColl = authDbH['users']

    latest_record = pd.DataFrame(
        list(usersColl.find({'role': {'$in': [1, 3, 12, 13]}, 'status': {'$gt': -1}}, {'_id': 1}).sort('_id', -1).limit(1)))
    # print('latest_record:', latest_record)

    if latestId is not None:
        # have new data in mongo
        if latest_record['_id'][0] > ObjectId(latestId.decode()):
            return True  # read new data
        else:
            return False  # skip read data
    else:
        return True


def get_data():
    SOURCES_REDIS_CONN_ID = "redisCon"
    redis_conn = RedisHook(redis_conn_id=SOURCES_REDIS_CONN_ID)
    red_db = redis_conn.get_conn()

    SOURCES_MONGO_DB_CONN_ID = "mongoAuth"
    SOURCES_MONGO_DB = "aidery-auth"
    SOURCES_MONGO_COLLECTION = "users"

    mongoAuth = MongoHook(conn_id=SOURCES_MONGO_DB_CONN_ID)
    usersColl = mongoAuth.get_collection(
        SOURCES_MONGO_COLLECTION, SOURCES_MONGO_DB)

    # client = MongoClient(
    #     "mongodb://airflow:98ZaEVYxq758h2tk@ddb.thailand-smartliving.com:38200/aidery-health")

    # aid_auth = client['aidery-auth']
    # # aidery-auth
    # usersColl = aid_auth['users']

    latestId = red_db.get('latestOfUsers')
    # latestId = '638466788740660019fd72d2'
    print('LatestId:', latestId)

    row_lim = 10000
    if (latestId):
        cursor = usersColl.find(
            {'role': {'$in': [1, 3, 12, 13]}, 'status': {'$gt': -1},
             #  'crt': {'$gte': datetime.min, '$lte': datetime.max},
             '_id': {'$gt': ObjectId(latestId.decode())}}, {'meta': 0}).sort('_id', 1).limit(row_lim)
        # cursor = usersColl.find(
        #     {'role': {'$in': [1, 3, 12, 13]}, 'status': {'$gt': -1},
        #      '_id': {'$gt': ObjectId(latestId)}}, {'meta': 0}).sort('_id', 1).limit(row_lim)

    else:
        cursor = usersColl.find(
            {'role': {'$in': [1, 3, 12, 13]}, 'status': {'$gt': -1}}, {'meta': 0}).sort('_id', 1).limit(row_lim)
        # 'swabDate': {'$gte': datetime.min, '$lte': datetime.max}

    list_cur = list(cursor)

    col_list = ['_id', 'domainId', 'firstName', 'lastName', 'role', 'birthday',
                'gender', 'nationalId', 'phone', 'address',
                'diseases', 'allergy', 'features', 'height',  'weight',
                'status', 'crt', 'families', 'caregivers', 'surgeries',
                'covidTest', 'swabDate']
    if list_cur:
        temp_data = pd.DataFrame(list_cur)
        lost_col = pd.Series(col_list).isin(temp_data.columns) == False
        lost_col_idx = [i for i, x in enumerate(lost_col) if x]
        if (lost_col.any()):  # add lost column name and fill with nan value
            for j in range(len(lost_col_idx)):
                s_idx = [i for i in lost_col.index if lost_col[i]].pop(j)
                temp_data[col_list[s_idx]] = temp_data.get(
                    col_list[s_idx], np.nan)

        temp_data = temp_data[col_list]

    df = temp_data.copy(deep=True).reset_index(drop=True)
    df = pd.concat([df.drop('allergy', axis=1),
                    df['allergy'].apply(pd.Series)], axis=1)

    df.columns = df.columns.map(str)
    if '0' in df.columns:
        df = df.drop(['0'], axis=1)

    path = os.path.join(os.getcwd(), "dags/temp/users-test.csv")
    df.to_csv(path, index=False)


def load_data():
    path = os.path.join(os.getcwd(), "dags/temp/users-test.csv")
    data_csv = pd.read_csv(path)
    data_csv[['crt', 'swabDate', 'birthday']] = data_csv[[
        'crt', 'swabDate', 'birthday']].astype('datetime64[ns]')
    data_csv[['nationalId']] = data_csv[['nationalId']].astype('string')
    data_csv.columns = [c.lower() for c in data_csv.columns]
    data_csv.reset_index(inplace=True, drop=True)

    Base = declarative_base()

    def connection():
        engine = create_engine(
            'postgresql://airflow:airflow@es.aidery.io:5433/postgres')
        return engine

    engine = connection()

    class CreateTable(Base):
        __tablename__ = 'users'
        seq = Sequence('users_idx_seq', start=1, increment=1)
        idx = Column('idx', Integer, seq,
                     server_default=seq.next_value(), primary_key=True)
        _id = Column('_id', Text, nullable=False)
        domainid = Column('domainid', String)
        firstname = Column('firstname', String)
        lastname = Column('lastname', String)
        role = Column('role', Integer)
        birthday = Column('birthday', DateTime)
        gender = Column('gender', String)
        nationalid = Column('nationalid', String)
        phone = Column('phone', String)
        address = Column('address', String)
        diseases = Column('diseases', String)
        allergy = Column('allergy', String)
        drug = Column('drug', String)
        food = Column('food', String)
        features = Column('features', String)
        height = Column('height', Float)
        weight = Column('weight', Float)
        status = Column('status', Integer)
        crt = Column('crt', DateTime)
        families = Column('families', String)
        caregivers = Column('caregivers', String)
        surgeries = Column('surgeries', String)
        covidtest = Column('covidtest', String)
        swabdate = Column('swabdate', DateTime)

    Base.metadata.create_all(engine)

    convert_dict = {'height': float,
                    'weight': float
                    }
    data_csv = data_csv.astype(convert_dict)

    data_csv.to_sql(name='users', if_exists='append',
                    con=engine, chunksize=10, method='multi', index=False)

    # data_count = data_csv.shape[0]
    # print('data',data_count)

    SOURCES_REDIS_CONN_ID = "redisCon"
    redis_conn = RedisHook(redis_conn_id=SOURCES_REDIS_CONN_ID)
    red_db = redis_conn.get_conn()
    red_db.set('latestOfUsers', data_csv['_id'].tail(1).to_string(index=False))


default_args = {
    'owner': 'ZG_DAP',
    'depends_on_past': False,
    'start_date': datetime(2022, 11, 24,  tzinfo=local_tz),
    # 'email': ['@gmail.com'],
    # 'email_on_failure': True,
    # 'email_on_retry': True,
    # 'retries': 3,
    # 'retry_delay': timedelta(seconds=5)
}

with DAG('collect_user',
         default_args=default_args,
         tags=['mongo', 'postgres', 'users'],
         schedule_interval=timedelta(hours=2)
         #  schedule_interval='@hourly'
         ) as dag:

    is_latest = ShortCircuitOperator(
        task_id='is_latest',
        python_callable=is_latest_id,
        ignore_downstream_trigger_rules=True
    )

    t1 = PythonOperator(
        task_id='t1_get_mongo',
        provide_context=True,
        python_callable=get_data
    )

    t2 = PythonOperator(
        task_id='t2_load_postgres',
        provide_context=True,
        python_callable=load_data
    )

    is_latest >> t1 >> t2
