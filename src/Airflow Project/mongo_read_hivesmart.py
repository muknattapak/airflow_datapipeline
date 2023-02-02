from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python import ShortCircuitOperator
import pendulum
from pymongo import MongoClient
from bson import ObjectId
from datetime import datetime, timedelta, date
import pandas as pd
import numpy as np
import os
from sqlalchemy import create_engine, Table, Column, MetaData, Integer, String, Float, DateTime, Sequence, Text
from sqlalchemy.ext.declarative import declarative_base
import redis

local_tz = pendulum.timezone("Asia/Bangkok")


def is_latest_id(**kwargs):
    r = redis.Redis(host='airflow_redis_1', port=6379, db=1)

    client = MongoClient(
        "mongodb://aidery:qLRV9wyaQ2R9vnTy@dev.zanegrowth.com:38201/admin")

    aid_admin = client['aidery-hive']
    hiveColl = aid_admin['hivesmart']
    latestId = r.get('latestOfHivesmart')
    print('LatestId:', latestId)

    latest_record = pd.DataFrame(
        list(hiveColl.find({}, {'_id': 1}).sort('_id', -1).limit(1)))

    if latestId is not None:
        if latest_record['_id'][0] > ObjectId(latestId.decode()):
            return True
        else:
            return False
    else:
        return True


def get_data():
    r = redis.Redis(host='airflow_redis_1', port=6379, db=1)

    client = MongoClient(
        "mongodb://aidery:qLRV9wyaQ2R9vnTy@dev.zanegrowth.com:38201/admin")

    aid_admin = client['aidery-hive']
    hiveColl = aid_admin['hivesmart']
    latestId = r.get('latestOfHivesmart')
    # print('LatestId:', latestId)

    limit_row = 10000
    # read health record
    if (latestId):
        cursor = hiveColl.find(
            {'_id': {'$gt': ObjectId(latestId.decode())}}).sort('_id', 1).limit(limit_row)
        # cursor = healthColl.find().sort('_id', 1).limit(limit_row)
    else:
        cursor = hiveColl.find({}).sort(
            '_id', 1).sort('_id', 1).limit(limit_row)

    temp_data = pd.DataFrame(list(cursor))
    hiveData = temp_data.copy().reset_index(drop=True)
    hiveData = pd.concat([hiveData.drop('data', axis=1),
                          hiveData['data'].apply(pd.Series).add_prefix('data_')], axis=1)
    hiveData = pd.concat([hiveData.drop('data_acc', axis=1),
                          hiveData['data_acc'].apply(pd.Series).add_prefix('acc_')], axis=1)
    hiveData = pd.concat([hiveData.drop('data_gyro', axis=1),
                          hiveData['data_gyro'].apply(pd.Series).add_prefix('gyro_')], axis=1)
    hiveData.rename({'acc_0': 'acc_x', 'acc_1': 'acc_y', 'acc_2': 'acc_z', 
                    'gyro_0': 'gyro_x', 'gyro_1': 'gyro_y', 'gyro_2': 'gyro_z'}, axis=1, inplace=True)
    hiveData.columns = hiveData.columns.map(str)
    hiveData['dts'] = pd.to_datetime(hiveData['dts'], unit='ms').astype('datetime64[ns, UTC]').dt.tz_convert('Asia/Bangkok')
    hiveData['dts'] = pd.to_datetime((hiveData['dts'].dt.strftime('%Y-%m-%d %H:%M:%S.%f'))) #('%Y-%m-%d %H:%M:%S.%f')[:-3]
    hiveData['data_unixtime'] = pd.to_datetime(hiveData['data_unixtime'], unit='ms').astype('datetime64[ns, UTC]').dt.tz_convert('Asia/Bangkok')
    hiveData['data_unixtime'] = pd.to_datetime((hiveData['data_unixtime'].dt.strftime('%Y-%m-%d %H:%M:%S.%f')))
    path = os.path.join(os.getcwd(), "dags/temp/selected_data3/hivesmart.csv")
    data = hiveData.to_csv(path, index=False)


def load_data():
    path = os.path.join(os.getcwd(), "dags/temp/selected_data3/hivesmart.csv")
    data_csv = pd.read_csv(path)
    # data_csv[['crt', 'mdt']] = data_csv[['crt', 'mdt']].astype('datetime64[ns]')

    convert_datetime = {
        'dts': 'datetime64[ns]',
        'data_unixtime': 'datetime64[ns]'
    }

    convert_integer = {
        'seq': int,
        'data_step_count': int,
        'data_calorie': int,
        'data_hr': int,
        'data_spo2': int,
        'data_alert': int,
        'data_batt': int,
        'acc_x': int,
        'acc_y': int,
        'acc_z': int,
        'gyro_x': int,
        'gyro_y': int,
        'gyro_z': int
    }

    convert_string = {
        '_id': 'string',
        'device_id': 'string'
    }

    data_csv = data_csv.astype(convert_datetime)
    data_csv = data_csv.astype(convert_integer)
    data_csv = data_csv.astype(convert_string)

    data_csv.columns = [c.lower() for c in data_csv.columns]
    data_csv.reset_index(inplace=True, drop=True)

    Base = declarative_base()

    table_name = 'hivesmart'

    def connection():
        engine = create_engine(
            'postgresql://airflow:airflow@es.aidery.io:5433/postgres')
        return engine

    engine = connection()

    class CreateTable(Base):
        __tablename__ = table_name
        seq = Sequence(table_name+'_idx_seq', start=1, increment=1)
        idx = Column('idx', Integer, seq,
                     server_default=seq.next_value(), primary_key=True)
        _id = Column('_id', Text, nullable=False)
        dts = Column('dts', DateTime)
        data_unixtime = Column('data_unixtime', DateTime)
        device_id = Column('device_id', String)
        seq = Column('seq', Integer)
        data_step_count = Column('data_step_count', Integer)
        data_calorie = Column('data_calorie', Integer)
        data_pressure = Column('data_pressure', Float)
        data_hr = Column('data_hr', Integer)
        data_spo2 = Column('data_spo2', Integer)
        data_alert = Column('data_alert', Integer)
        data_batt = Column('data_batt', Integer)
        acc_x = Column('acc_x', Integer)
        acc_y = Column('acc_y', Integer)
        acc_z = Column('acc_z', Integer)
        gyro_x = Column('gyro_x', Integer)
        gyro_y = Column('gyro_y', Integer)
        gyro_z = Column('gyro_z', Integer)

    Base.metadata.create_all(engine)

    data_csv.to_sql(name=table_name, if_exists='append',
                    con=engine, chunksize=10, method='multi', index=False)

    # data_count = data_csv.shape[0]
    # print('data',data_count)

    r = redis.Redis(host='airflow_redis_1', port=6379, db=1)
    r.set('latestOfHivesmart', data_csv['_id'].tail(1).to_string(index=False))


default_args = {
    'owner': 'zg',
    'depends_on_past': False,
    'start_date': datetime(2022, 11, 7,  tzinfo=local_tz),
    # 'email': ['@gmail.com'],
    # 'email_on_failure': True,
    # 'email_on_retry': True,
    # 'retries': 3,
    'retry_delay': timedelta(seconds=5),
}


with DAG('collects_hivesmart',
         default_args=default_args,
         schedule_interval=timedelta(hours=2), 
         tags=['mongo','postgres','hivesmart']) as dag:

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