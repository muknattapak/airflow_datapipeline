from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python import ShortCircuitOperator
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.providers.redis.hooks.redis import RedisHook

import pandas as pd
import numpy as np
import os
import pendulum
import redis
from datetime import datetime, timedelta
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine, DDL, Column, Integer, String, Float, DateTime, Sequence, Text
# from func.database_post_connect import DatabasePostgConnect
# from func.database_connect import DatabaseConnect
# import constant

from pymongo import MongoClient
from bson import ObjectId

local_tz = pendulum.timezone("Asia/Bangkok")

# mongoexport -h ddb.thailand-smartliving.com:38200 --authenticationDatabase=aidery-health -u airflow -p 98ZaEVYxq758h2tk -d aidery-health -c survey-answers --fields=_id,userId,ts,response,crt --type=json --out=/Users/muk/symtoms.json


def is_latest_id(**kwargs):
    # r = redis.Redis(host='airflow_redis_1', port=6379, db=1)
    SOURCES_REDIS_CONN_ID = "redisCon"
    redis_conn = RedisHook(redis_conn_id=SOURCES_REDIS_CONN_ID)
    red_db = redis_conn.get_conn()
    latestId = red_db.get('latestOfHealthRecords')

    SOURCES_MONGO_DB_CONN_ID = "mongoAuth"
    SOURCES_MONGO_DB = "aidery-health"
    SOURCES_MONGO_COLLECTION = "health-records"

    mongoAuth = MongoHook(conn_id=SOURCES_MONGO_DB_CONN_ID)
    healthRecColl = mongoAuth.get_collection(
        SOURCES_MONGO_COLLECTION, SOURCES_MONGO_DB)

    # client = MongoClient(
    #     "mongodb://airflow:98ZaEVYxq758h2tk@ddb.thailand-smartliving.com:38200/aidery-health")

    # authDbH = client['aidery-health']
    # healthColl = authDbH['health-records']

    latest_record = pd.DataFrame(list(healthRecColl.find({'type': {'$in': [
                                 4, 6, 7, 8, 9, 10, 13, 14, 19, 21]}}).sort('_id', -1).limit(1)))
    print('latest_record:', latest_record)
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


# thrsh = thr_ls
# tmp = data.loc[data['type'] == type_l, 'value'].to_list()
# values = tmp[0]

def cal_threshold(values, thrsh):
    key_l = ['abnormal', 'urgent', 'emergency']
    # print('values:', values)
    for item in key_l:
        # item = 'abnormal'
        g_level = 'none'
        for level_l in retrieve_nested_value(thrsh['low'], item):
            # existing keys min, max check in nested dict
            if ('min' in level_l) & ('max' in level_l):
                if (thrsh['low'].str.get(item).str.get('min').notna() | thrsh['low'].str.get(item).str.get('max').notna()).all():
                    if (level_l['min'] <= values <= level_l['max']):
                        g_level = item
            elif ('max' in level_l):
                if thrsh['low'].str.get(item).str.get('max').notna().all():
                    if (level_l['max'] >= values):
                        g_level = item

        for level_l in retrieve_nested_value(thrsh['high'], item):
            if ('min' in level_l) & ('max' in level_l):
                if (thrsh['high'].str.get(item).str.get('min').notna() | thrsh['high'].str.get(item).str.get('max').notna()).all():
                    if (level_l['min'] <= values <= level_l['max']):
                        g_level = item
            elif ('min' in level_l):
                if thrsh['high'].str.get(item).str.get('min').notna().all():
                    if (level_l['min'] <= values):
                        g_level = item

        if (g_level != 'none'):
            break

    if (g_level == 'none'):
        g_level = 'normal'

    # print(values, ' ', g_level)
    return g_level


def new_nest_dict(m):
    for val in m.values():
        if isinstance(val, dict):
            yield from new_nest_dict(val)
        else:
            yield val


def get_data():
    SOURCES_REDIS_CONN_ID = "redisCon"
    redis_conn = RedisHook(redis_conn_id=SOURCES_REDIS_CONN_ID)
    red_db = redis_conn.get_conn()

    SOURCES_MONGO_DB_CONN_ID = "mongoAuth"
    SOURCES_MONGO_DB_H = "aidery-health"
    SOURCES_MONGO_DB_A = "aidery-auth"
    SOURCES_MONGO_HEALTHCOLL = "health-records"
    SOURCES_MONGO_SETTINGCOLL = "settings"
    SOURCES_MONGO_SETTINGMASTERCOLL = "master-setting"
    SOURCES_MONGO_USERCOLL = "users"

    mongoAuth = MongoHook(conn_id=SOURCES_MONGO_DB_CONN_ID)
    healthColl = mongoAuth.get_collection(
        SOURCES_MONGO_HEALTHCOLL, SOURCES_MONGO_DB_H)
    settingColl = mongoAuth.get_collection(
        SOURCES_MONGO_SETTINGCOLL, SOURCES_MONGO_DB_H)
    mastersettingColl = mongoAuth.get_collection(
        SOURCES_MONGO_SETTINGMASTERCOLL, SOURCES_MONGO_DB_H)
    userColl = mongoAuth.get_collection(
        SOURCES_MONGO_USERCOLL, SOURCES_MONGO_DB_A)

    # client = MongoClient(
    #     "mongodb://airflow:98ZaEVYxq758h2tk@ddb.thailand-smartliving.com:38200/aidery-health")

    # authDbH = client['aidery-health']
    # authDbA = client['aidery-auth']

    # healthColl = authDbH['health-records']
    # settingColl = authDbH['settings']
    # mastersettingColl = authDbH['master-setting']
    # userColl = authDbA['users']

    latestId = red_db.get('latestOfHealthRecords')
    print('LatestId:', latestId)
    # latestId = '5f3d00fd9321ac0011fafa41' # ObjectId("5f3d00fd9321ac0011fafa41")

    limit_row = 10000
    # read health record
    if (latestId):
        cursor = healthColl.find(
            {'_id': {'$gt': ObjectId(latestId.decode())}}).sort('_id', 1).limit(limit_row)
        # cursor = healthColl.find({'_id': {'$gt': ObjectId(latestId)}}).sort('_id', 1).limit(limit_row)
    else:
        cursor = healthColl.find().sort('_id', 1).limit(limit_row)

    temp_data = pd.DataFrame(list(cursor))

    if 'id' not in temp_data.columns:
        temp_data['id'] = np.nan

    if 'source' not in temp_data.columns:
        temp_data['source'] = np.nan

    temp_data.rename(columns={'from': 'start', 'to': 'end'}, inplace=True)
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
            # print('settings')
            cursorU = userColl.find(
                {'_id': ObjectId(user_list[uid])}, {'_id', 'domainId'})
            users_ls = pd.DataFrame(list(cursorU))

            # setting
            cursor = settingColl.find({'userId': ObjectId(user_list[uid])}).sort(
                '_id', 1).limit(1000)

        else:
            # print('mastersetting')
            cursorU = userColl.find(
                {'_id': ObjectId(user_list[uid])}, {'_id', 'domainId'})
            users_ls = pd.DataFrame(list(cursorU))

            # setting
            cursor = mastersettingColl.find(
                {'domainId': str(users_ls['domainId'][0])}).sort('_id', 1).limit(1000)
            if (mastersettingColl.count_documents({'domainId': str(users_ls['domainId'][0])}) <= 0):
                # print('mastersetting2')
                cursor = mastersettingColl.find(
                    {'domainId': {'$exists': False}, 'category': 'health'}).sort('_id', 1).limit(1000)

        temp_thresh = pd.DataFrame(list(cursor))
        thresh = temp_thresh[['_id', 'type', 'low', 'high']].copy()
        # condition setting
        data = healthData[healthData['userId'] == user_list[uid]].copy()
        type_list = data['type'].drop_duplicates(
        ).reset_index(drop=True).values.tolist()

        for type_l in type_list:
            # check health data type is in threshold data type
            if type_l in thresh['type']:
                # select health threshold from table threshold
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
                        data.loc[data['type'] == type_l, 'label'] = data.loc[data['type'] == type_l, 'value'].apply(
                            cal_threshold, thrsh=thr_ls)
            else:
                data.loc[data['type'] == type_l, 'label'] = 'none'

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
        health_label = pd.concat((health_label, data),
                                 axis=0, ignore_index=True)

    health_label = health_label.where(pd.notnull(health_label), None)

    path = os.path.join(os.getcwd(), "dags/temp/health_labeld.csv")
    data = health_label.to_csv(path, index=False)


def load_data():
    path = os.path.join(os.getcwd(), "dags/temp/health_labeld.csv")
    # try:
    data_csv = pd.read_csv(path)
    # except:
    #     exit(1)
    convert_dict = {
        'userId': 'string',
        'domainId': 'string',
        'label': 'string',
        'source': 'string',
        'crt': 'datetime64[ns]',
        'mdt': 'datetime64[ns]',
        'start': 'datetime64[ns]',
        'end': 'datetime64[ns]',
        'value': float,
        'value2': float,
        'value3': float,
        'provider': 'Int64',
        'type': 'Int64',
        'birth_year': 'Int64',
        'userEntered': 'bool'
    }

    col_list = list(data_csv.columns)
    for col in col_list:
        if col in convert_dict:
            data_csv = data_csv.astype({col: convert_dict[col]})

    data_csv.columns = [c.lower() for c in data_csv.columns]
    data_csv.reset_index(inplace=True, drop=True)

    def connection():
        engine = create_engine(
            'postgresql://airflow:airflow@es.aidery.io:5433/postgres')
        return engine

    engine = connection()

    table_name = 'health_records'

    # conn = DatabasePostgConnect()  # new postgres db connection class
    # db = conn.getPostgDB()

    Base = declarative_base()

    class CreateTable(Base):
        __tablename__ = table_name
        seq = Sequence('health_idx_seq', start=1, increment=1)
        idx = Column('idx', Integer, seq,
                     server_default=seq.next_value(), primary_key=True)
        _id = Column('_id', Text, nullable=False)
        provider = Column('provider', Integer)
        id = Column('id', String)
        userid = Column('userid', String)
        domainid = Column('domainid', String)
        birth_year = Column('birth_year', Integer)
        type = Column('type', Integer)
        start = Column('start', DateTime)
        end = Column('end', DateTime)
        value = Column('value', Float)
        value2 = Column('value2', Float)
        value3 = Column('value3', Float)
        source = Column('source', String)
        crt = Column('crt', DateTime)
        mdt = Column('mdt', DateTime)
        label = Column('label', String)

    # Base.metadata.create_all(engine)
    Base.metadata.create_all(engine)

    # -------------- check existing column name in Postgres ------------------
    # table_name = '\'health_records\''
    query = DDL(
        " ".join(map(str, ['select * from ' + table_name + ' limit 1'])))
    colname = list(engine.execute(query).keys())

    col_post = colname[1:]
    col_unknown = list(set(data_csv.columns.to_list()) - set(col_post))

    if len(col_unknown) > 0:
        for col in col_unknown:
            if data_csv[col].dtype.name == 'int64':
                col_type = 'integer'
            elif data_csv[col].dtype.name == 'datetime64[ns]':
                col_type = 'timestamp'
            elif data_csv[col].dtype.name == 'float64':
                col_type = 'double precision'
            else:
                col_type = 'text'

            add_col = DDL(" ".join(
                map(str, ['alter table ' + table_name + ' add column ' + col + ' ' + col_type])))
            engine.execute(add_col)

    colname = list(engine.execute(query).keys())

    # insert data to DWH
    data_csv.to_sql(name=table_name, if_exists='append',
                    con=engine, chunksize=10, method='multi', index=False)

    SOURCES_REDIS_CONN_ID = "redisCon"
    redis_conn = RedisHook(redis_conn_id=SOURCES_REDIS_CONN_ID)
    red_db = redis_conn.get_conn()

    red_db.set('latestOfHealthRecords', data_csv['_id'].tail(
        1).to_string(index=False))


default_args = {
    'owner': 'ZG_DAP',
    'depends_on_past': False,
    'start_date': datetime(2022, 11, 28,  tzinfo=local_tz),
    # 'email': ['@gmail.com'],
    # 'email_on_failure': True,
    # 'email_on_retry': True,
    # 'retries': 3,
    'schedule_interval': '@hourly',
    'retry_delay': timedelta(seconds=5),
}

with DAG('collect_health_records',
         default_args=default_args,
         tags=['mongo', 'postgres', 'healthrecords'],
         schedule_interval='@hourly') as dag:

    is_latest = ShortCircuitOperator(
        task_id='is_latest',
        python_callable=is_latest_id,
        ignore_downstream_trigger_rules=True
    )

    t1 = PythonOperator(
        task_id='t1_read_mongo',
        provide_context=True,
        python_callable=get_data
    )

    t2 = PythonOperator(
        task_id='t2_load_postgres',
        provide_context=True,
        python_callable=load_data
    )

    is_latest >> t1 >> t2
