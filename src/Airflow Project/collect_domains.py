from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.providers.redis.hooks.redis import RedisHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import ShortCircuitOperator
import pendulum
from pymongo import MongoClient
from bson import ObjectId
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import os
from sqlalchemy import create_engine, DDL, Column, Integer, String, DateTime, Sequence, Text
from sqlalchemy.ext.declarative import declarative_base
from dataclasses import replace
import redis

local_tz = pendulum.timezone("Asia/Bangkok")


def is_latest_id(**kwargs):
    SOURCES_REDIS_CONN_ID = "redisCon"
    redis_conn = RedisHook(redis_conn_id=SOURCES_REDIS_CONN_ID)
    red_db = redis_conn.get_conn()
    # red_db = redis.Redis(host='airflow_redis_1', port=6379, db=1)
    latestId = red_db.get('latestOfDomains')
    print('latestId:', latestId)

    SOURCES_MONGO_DB_CONN_ID = "mongoAuth"
    SOURCES_MONGO_DB = "aidery-auth"
    SOURCES_MONGO_COLLECTION = "domains"

    mongoAuth = MongoHook(conn_id=SOURCES_MONGO_DB_CONN_ID)
    domaColl = mongoAuth.get_collection(SOURCES_MONGO_COLLECTION,SOURCES_MONGO_DB)

    # client = MongoClient(
    #     "mongodb://airflow:98ZaEVYxq758h2tk@ddb.thailand-smartliving.com:38200/aidery-health")

    # aid_auth = client['aidery-auth']
    # # aidery-auth
    # domaColl = aid_auth['domains']

    latest_record = pd.DataFrame(
        list(domaColl.find({}, {'_id': 1}).sort('_id', -1).limit(1)))

    if latestId is not None:
        if latest_record['_id'][0] > ObjectId(latestId.decode()):
            return True
        else:
            return False
    else:
        return True


def get_data():
    
    SOURCES_REDIS_CONN_ID = "redisCon"
    redis_conn = RedisHook(redis_conn_id=SOURCES_REDIS_CONN_ID)
    red_db = redis_conn.get_conn()
    # red_db = redis.Redis(host='airflow_redis_1', port=6379, db=1)
    latestId = red_db.get('latestOfDomains')
    print('latestId:', latestId)

    SOURCES_MONGO_DB_CONN_ID = "mongoAuth"
    SOURCES_MONGO_DB = "aidery-auth"
    SOURCES_MONGO_COLLECTION = "domains"

    mongoAuth = MongoHook(conn_id=SOURCES_MONGO_DB_CONN_ID)
    domaColl = mongoAuth.get_collection(SOURCES_MONGO_COLLECTION,SOURCES_MONGO_DB)
    
    # red_db = redis.Redis(host='airflow_redis_1', port=6379, db=1)

    # client = MongoClient(
    #     "mongodb://airflow:98ZaEVYxq758h2tk@ddb.thailand-smartliving.com:38200/aidery-health")

    # aid_auth = client['aidery-auth']
    # # aidery-auth
    # domaColl = aid_auth['domains']

    # latestId = red_db.get('latestOfDomains')
    # # print('LatestId:', latestId)

    limit_row = 500

    if (latestId):
        cursor = domaColl.find(
            {'_id': {'$gt': ObjectId(latestId.decode())}}).sort('_id', 1).limit(limit_row)
        # cursor = healthColl.find().sort('_id', 1).limit(limit_row)
    else:
        cursor = domaColl.find({}).sort(
            '_id', 1).sort('_id', 1).limit(limit_row)

    temp_data = pd.DataFrame(list(cursor))
    if 'parentId' not in temp_data.columns:
        domaData = temp_data[['_id', 'crt', 'mdt', 'name', 'domainName', 'type', 'status',
                              'hospitals', 'location', 'address']].copy().reset_index(drop=True)
        domaData['parentId'] = np.nan
    else:
        domaData = temp_data[['_id', 'crt', 'mdt', 'name', 'domainName', 'type', 'status',
                              'hospitals', 'location', 'address', 'parentId']].copy().reset_index(drop=True)

    domaData = pd.concat([domaData.drop('location', axis=1),
                          domaData['location'].apply(pd.Series).add_prefix('loc_')], axis=1)
    domaData = pd.concat([domaData.drop('address', axis=1),
                          domaData['address'].apply(pd.Series).add_prefix('add_')], axis=1)
    domaData.columns = domaData.columns.map(str)
    if 'loc_0' in domaData.columns:
        domaData = domaData.drop(['loc_0'], axis=1)

    if 'add_0' in domaData.columns:
        domaData = domaData.drop(['add_0'], axis=1)
    domaData = domaData.where(pd.notnull(domaData), None)

    path = os.path.join(os.getcwd(), "dags/temp/domains.csv")
    domaData.to_csv(path, index=False)


def load_data():
    path = os.path.join(os.getcwd(), "dags/temp/domains.csv")
    data_csv = pd.read_csv(path)
    # data_csv[['crt', 'mdt']] = data_csv[['crt', 'mdt']].astype('datetime64[ns]')

    convert_dict = {
        'hospitals': 'string',
        'loc_type': 'string',
        'loc_coordinates': 'string',
        'add_number': 'string',
        'add_premise': 'string',
        'add_villageNo': 'string',
        'add_alley': 'string',
        'add_road': 'string',
        'add_subDistrict': 'string',
        'add_district': 'string',
        'add_province': 'string',
        'add_postcode': 'string',
        'add_formatted': 'string',
        'add_nearby': 'string',
        'add_detail': 'string',
        'parentId': 'string',
        'type': 'Int64',
        'status': 'Int64',
        'crt': 'datetime64[ns]',
        'mdt': 'datetime64[ns]'
    }

    data_csv = data_csv.astype(convert_dict)
    data_csv.columns = [c.lower() for c in data_csv.columns]
    data_csv.reset_index(inplace=True, drop=True)

    # SOURCES_REDIS_CONN_ID = "postgresCon"
    # postg_conn = PostgresHook(postgres_conn_id=SOURCES_REDIS_CONN_ID)
    # pos_db = postg_conn.get_conn()
    # print('pos_db:', pos_db)
    # Base = declarative_base()
    
    # def _query_data():
    # pg_hook = PostgresHook(
    #     postgres_conn_id="airflow_metastore",
    #     schema="airflow",
    # )
    # connection = pg_hook.get_conn()
    # cursor = connection.cursor()

    # sql = """
    #     SELECT dag_id, owners FROM dag
    # """
    # cursor.execute(sql)
    # rows = cursor.fetchall()
    # for each in rows:
    #     logging.info(each)

    # class CreateTable(Base):
    #     __tablename__ = table_name
    #     seq = Sequence(table_name + '_idx_seq', start=1, increment=1)
    #     idx = Column('idx', Integer, seq,
    #                  server_default=seq.next_value(), primary_key=True)
    #     _id = Column('_id', Text, nullable=False)
    #     name = Column('name', String)
    #     lastname = Column('lastname', String)

    # Base.metadata.create_all(postg_conn)

    # df = pd.DataFrame(
    #     [(1, "new row 4 text"), (2, "row 3 new text")], columns=["id", "txt"])
    # pos_db.('test_load_postg',df)

    # df.to_sql(name=table_name, if_exists='append',
    #           con=postg_conn, chunksize=10, method='multi', index=False)

    def connection():
        engine = create_engine(
            'postgresql://airflow:airflow@es.aidery.io:5433/postgres')
        return engine

    engine = connection()

    Base = declarative_base()

    table_name = 'domains'

    class CreateTable(Base):
        __tablename__ = table_name
        seq = Sequence(table_name+'_idx_seq', start=1, increment=1)
        idx = Column('idx', Integer, seq,
                     server_default=seq.next_value(), primary_key=True)
        _id = Column('_id', Text, nullable=False)
        crt = Column('crt', DateTime)
        mdt = Column('mdt', DateTime)
        name = Column('name', String)
        domainname = Column('domainname', String)
        typet = Column('type', Integer)
        status = Column('status', Integer)
        hospitals = Column('hospitals', String)
        loc_type = Column('loc_type', String)
        loc_coordinates = Column('loc_coordinates', String)
        add_number = Column('add_number', String)
        add_premise = Column('add_premise', String)
        add_villageno = Column('add_villageno', String)
        add_alley = Column('add_alley', String)
        add_road = Column('add_road', String)
        add_subdistrict = Column('add_subdistrict', String)
        add_district = Column('add_district', String)
        add_province = Column('add_province', String)
        add_postcode = Column('add_postcode', String)
        add_formatted = Column('add_formatted', String)
        add_nearby = Column('add_nearby', String)
        add_detail = Column('add_detail', String)
        parentid = Column('parentid', String)

    Base.metadata.create_all(engine)

    # -------------- check existing column name in Postgres ------------------
    # table_name = '\'health_records\''
    query = DDL(
        " ".join(map(str, ['select * from ' + table_name + ' limit 1'])))
    colname = list(engine.execute(query).keys())

    # col_df_l = [c.lower() for c in data_csv.columns.to_list()]
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

    data_csv.to_sql(name=table_name, if_exists='append',
                    con=engine, chunksize=10, method='multi', index=False)

    # data_count = data_csv.shape[0]
    # print('data',data_count)

    # r = redis.Redis(host='airflow_redis_1', port=6379, db=1)
    SOURCES_REDIS_CONN_ID = "redisCon"
    redis_conn = RedisHook(redis_conn_id=SOURCES_REDIS_CONN_ID)
    red_db = redis_conn.get_conn()
    red_db.set('latestOfDomains', data_csv['_id'].tail(1).to_string(index=False))


default_args = {
    'owner': 'ZG_DAP',
    'depends_on_past': False,
    'start_date': datetime(2022, 7, 26,  tzinfo=local_tz),
    'schedule_interval': '@hourly',
    'retry_delay': timedelta(seconds=5),
}


with DAG('collect_domains',
         default_args=default_args,
         tags=['mongo', 'postgres', 'domains'],
         schedule_interval='@daily') as dag:

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
