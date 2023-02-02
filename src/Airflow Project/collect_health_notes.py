from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.providers.redis.hooks.redis import RedisHook
from airflow.operators.python import ShortCircuitOperator
from elasticsearch import Elasticsearch, helpers, RequestsHttpConnection
from datetime import datetime, timedelta
from bson import ObjectId
# from pymongo import MongoClient
import pandas as pd
import os
import requests
import pytz

local_tz = pytz.timezone("Asia/Bangkok")

# mongoexport -h ddb.thailand-smartliving.com:38200 --authenticationDatabase=aidery-health -u airflow -p 98ZaEVYxq758h2tk -d aidery-health -c survey-answers --fields=_id,userId,ts,response,crt --type=json --out=/Users/muk/symtoms.json

def is_latest_id(**kwargs):
    SOURCES_REDIS_CONN_ID = "redisCon"
    redis_conn = RedisHook(redis_conn_id=SOURCES_REDIS_CONN_ID)
    red_db = redis_conn.get_conn()

    SOURCES_MONGO_DB_CONN_ID = "mongoAuth"
    SOURCES_MONGO_DB = "aidery-auth"
    SOURCES_MONGO_COLLECTION = "personal-records"

    mongoAuth = MongoHook(conn_id=SOURCES_MONGO_DB_CONN_ID)
    personalrecColl = mongoAuth.get_collection(SOURCES_MONGO_COLLECTION,SOURCES_MONGO_DB)
    latest_record = pd.DataFrame(list(personalrecColl.find({'_id': 1}).sort('_id', -1).limit(1)))

    latestId = red_db.get('latestOfPersonalRecordsPipeline')

    if latestId is not None:
        # have new data in mongo
        if latest_record['_id'][0] > ObjectId(latestId.decode()):
            return True  # read new data
        else:
            return False  # skip read data
    else:
        return True

def check_dateOutOfBound(values):
        if pd.notna(values):
            if (pd.Timestamp.max < values) | (values < pd.Timestamp.min):
                return pd.to_datetime(pd.to_datetime(values.replace(year=(values.year-543)), errors='coerce').isoformat(sep=' ', timespec='seconds'))
            else:
                return pd.to_datetime(pd.to_datetime(values, errors='coerce').isoformat(sep=' ', timespec='seconds'))

def get_data():
    # r = redis.Redis(host='airflow_redis_1', port=6379, db=1)
    SOURCES_REDIS_CONN_ID = "redisCon"
    redis_conn = RedisHook(redis_conn_id=SOURCES_REDIS_CONN_ID)
    red_db = redis_conn.get_conn()

    SOURCES_MONGO_DB_CONN_ID = "mongoAuth"
    SOURCES_MONGO_DB = "aidery-auth"
    SOURCES_MONGO_COLLECTION = "personal-records"
    SOURCES_MONGO_COLLECTION2 = "users"

    mongoAuth = MongoHook(conn_id=SOURCES_MONGO_DB_CONN_ID)
    personalrecColl = mongoAuth.get_collection(SOURCES_MONGO_COLLECTION,SOURCES_MONGO_DB)
    userColl = mongoAuth.get_collection(SOURCES_MONGO_COLLECTION2,SOURCES_MONGO_DB)

    latestId = red_db.get('latestOfPersonalRecordsPipeline')
    print('LatestId:', latestId)

    # client = MongoClient(
    #     "mongodb://airflow:98ZaEVYxq758h2tk@ddb.thailand-smartliving.com:38200/aidery-health")

    # # print(client.server_info())
    # mydb = client['aidery-auth']
    # personalrecColl = mydb['personal-records']
    # userColl = mydb['users']

    if (latestId):
        cursor = personalrecColl.find(
            {'tags': ['Health-Note'], '_id': {'$gt': ObjectId(latestId.decode())}}).sort('_id', 1).limit(100)
    else:
        cursor = personalrecColl.find(
            {'tags': ['Health-Note']}).sort('_id', 1).limit(100)

    list_cur = list(cursor)
    temp_data = pd.DataFrame(list_cur)
    df = temp_data.copy().reset_index(drop=True)
    df = df.dropna(axis=1, how='all')
    df = pd.concat([df.drop('data', axis=1),
                    df['data'].apply(pd.Series).add_prefix('data_')], axis=1)
    df.columns = df.columns.map(str)
    # df = df.drop(['ts'], axis=1)

    col_date_list = list(df.select_dtypes(include=['datetime']).columns)
    col_date_list.append('ts')
    for type_l in col_date_list:
        df[type_l] = df[type_l].apply(check_dateOutOfBound)

    # # read user record
    # get list of userid
    user_list = df['userId'].copy().drop_duplicates().reset_index(drop=True)  
    personal_record = pd.DataFrame()
    users_ls = pd.DataFrame()
    for uid in range(len(user_list)):
        cursorU = userColl.find(
            {'_id': ObjectId(user_list[uid])}, {'_id', 'domainId'})
        temp_us = pd.DataFrame(list(cursorU))
        users_ls = users_ls.append(temp_us, ignore_index=True)
       
    data = df.merge(users_ls, how='left', left_on='userId',
                        right_on='_id', suffixes=('', '_y'))
    data.drop(data.filter(regex='_y$').columns, axis=1, inplace=True)
    
    personal_record = pd.concat((personal_record, data),
                                    axis=0, ignore_index=True)
    personal_record = personal_record.where(pd.notnull(personal_record), None)
    personal_record[personal_record['domainId'].isnull()]
    personal_record = personal_record.dropna(subset=['domainId'])
    personal_record['ts'] = personal_record['ts'].apply(lambda x : "Null" if pd.isnull(x) else x)
    personal_record['desc'] = personal_record['desc'].apply(lambda x : "N/A" if pd.isnull(x) else x)
    personal_record = personal_record[['_id','userId','domainId','creator','type','category','title','desc','tags','data_tags','ts','crt','mdt']]

    path = os.path.join(os.getcwd(), "dags/temp/health_notes-test.csv")
    data = personal_record.to_csv(path, index=False)

def load_data():
    path = os.path.join(os.getcwd(), "dags/temp/health_notes-test.csv")
    data_csv = pd.read_csv(path)
    # print(data_csv.info())

    SOURCES_REDIS_CONN_ID = "redisCon"
    redis_conn = RedisHook(redis_conn_id=SOURCES_REDIS_CONN_ID)
    red_db = redis_conn.get_conn()
    
    convert_dict = {
        '_id': 'string',
        'userId': 'string',
        'domainId': 'string',
        'creator': 'string',
        'type': 'Int64',
        'category': 'Int64',
        'title': 'string',
        'desc': 'string',
        'tags': 'object',
        'data_tags': 'object'
        # 'ts': 'datetime64[ns]',
        # 'crt': 'datetime64[ns]',
        # 'mdt': 'datetime64[ns]'
    }

    data_csv = data_csv.astype(convert_dict)
    # print(data_csv.info())
    data_elastic = data_csv.to_dict(orient='records')

    es = Elasticsearch(hosts="http://elastic:qAMhwPV4yT3dKxK2@zg-es01:9200",  
                        use_ssl = True,
                        verify_certs=False,
                        raise_on_error=False)

    index_name = 'healthnote_2'
    helpers.bulk(es, data_elastic, index=index_name)
    red_db.set('latestOfPersonalRecordsPipeline', data_csv['_id'].tail(1).to_string(index=False))

    #     # es.indices.analyze(index=index_name,body={
    #     #     "analyzer" : "thai", 
    #     #     "tokenizer" : "thai"
    #     #     })

def send_line_notify(context):
    url = 'https://notify-api.line.me/api/notify'
    token = 'mFS0UYBzj5Sfav17UV0UNRLtvYqm1lPhk6hI8gqJ42a'
    headers = {
        'content-type':
        'application/x-www-form-urlencoded',
        'Authorization': 'Bearer '+token
    }
    ds = context.get("execution_date")
    dag_id = context.get("task_instance").dag_id
    task_id = context.get("task_instance").task_id
    msg = "alert failed report \n"
    msg += str(ds.replace(tzinfo=pytz.utc).astimezone(local_tz).strftime('%Y-%m-%d %H:%M:%S.%f')) + " "+str(dag_id) +" "+ str(task_id)
    re = requests.post(url, headers=headers, data={'message': msg})
    # print(re.text)

default_args = {
    'owner': 'zg',
    'depends_on_past': False,
    'start_date': datetime(2022, 9, 6,  tzinfo=local_tz),
    # 'email': ['muk.richie@gmail.com'],
    # 'email_on_failure': True,
    # 'email_on_retry': True,
    # 'retries': 3,
    'schedule_interval': '@daily',
    'retry_delay': timedelta(seconds=5),
}

with DAG('collect_health_notes',
         default_args=default_args,
         tags=['mongo', 'elastic', 'healthnotes'],
         schedule_interval='@daily')as dag:
    #  schedule_interval=timedelta(minutes=5)

    is_latest = ShortCircuitOperator(
        task_id='is_latest',
        python_callable=is_latest_id,
        ignore_downstream_trigger_rules=True
    )

    t1 = PythonOperator(
        task_id='t1_read_mongo',
        provide_context=True,
        python_callable=get_data,
        on_failure_callback=send_line_notify
    )

    t2 = PythonOperator(
        task_id='t2_load_elastic',
        provide_context=True,
        python_callable=load_data,
        on_failure_callback=send_line_notify
    )

    is_latest >> t1 >> t2
