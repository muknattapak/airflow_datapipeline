from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from elasticsearch import Elasticsearch, helpers, RequestsHttpConnection
from datetime import datetime, timedelta
from bson import ObjectId
from pymongo import MongoClient
import pendulum
import pandas as pd
import os
import redis

local_tz = pendulum.timezone("Asia/Bangkok")

# mongoexport -h ddb.thailand-smartliving.com:38200 --authenticationDatabase=aidery-health -u airflow -p 98ZaEVYxq758h2tk -d aidery-health -c survey-answers --fields=_id,userId,ts,response,crt --type=json --out=/Users/muk/symtoms.json

# def check_dateOutOfBound(values):
#         if pd.notna(values):
#             if (pd.Timestamp.max < values) | (values < pd.Timestamp.min):
#                 return pd.to_datetime(pd.to_datetime(values.replace(year=(values.year-543)), errors='coerce').isoformat(sep=' ', timespec='seconds'))
#             else:
#                 return pd.to_datetime(pd.to_datetime(values, errors='coerce').isoformat(sep=' ', timespec='seconds'))

def get_data():
    r = redis.Redis(host='airflow_redis_1', port=6379, db=1)

    client = MongoClient(
        "mongodb://airflow:98ZaEVYxq758h2tk@ddb.thailand-smartliving.com:38200/aidery-health")

    mg_auth = client['aidery-auth']
    personalrecColl = mg_auth['personal-records']
    # userColl = mg_auth['users']
    latestId = r.get('latestOfPersonalRecordsPipeline')
    print('LatestId:', latestId)

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
    df = df.drop(['ts'], axis=1)

    # col_date_list = list(df.select_dtypes(include=['datetime']).columns)
    # col_date_list.append('ts')
    # for type_l in col_date_list:
    #     df[type_l] = df[type_l].apply(check_dateOutOfBound)
    
    # df_column = ['_id','type','category','tags','userId','creator','title','desc','ts','crt','mdt','data_tags']

    # # read user record
    # user_list = df['userId'].copy().drop_duplicates().reset_index(drop=True)  # get list of userid
    # personal_record = pd.DataFrame()
    # for uid in range(len(user_list)):
    #     cursorU = userColl.find(
    #         {'_id': ObjectId(user_list[uid])}, {'_id', 'domainId'})
    #     users_ls = pd.DataFrame(list(cursorU))
    #     # users_ls['_id'] = users_ls['_id'].astype(str)
        
    #     data = df[df['userId'] == user_list[uid]].copy()
    #     data = data.merge(users_ls, how='left', left_on='userId',
    #                         right_on='_id', suffixes=('', '_y'))
    #     data.drop(data.filter(regex='_y$').columns, axis=1, inplace=True)
        
    # personal_record = pd.concat((personal_record, data),
    #                                 axis=0, ignore_index=True)
    # personal_record = personal_record.where(pd.notnull(personal_record), None)

    path = os.path.join(os.getcwd(), "dags/temp/health_notes-test.csv")
    data = df.to_csv(path, index=False)


def load_data():
    path = os.path.join(os.getcwd(), "dags/temp/health_notes-test.csv")
    data_csv = pd.read_csv(path)

    r = redis.Redis(host='airflow_redis_1', port=6379, db=1)
    
    # convert_dict = {
    #     '_id': 'string',
    #     'userId': 'string',
    #     'type': 'Int64',
    #     'category': 'Int64',
    #     'title': 'string',
    #     'desc': 'string',
    #     'ts': 'datetime64[ns]',
    #     'tags': 'object',
    #     'creator': 'string',
    #     'crt': 'datetime64[ns]',
    #     'mdt': 'datetime64[ns]',
    #     'data_tags': 'object'
    # }

    # data_csv = data_csv.astype(convert_dict)
    data_elastic = data_csv.to_dict(orient='records')

    es = Elasticsearch(hosts="http://elastic:qAMhwPV4yT3dKxK2@zg-es01:9200",  
                        use_ssl = True,
                        verify_certs=False)
    index_name = 'healthnote_test'
    helpers.bulk(es, data_elastic, index=index_name)
    r.set('latestOfPersonalRecordsPipeline', data_csv['_id'].tail(1).to_string(index=False))

    # try:
    #     es = Elasticsearch(hosts="https://elastic:qAMhwPV4yT3dKxK2@es.aidery.io:9200",
    #                     use_ssl = True,
    #                     verify_certs=False)

    #     print("Connection to ES Server successful")
        
    #     # es.indices.analyze(index=index_name,body={
    #     #     "analyzer" : "thai", 
    #     #     "tokenizer" : "thai"
    #     #     })
    #     index_name = 'healthnote_test'
    #     helpers.bulk(es, data_elastic, index=index_name)
    #     r.set('latestOfPersonalRecordsPipeline', data_csv['_id'].tail(1).to_string(index=False))
    #     # resp = es.index(index="healthnotes", id=1, document=data_elastic)
    # except:
    #     print("Unable to connect to server")
    #     exit(1)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2022, 9, 6,  tzinfo=local_tz),
    # 'email': ['@gmail.com'],
    # 'email_on_failure': True,
    # 'email_on_retry': True,
    # 'retries': 3,

    'schedule_interval': '@daily',
    'retry_delay': timedelta(seconds=5),
}

with DAG('elt_health-notes',
         default_args=default_args,
         schedule_interval='@daily')as dag:
    #  schedule_interval=timedelta(minutes=5)

    t1 = PythonOperator(
        task_id='t1_get_mongo',
        provide_context=True,
        python_callable=get_data
    )

    t2 = PythonOperator(
        task_id='t2_load_elastic',
        provide_context=True,
        python_callable=load_data
    )

    t1 >> t2
