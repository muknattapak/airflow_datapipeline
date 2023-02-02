from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import pendulum
import psycopg2
from sqlalchemy import create_engine
from 

local_tz = pendulum.timezone("Asia/Bangkok")

def read_postgres():
    conn = psycopg2.connect(database="postgres",
                        host="es.aidery.io",
                        user="airflow",
                        password="airflow",
                        port="5433")
    
    cursor = conn.cursor()
    # cursor.execute('SELECT * FROM users LIMIT 10')
    sql_str = 'select * from users order by idx asc limit 10'
    cursor.execute(sql_str)
    colnames = [desc[0] for desc in cursor.description] 
    data_list = cursor.fetchall()
    users_dataset = pd.DataFrame(data_list, columns=colnames )

    users_dataset.to_csv('/opt/airflow/dags/models/data/AppleWatch.csv',index=False)
    
    cursor.close()
    conn.close()

    
