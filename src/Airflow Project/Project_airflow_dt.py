# from __future__ import print_function
# from scipy.sparse import random

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import os
import sqlite3
import pendulum
# from tkinter.tix import COLUMN
import psycopg2
import sys
local_tz = pendulum.timezone("Asia/Bangkok")

from airflow.models import Variable
from sklearn.metrics import roc_curve, auc
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import MinMaxScaler
from sklearn.metrics import accuracy_score, confusion_matrix
from sklearn.metrics import classification_report
from sklearn.tree import DecisionTreeClassifier

dag_config = Variable.get("variables", deserialize_json=True)
data_path = dag_config["data_path"]
preprocessing_data = dag_config["preprocessing_data"]
scaling_data = dag_config["scaling_data"]
split_data = dag_config["split_data"]
training_data = dag_config["training_data"]
testing_data = dag_config['testing_data']

def data_from_postgres():
    conn = psycopg2.connect(database="postgres",
                        host="es.aidery.io",
                        user="airflow",
                        password="airflow",
                        port="5433")

    cursor = conn.cursor()
    cursor.execute("SELECT * FROM applewatch;")

    tuples_list = cursor.fetchall()
    df = pd.DataFrame(tuples_list)
    df.drop([0],axis=1,inplace=True)
    df.rename(columns = {1:'Heart', 2:'Calories',3:'Steps',4:'Distance',5:'Age',6:'Gender',7:'Weight',8:'Height',9:'Activity'}, inplace = True)
    print(df)
    df.to_csv('/opt/airflow/dags/models/data/AppleWatch.csv',index=False)

def data_import(_file_name=data_path, **kwargs):
    AP = pd.read_csv(_file_name)
#        ,usecols=['Heart','Calories','Steps','Distance','Gender','Activity']
    print(AP)
    AP.to_csv('/opt/airflow/dags/models/data/AP.csv',index=False)

def preprocessing(_file_name=preprocessing_data, **kwargs):
    AP = pd.read_csv(_file_name)
    AP['Activity'] = AP['Activity'].replace("0.Sleep", 0)
    AP['Activity'] = AP['Activity'].replace("1.Sedentary", 1)
    AP['Activity'] = AP['Activity'].replace("2.Light", 2)
    AP['Activity'] = AP['Activity'].replace("3.Moderate", 3)
    AP['Activity'] = AP['Activity'].replace("4.Vigorous", 4)
    AP['Gender'] = AP['Gender'].replace("M", 0)
    AP['Gender'] = AP['Gender'].replace("F", 1)
    #drop values
    AP.drop(AP[AP.Activity > 1].index, inplace=True)
    print(AP)
    AP.to_csv('/opt/airflow/dags/models/data/AP1.csv',index=False)

def scaling(_file_name=scaling_data, **kwargs):
    dataset = pd.read_csv(_file_name)
    y = dataset['Activity']
    X = dataset.drop(['Activity'], axis=1)
    X_colums = X.columns
    # Feature Scaling
    sc_X = MinMaxScaler()
    X = sc_X.fit_transform(X)
    scaled_X = pd.DataFrame(X, columns=X_colums)
    scaled_X['Activity'] = y
    print(scaled_X.head())
    scaled_X.to_csv('/opt/airflow/dags/models/data/AP2.csv',index=False)

def splitting_data(_file_name=split_data, **kwargs):
    dataset = pd.read_csv(_file_name)
    y = dataset['Activity']
    X = dataset.drop(['Activity'], axis=1)
    X_colums = X.columns
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size = 0.2, random_state = 1)
    X_train = pd.DataFrame(X_train,columns=X_colums)
    X_train['Activity'] = y_train
    X_test = pd.DataFrame(X_test,columns=X_colums)
    X_test['Activity'] = y_test
    X_train.to_csv('/opt/airflow/dags/models/data/AP_training_data.csv',index=False)
    X_test.to_csv('/opt/airflow/dags/models/data/AP_testing_data.csv',index=False)
    print("Ratio is 80:20")

def model_dt(_file_name=training_data, _test_file=testing_data, **kwargs):
    dataset = pd.read_csv(_file_name)
    test = pd.read_csv(_test_file)
    y_train = dataset['Activity']
    X_train = dataset.drop(['Activity'], axis=1)
    y_test = test['Activity']
    X_test = test.drop(['Activity'], axis=1)
    
    dt = DecisionTreeClassifier(criterion='gini', max_depth=10, min_samples_leaf=5,random_state=42)
    dt.fit(X_train, y_train)

    y_train_pred = dt.predict(X_train)
    y_test_pred = dt.predict(X_test)
    false_positive_rate, true_positive_rate, thresholds = roc_curve(y_test, y_test_pred)
    roc_auc = auc(false_positive_rate, true_positive_rate)



    print("Train Accuracy :",accuracy_score(y_train, y_train_pred))
    print("Train Confusion Matrix:")
    print(confusion_matrix(y_train, y_train_pred))
    print("Test Accuracy :",accuracy_score(y_test, y_test_pred))
    print("Test Confusion Matrix:")
    print(confusion_matrix(y_test, y_test_pred))
    print("Classification_report:")
    print(classification_report(y_test, dt.predict(X_test)))
    print("roc_accuracy:",roc_auc)


    
    

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2022, 9, 15,  tzinfo=local_tz),
    # 'email': ['@gmail.com'],
    # 'email_on_failure': True,
    # 'email_on_retry': True,
    'retries': 1,

    'schedule_interval': '@hourly',
    'retry_delay': timedelta(seconds=5),
}

with DAG('Airflow_DecisionTree_Pipeline',
          default_args=default_args,
          schedule_interval=timedelta(days=1)) as dag:
    
    task_1 = PythonOperator(
        task_id='data_from_postgres',
        provide_context=True,
        python_callable=data_from_postgres,
    #    op_kwargs={'_file_name': '50_Startups.csv'},
        dag=dag,
    )

    task_2 = PythonOperator(
        task_id='data_import',
        provide_context=True,
        python_callable=data_import,
    #    op_kwargs={'_file_name': '50_Startups.csv'},
        dag=dag,
    )
    
    task_3 = PythonOperator(
        task_id='preprocessing',
        provide_context=True,
        python_callable=preprocessing,
        dag=dag,
    )

    task_4 = PythonOperator(
        task_id='scaling',
        provide_context=True,
        python_callable=scaling,
        dag=dag,
    )

    task_5 = PythonOperator(
        task_id='splitting_data',
        provide_context=True,
        python_callable=splitting_data,
        dag=dag,
    )

    task_6 = PythonOperator(
        task_id='Decision_Tree',
        provide_context=True,
        python_callable=model_dt,
        dag=dag,
    )

    task_1 >> task_2 >> task_3 >> task_4 >> task_5 >> task_6

