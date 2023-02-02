from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
import constant
from func.database_connect import DatabaseConnect

from func.init_func import use_func_folder
use_func_folder()


default_args = {
    'owner': 'thawareeSukk',
}


def my_function():
    # ======================= 1. Create connection =======================
    connection = DatabaseConnect()
    # ------------ connect to collection ------------
    authDbA = connection.getAuthDB()
    domainColl = authDbA.get_collection(constant.AUTH_DOMAIN_COLL)
    cursor = domainColl.find_one()
    print('TTT: ', cursor)


def my_function2():
    # connection = DatabaseConnect()
    authDbH = connection.getHealthDB()
    healthColl = authDbH.get_collection(constant.HEALTH_RECORD_COLL)
    cursor = healthColl.find_one()
    print('TTT: ', cursor)


with DAG(
    'simple_dag',
    start_date=days_ago(1),
    schedule_interval=None,
    tags=['exercise']
) as dag:

    t1 = PythonOperator(
        task_id="print_hello",
        python_callable=my_function,
        # op_kwargs={"something": "Hello World!"},
    )

    t2 = PythonOperator(
        task_id="print_date",
        python_callable=my_function2,
        # bash_command="echo $(date)",
    )

    # t2 = BashOperator(
    #     task_id="print_date",
    #     bash_command="echo $(date)",
    # )

    # task dependencies
    t1 >> t2
