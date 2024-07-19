from datetime import datetime, timedelta
from textwrap import dedent

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
with DAG(
    'helloworld',
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={
        'depends_on_past': True,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5)
    },
    description='hello world DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 7, 10),
    catchup=True,
    tags=['helloworld'],
) as dag:

    # t1, t2 and t3 are examples of tasks created by instantiating operators
    task_start = DummyOperator (task_id = 'start')
    task_end = DummyOperator (task_id = 'end')
    task_sleep = DummyOperator (task_id = 'sleep')
    t1  = DummyOperator(task_id = 't1')
    t10  = DummyOperator(task_id = 't10')
    t100  = DummyOperator(task_id = 't100')
    t101  = DummyOperator(task_id = 't101')
    t2  = DummyOperator(task_id = 't2')
    t20  = DummyOperator(task_id = 't20')
    t200  = DummyOperator(task_id = 't200')
    t201  = DummyOperator(task_id = 't201')
   
    task_start >> t1 >> t10 >> [t100, t101] >> task_sleep
    task_start >> t2 >> t20 >> [t200, t201] >> task_sleep
    task_sleep >> task_end
