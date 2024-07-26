from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.models import Variable
import pandas as pd 

def gen_emp(id, rule="all_success"):
    op  = EmptyOperator(task_id=id, trigger_rule=rule)
    return op


with DAG(
    'Movie',
    default_args={
        'depends_on_past':False,
        'retries': 1,
        'retry_delay': timedelta(minutes=3)
    },
    description='Making Parquet DAG',
    schedule="10 4 * * *",
    start_date=datetime(2024, 7, 24),
    catchup=True,
    tags=['movie'],
) as dag:
     task_get = BashOperator(
        task_id="get_data",
        bash_command="""
            echo "get"
        """
     )
     task_save = BashOperator(
        task_id="save_data",
        bash_command="""
            echo "save"
        """
     )
     task_done = BashOperator(
        task_id="make_done",
        bash_command="""
            echo "done"
        """
     )
     
     task_err = BashOperator(
        task_id="err.report",
        bash_command="""
            echo "err report"
        """,
        trigger_rule="one_failed"
     )
     
     task_end = gen_emp('end','all_done')
     task_start = gen_emp('start')
    
     task_start >> task_get >> task_save >> task_end 
