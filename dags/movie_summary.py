from datetime import datetime, timedelta
from textwrap import dedent
from pprint import pprint

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator, PythonVirtualenvOperator, BranchPythonOperator


with DAG(
    'movie_summary',
    default_args={
        'depends_on_past': False,
        'retries': 1,
        'retry_delay': timedelta(seconds=3),
    },
    description='movie summary',
    schedule = "10 4 * * *",
    start_date=datetime(2024, 7, 20),
    catchup=True,
    tags=['movie', 'summary', 'api' ],
) as dag:
    def apply_data():
        return 0
    def merge_data():
        return 0
    def dedup_data():
        return 0
    def summary_data():
        return 0

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    apply_type = PythonVirtualenvOperator(
        task_id = "apply.df",
        python_callable = apply_data
    )
    merge_df = PythonVirtualenvOperator(
        task_id = "merge.df",
        python_callable = merge_data
    )
    de_dup = PythonVirtualenvOperator(
        task_id = "dedup.df",
        python_callable = dedup_data
    )
    summary_df = PythonVirtualenvOperator(
        task_id = "summary.df",
        python_callable = summary_data
    )
      
    start >> apply_type >> merge_df >> de_dup >> summary_df >> end    
