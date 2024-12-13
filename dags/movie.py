from datetime import datetime, timedelta
from textwrap import dedent
from pprint import pprint
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.models import Variable
import pandas as pd 
from airflow.operators.python import PythonOperator, PythonVirtualenvOperator,BranchPythonOperator

def gen_emp(id, rule="all_success"):
    op  = EmptyOperator(task_id=id, trigger_rule=rule)
    return op


with DAG(
    'Movie',
    default_args={
        'depends_on_past':False,
        'retries': 1,
        'retry_delay': timedelta(seconds=3)
    },
    description='Making Parquet DAG',
    schedule="10 4 * * *",
    start_date=datetime(2024, 7, 24),
    catchup=True,
    tags=['movie'],
) as dag:
        
     def get_data(ds_nodash):
         from mov.api.call import get_key,save2df
         key = get_key()
         print(f"MOVIE_API_KEY => {key}")
         df = save2df(ds_nodash)
         print(df.head(3))

     def save_data(ds_nodash):
         from mov.api.call import apply_type2df
         df = apply_type2df(load_dt=ds_nodash)
         print("*" * 33)
         print(df.head(10))
         print("*" * 33)
         print(df.dtypes)

         # 개봉일 기준 그룹핑 누적 관객수 합 
         print("개봉일 기준 그룹핑 누적 관객수 합")
         g = df.groupby('openDt')
         sum_df = g.agg({'audiCnt' : 'sum'}).reset_index()
         print(df)
     
     def branch_fun(**kwargs):
         ds_nodash = kwargs['ds_nodash']
         import os
         home_dir = os.path.expanduser("~")
         path = os.path.join(home_dir,f'tmp/test_parquet/load_dt={ds_nodash}')
         if  os.path.exists(path):
             return "rm_dir"
         else:
             return "get_data","echo_task"

     branch_op = BranchPythonOperator(
        task_id="branch.op",
        python_callable=branch_fun,
     )

    
     get_data = PythonVirtualenvOperator(
         task_id="get_data",
         python_callable=get_data,
         requirements=["git+https://github.com/GITSangWoo/movie.git@0.3/api"],
         system_site_packages=False,
         trigger_rule='all_done',
         # venv_cache_path="/home/centa/tmp/air_venv/get_data"
     )

     task_save = PythonVirtualenvOperator(
         task_id="save_data",
         python_callable=save_data,
         requirements=["git+https://github.com/GITSangWoo/movie.git@0.3/api"],
         system_site_packages=False,
         trigger_rule='one_success',
         # venv_cache_path="/home/centa/tmp/air_venv/get_data"
     )

     rm_dir = BashOperator(
         task_id='rm_dir',
         bash_command="rm -rf ~/tmp/test_parquet/load_dt={{ds_nodash}}",
     )

     echo_task = BashOperator(
        task_id='echo_task',
        bash_command="echo task"
     )
     
     task_end = gen_emp('end','all_done')
     task_start = gen_emp('start')
     join_task = BashOperator(
        task_id ='join',
        bash_command="exit 1",
        trigger_rule="all_done"
     ) 

     task_start >> branch_op 
     task_start >> join_task >> task_save

     branch_op >> rm_dir >> get_data 
     branch_op >> get_data
     branch_op >> echo_task >> task_save 
     

     get_data >> task_save >> task_end 
