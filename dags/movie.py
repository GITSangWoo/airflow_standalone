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
    max_active_tasks=3,
    max_active_runs=1,
    description='movie',
    schedule="10 2 * * *",
    start_date=datetime(2024, 7, 24),
    catchup=True,
    tags=['api','movie','ant'],
) as dag:
     def get_data(ds_nodash):
        from movie.api.call import save2df
        df=save2df(ds_nodash)
        print(df.head(5))    

     def fun_multi(ds_nodash,args):
         from mov.api.call import get_key,save2df
         df = save2df(load_dt=ds_nodash, url_param=args)
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
     # 영화 다양성 유무
     multi_y = PythonVirtualenvOperator(
        task_id='multi.y',
        python_callable=fun_multi,
        system_site_packages=False,
        op_args=["{{ds_nodash}}"],
        op_kwargs={"multiMovieYn": "Y"},
        requirements=["git+https://github.com/GITSangWoo/movie.git@0.3/api"],
     )
     multi_n = PythonVirtualenvOperator(
        task_id='multi.n',
        python_callable=fun_multi,
        system_site_packages=False,
        op_args=["{{ds_nodash}}"],
        op_kwargs={"multiMovieYn": "N"},
        requirements=["git+https://github.com/GITSangWoo/movie.git@0.3/api"],
     )
     # 한국 영화 구분
     nation_k = PythonVirtualenvOperator(
        task_id='nation.k',
        python_callable=fun_multi,
        system_site_packages=False,
        op_args=["{{ds_nodash}}"],
        op_kwargs={"repNationCd": "K"},
        requirements=["git+https://github.com/GITSangWoo/movie.git@0.3/api"],
     )
     nation_f = PythonVirtualenvOperator(
        task_id='nation.f',
        python_callable=fun_multi,
        system_site_packages=False,
        op_args=["{{ds_nodash}}"],
        op_kwargs={"repNationCd": "F"},
        requirements=["git+https://github.com/GITSangWoo/movie.git@0.3/api"],
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

     
     get_start=EmptyOperator(
             task_id='get_start',
             trigger_rule='all_done'
     )   
     get_end= EmptyOperator(task_id='get_end')

     throw_error = BashOperator(
        task_id ='throw_err',
        bash_command="exit 1",
        trigger_rule="all_done"
     ) 
        
     task_start >> branch_op 
     task_start >> throw_error >> task_save 
     
     branch_op >> get_start
     branch_op >> rm_dir >> get_start
     branch_op >> echo_task  
     
     get_start >> [get_data, multi_y, multi_n, nation_k, nation_f] >> get_end
     get_end >> task_save >> task_end
