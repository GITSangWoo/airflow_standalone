from datetime import datetime, timedelta
from textwrap import dedent
from pprint import pprint
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.models import Variable
import pandas as pd 
from airflow.operators.python import PythonOperator, PythonVirtualenvOperator

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
    def get_data(ds, **kwargs):
        print(ds)
        print(kwargs)
        print("*" * 20)
        print(f"ds_nodash=>{kwargs['ds_nodash']}")
        print(f"kwargs type =>{type(kwargs)}")
        print("*" * 20) 
        from mov.api.call import get_key,save2df
        key = get_key()
        print(f"MOVIE_API_KEY => {key}")
        YYYYMMDD=kwargs['ds_nodash']
        df = save2df(YYYYMMDD)
        print(df.head(3))

    def print_context(ds=None, **kwargs):
        print("::group::All kwargs")
        pprint(kwargs)
        print(kwargs)
        print("::endgroup::")
        print("::group::Context variable ds")
        print(ds)
        print("::endgroup::")
        return "Whatever you return gets printed in the logs"
   
    run_this = PythonOperator(
            task_id="print_the_context",
            python_callable=print_context,
    )
    
    get_data = PythonVirtualenvOperator(
        task_id="get_data",
        python_callable=get_data,
        requirements=["git+https://github.com/GITSangWoo/movie.git@0.2/api"],
        system_site_packages=False,
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
    
    task_start >> get_data >> task_save >> task_end 
    task_start >> run_this >> task_end
