from datetime import datetime, timedelta
from textwrap import dedent
from pprint import pprint as pp
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
    
    REQUIREMENTS = ["git+https://github.com/GITSangWoo/mov_agg.git@0.5/agg"]
    
    # def gen_empty(id):
    def gen_empty(*ids):
        tasks=[]
        for id in ids:
            task = EmptyOperator(task_id=id)
            tasks.append(task)
        return tuple(tasks) # 튜플 사용법 알려주려고

    # def gen_vpython(id, fun_obj, op_kwargs):
    def gen_vpython(**kw):
        task = PythonVirtualenvOperator(
               task_id = kw['id'],
               python_callable = kw['fun_obj'], 
               system_site_packages = False,
               requirements = REQUIREMENTS,
               op_kwargs=kw['op_kwargs'],
               #op_kwargs={
               #    "url_param" : {"multiMovieYn":"Y"},
               #     }
               )
        return task

    # def pro_data(ds_nodash, url_param):

    def pro_data(**params):
        print("@" * 33)
        print(params['task_name'])
        print("@" * 33)

    def pro_merge(task_name, **params):
        load_dt =params['ds_nodash']
        from mov_agg.u2 import merge
        df = merge(load_dt)
        print("*" * 33)
        print(df)
        print("*" * 33)


    
    def pro_data3(task_name):
        print("@" * 33)
        print(task_name)
        print("@" * 33)
    
    def pro_data4(task_name,ds_nodash,**kwargs):
        print("@" * 33)
        print(task_name)
        print(ds_nodash)
        print("@" * 33)


    
    #  start,end = gen_empty('start')
    # end = gen_empty('end')
    start, end = gen_empty('start','end')
    
    apply_type = gen_vpython(
        id='apply.type',
        fun_obj = pro_data,
        op_kwargs ={ "task_name" : "apply_type!!!"}
    )

    merge_df = gen_vpython( 
        id='merge.df',
        fun_obj = pro_merge,
        op_kwargs ={ "task_name" : "merge_df!!!"}
    )

    de_dup = gen_vpython( 
        id='de.dup',
        fun_obj = pro_data3,
        op_kwargs ={ "task_name" : "de_dup!!!"}
    )

    summary_df = gen_vpython(
        id='summary.df',
        fun_obj = pro_data4,
        op_kwargs ={ "task_name" : "summary_df!!!"}
    )
      
    start >> merge_df 
    merge_df >> de_dup >> apply_type 
    apply_type >> summary_df >> end    
