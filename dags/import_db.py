from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
with DAG(
    'ImportDB',
    default_args={
        'depends_on_past':False,
        'retries': 1,
        'retry_delay': timedelta(seconds=3)
    },
    description='Import Database DAG',
    schedule="10 4 * * *",
    start_date=datetime(2024, 7, 10),
    catchup=True,
    tags=['simple','bash','etl','shop','db','history'],
) as dag:
     task_start = EmptyOperator(
        task_id="start"
     )
     task_check = BashOperator(
        task_id="check",
        bash_command="""
            echo "check"
            bash {{ var.value.CHECK_SH }} {{ ds_nodash }}
        """
     )

     task_csv = BashOperator(
        task_id="to_csv",
        bash_command="""
            echo "to_csv"
            C_PATH=~/data/count/{{ds_nodash}}/count.log
            CSV_PATH=~/data/csv/{{ds_nodash}}

            mkdir -p ${CSV_PATH}

            cat ${C_PATH} | awk '{print "{{ds}}," $2 "," $1}' > ${CSV_PATH}/csv.csv
        """
     )

     task_tmp = BashOperator(
        task_id="to_tmp",
        bash_command="""
            echo "tmp"
        """
     )

     task_base = BashOperator(
        task_id="to_base",
        bash_command="""
            echo "base"
        """
     )

     task_done = BashOperator(
        task_id="make_done",
        bash_command="""
            echo "done"
        """
     )
     task_end = EmptyOperator(
        task_id="end",
        trigger_rule = "all_done"
     )
     task_err = BashOperator(
        task_id="err.report",
        bash_command="""
            echo "err report"
        """,
        trigger_rule="one_failed"
     )

     task_start >> task_check >> task_csv 
     task_check >> task_err >> task_end
     task_csv >> task_tmp >> task_base 
     task_base >> task_done >> task_end
