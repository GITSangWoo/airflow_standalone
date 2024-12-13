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
        task_id="to.csv",
        bash_command="""
            echo "to.csv"

            U_PATH=~/data/count/{{ds_nodash}}/count.log
            CSV_PATH=~/data/csv/{{ds_nodash}}
            CSV_FILE=~/data/csv/{{ds_nodash}}/csv.csv

            mkdir -p $CSV_PATH

            # cat $U_PATH | awk '{print "\\"{{ds}}\\",\\"" $2 "\\",\\"" $1 "\\""}' > ${CSV_FILE}
            cat $U_PATH | awk '{print "^{{ds}}^,^" $2 "^,^" $1 "^"}' > ${CSV_FILE}
            echo $CSV_PATH
            """
     )

     

     task_create_table = BashOperator(
        task_id='create_table',
        bash_command="""
            SQL={{ var.value.SQL_PATH }}/create_db_table.sql
            echo "SQL_PATH=${SQL}"
            MYSQL_PWD='{{ var.value.DB_PASSWD }}' mysql -u root < ${SQL}
        """
        )


     task_tmp = BashOperator(
        task_id="to_tmp",
        bash_command="""
            echo "to_tmp"
            CSV_PATH=~/data/csv/{{ds_nodash}}/csv.csv
            bash {{var.value.SH_HOME}}/cvs2mysql.sh ${CSV_PATH} {{ds}}
        """
        )  

     task_base = BashOperator(
        task_id="to_base",
        bash_command="""
            echo "base"
            bash {{ var.value.SH_HOME }}/tmp2base.sh {{ds}} 
        """
     )

     task_done = BashOperator(
        task_id="make_done",
        bash_command="""
            figlet "make.done.start"
            DONE_PATH={{var.value.TOBASE_DONE}}/{{ds_nodash}}
            echo "$DONEPATH"
            mkdir -p ${DONE_PATH}
            touch ${DONE_PATH}/_DONE
            figlet "make.done.end"
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
     task_csv >> task_create_table >> task_tmp >> task_base 
     task_base >> task_done >> task_end
