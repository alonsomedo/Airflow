from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.subdag import SubDagOperator
from airflow.utils.task_group import TaskGroup 

from datetime import datetime
 

default_args={
    'start_date': datetime(2022, 6, 30)
}

with DAG('taskgroup_dag', default_args=default_args, schedule_interval='@daily', catchup=False) as dag:
 
    task_1 = BashOperator(
        task_id='task_1',
        bash_command='sleep 3'
    )

    with TaskGroup('processing_tasks') as processing_tasks:
         
        task_2 = BashOperator(
            task_id='task_2',
            bash_command='sleep 3'
        )

        task_3 = BashOperator(
            task_id='task_3',
            bash_command='sleep 3'
        )


    task_4 = BashOperator(
        task_id='task_4',
        bash_command='sleep 3'
    )
 
    task_1 >> processing_tasks >> task_4