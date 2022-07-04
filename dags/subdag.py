from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.subdag import SubDagOperator
from subdags.subdag_parallel_dag import subdag_parallel_dag
 
from datetime import datetime
 

default_args={
    'start_date': datetime(2022, 6, 30)
}

with DAG('subdag_dag', default_args=default_args, schedule_interval='@daily', catchup=False) as dag:
 
    task_1 = BashOperator(
        task_id='task_1',
        bash_command='sleep 3'
    )

    processing = SubDagOperator(
        task_id='processing_tasks',
        subdag=subdag_parallel_dag('subdag_dag','processing_tasks', default_args) # expects a function that return a subdag
    )

    task_4 = BashOperator(
        task_id='task_4',
        bash_command='sleep 3'
    )
 
    task_1 >> processing >> task_4