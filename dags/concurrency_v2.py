from airflow.models import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id='concurrency_v2',
    start_date=datetime(2023,4,10),
    schedule_interval='@daily',
    catchup=True,
    max_active_runs=1,
    max_active_tasks=2
) as dag:

    extract = EmptyOperator(task_id='extract')
    
    transform_1 = BashOperator(task_id='transform_1',
                             bash_command='echo sleeping 10 seconds; sleep  10')
    
    transform_2 = BashOperator(task_id='transform_2',
                             bash_command='echo sleeping 15 seconds; sleep  15')
    
    transform_3 = BashOperator(task_id='transform_3',
                             bash_command='echo sleeping 10 seconds; sleep  10')
    
    load = EmptyOperator(task_id='load')
    
    extract >> [transform_3, transform_1, transform_2] >> load