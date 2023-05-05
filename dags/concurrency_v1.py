from airflow.models import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id='concurrency_v1',
    start_date=datetime(2023,4,10),
    schedule_interval='@daily',
    catchup=True,
    max_active_runs=2
) as dag:

    extract = EmptyOperator(task_id='extract')
    
    transform = BashOperator(task_id='transform',
                             bash_command='echo sleeping 20 seconds; sleep  20')
    
    load = EmptyOperator(task_id='load')
    
    extract >> transform >> load