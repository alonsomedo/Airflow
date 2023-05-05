from airflow.models import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id='wait_for_downstream',
    start_date=datetime(2023,4,10),
    schedule_interval='@daily',
    catchup=True,
    max_active_runs=2
) as dag:

    extract = BashOperator(task_id='extract',
                            bash_command='echo sleeping 20 seconds; sleep  20',
                            wait_for_downstream=True)
    
    transform = BashOperator(task_id='transform',
                            bash_command='echo sleeping 7 seconds; sleep  7') 
    
    load = EmptyOperator(task_id='load')
    
    extract >> transform >> load