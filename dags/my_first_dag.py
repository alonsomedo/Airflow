from airflow.models import DAG
from airflow.operators.empty import EmptyOperator
from datetime import datetime

with DAG(
    dag_id='my_first_dag',
    start_date=datetime(2023,4,10),
    schedule_interval='@daily',
    catchup=True
) as dag:

    extract = EmptyOperator(task_id='extract')
    
    transform = EmptyOperator(task_id='transform')
    
    load = EmptyOperator(task_id='load')
    
    extract >> transform >> load