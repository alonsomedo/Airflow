from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.subdag import SubDagOperator
from subdags.subdag_parallel_dag import subdag_parallel_dag

from datetime import datetime, timedelta


# Updates
# All the arguments available for BaseOperator are available too for default_args
default_args = {
    'owner': 'DataOperations',
    'start_date': datetime(2023,4,19),
    'retries': 5,
    'retry_delay': timedelta(seconds=30),
    'execution_timeout': timedelta(seconds=500)
}


with DAG(
    dag_id='subdags',
    schedule='@daily',
    catchup=False,
    default_args=default_args
) as dag:
        
    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')
    
    task_1 = BashOperator(
        task_id='task_1',
        bash_command='sleep 5'
    )
    
    processing = SubDagOperator(
        task_id='processing_tasks',
        subdag=subdag_parallel_dag('subdags', 'processing_tasks', default_args=default_args)
    )
    
    task_4 = BashOperator(
        task_id='task_4',
        bash_command='sleep 5'
    )
    
start >> task_1 >> processing >> task_4 >> end
    
