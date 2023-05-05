from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime, timedelta


# Updates
# All the arguments available for BaseOperator are available too for default_args
default_args = {
    'owner': 'DataOperations',
    'execution_timeout': timedelta(seconds=500)
}


with DAG(
    dag_id='trigger_rules',
    start_date= datetime(2023,4,19),
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
    
    with TaskGroup('bigquery_tasks') as bigquery_tasks:
        task_2 = BashOperator(
            task_id='task_2', # bigquery_tasks.task_2
            bash_command='exit 1'
        )
            
        task_3 = BashOperator(
            task_id='task_3',
            bash_command='exit 1'
        )
        
        task_5 = BashOperator(
            task_id='task_5',
            bash_command='exit 1'
        )
        
    task_4 = BashOperator(
        task_id='task_4',
        bash_command='sleep 5',
        trigger_rule='one_success'
    )
    
    start >> task_1 >> bigquery_tasks >> task_4 >> end
    
