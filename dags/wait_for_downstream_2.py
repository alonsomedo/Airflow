from airflow.models import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id='wait_for_downstream_v2',
    start_date=datetime(2023,4,10),
    schedule_interval='@daily',
    catchup=True,
    max_active_runs=2
) as dag:

    start = BashOperator(task_id='start',
                         bash_command='echo sleeping 15 seconds; sleep  15',
                         wait_for_downstream=True)
    
    slack_notification = BashOperator(task_id='slack_notification',
                                      bash_command='echo sleeping 15 seconds;')
    
    send_email = BashOperator(task_id='send_email',
                              bash_command='echo sleeping 5 seconds;')
    
    extract = BashOperator(task_id='extract',
                           bash_command='echo sleeping 10 seconds;')
    
    transform = BashOperator(task_id='transform',
                             bash_command='echo Hello world') 
    
    load = EmptyOperator(task_id='load')
    
    #start >> [extract, slack_notification]
    #extract >> transform >> load
    #slack_notification >> send_email
    
    start >> extract >> transform >> load
    start >> slack_notification >> send_email