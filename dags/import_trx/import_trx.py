import logging
import os
from datetime import date, datetime, timedelta

from utils.config import get_config
from utils.config import get_md
from utils.config import get_sql


from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from import_trx.util.upload_trx_files_to_s3 import upload_trx_files_to_s3
from airflow.operators.bash import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


# Declare configuration variables
dag_file_name = os.path.basename(__file__).split('.')[0]
dag_config = get_config(dag_file_name = dag_file_name, config_filename = 'dag_config.yaml')
pipeline_config = get_config(dag_file_name = dag_file_name, config_filename = 'pipeline_config.yaml')

env="dev"
default_arguments = dag_config['default_args'][env]

# Getting variables of pipeline configs
endpoint = pipeline_config['endpoint']

#Airflow docstring
doc_md = get_md(dag_file_name, 'README.md')

#logging.basicConfig(level=logging.INFO)

#Declare DAG insrtance and DAG operators
with DAG(dag_file_name,
          description='Very short description (optional)',
          start_date=datetime(2022,9,10),
          end_date=datetime(2022,9,12),
          max_active_runs=1,
          catchup=True,
          tags = [],
          schedule_interval=dag_config['schedule'][env],
          default_args=default_arguments,
          dagrun_timeout=timedelta(hours=dag_config["dag_run_timeout"]),
          doc_md=doc_md,
          ) as dag:
    
    ##### DECLARING THE OPERATORS ######
    
    # Declare Dummy Operators
    start_operator = DummyOperator(task_id='start-operator')
    end_operator = DummyOperator(task_id='end-operator')

    sleep = BashOperator(
        task_id='sleep',
        bash_command='sleep 15'
    )
    
    upload_trx_files_to_s3 = PythonOperator(
        task_id='upload_trx_files_to_s3',
        python_callable=upload_trx_files_to_s3
    )
    
    tigger_csv_to_mysql = TriggerDagRunOperator(
        task_id='trigger_dag_import_csv_to_mysql_trigger',
        trigger_dag_id='import_csv_to_mysql_trigger'
    )
     
    start_operator >> sleep >> upload_trx_files_to_s3 >> tigger_csv_to_mysql >> end_operator