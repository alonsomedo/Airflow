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
          start_date=datetime.strptime(dag_config['dag_args'][env]["start_date"], '%Y-%m-%d'),
          max_active_runs=dag_config['dag_args'][env]["max_active_runs"],
          catchup=dag_config['dag_args'][env]["catchup"],
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
       

     
    start_operator >> end_operator