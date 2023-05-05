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
from airflow.operators.bash import BashOperator
#from airflow.providers.http.sensors.http import HttpSensor
#from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.mysql.operators.mysql import MySqlOperator

from import_soccer_players_data.util.get_soccer_team_players import get_soccer_team_players
from import_soccer_players_data.util.load_soccer_team_players import load_soccer_team_players


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

#Declare DAG insrtance and DAG operators
with DAG(dag_file_name,
          description='Pipeline that imports soccer players data',
          start_date=datetime.strptime(dag_config['dag_args'][env]["start_date"], '%Y-%m-%d'),
          max_active_runs=dag_config['dag_args'][env]["max_active_runs"],
          catchup=dag_config['dag_args'][env]["catchup"],
          tags = dag_config['tags'],
          schedule_interval=dag_config['schedule'][env],
          default_args=default_arguments,
          dagrun_timeout=timedelta(hours=dag_config["dag_run_timeout"]),
          doc_md=doc_md,
          ) as dag:
    
    
    start_operator = DummyOperator(task_id='start-operator')
    end_operator = DummyOperator(task_id='end-operator')
    
    soccer_players_table = MySqlOperator(
        task_id='create_soccer_players_table',
        sql=get_sql(dag_file_name, "create_soccer_players_table.sql").format(table_name='soccer_players')
    )
    
    get_soccer_players = PythonOperator(
        task_id='get_scoccer_players',
        python_callable=get_soccer_team_players,
        op_args=[529, endpoint]
    )
    
    load_soccer_players = PythonOperator(
        task_id='load_scoccer_players',
        python_callable=load_soccer_team_players,
        op_args=[529]
    )

    #position_list = []
    
    for position in pipeline_config['soccer_positions']:
        create_soccer_position_table = MySqlOperator(
            task_id=f'create_soccer_{position}_table',
            sql=get_sql(dag_file_name, "create_soccer_players_table.sql").format(table_name=f'soccer_players_{position}')
        )
        
        
        #position_list.append(print_message)
        start_operator >> soccer_players_table >> get_soccer_players >> load_soccer_players >> create_soccer_position_table >> end_operator
    
    #start_operator >> soccer_players_table >> get_soccer_players >> load_soccer_players >> position_list >> end_operator