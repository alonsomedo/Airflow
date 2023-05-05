import logging
from airflow.providers.mysql.hooks.mysql import MySqlHook

def load_soccer_team_players(soccer_team_id):
    file_path = f'/opt/airflow/data/{soccer_team_id}.csv'
    mysql_hook = MySqlHook()
    mysql_hook.bulk_load('soccer_players', file_path)
    logging.info("The data was loaded successfully.")