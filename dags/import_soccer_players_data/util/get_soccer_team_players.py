import logging
import json
import requests
from pandas import json_normalize
from airflow.models import Variable


def get_soccer_team_players(soccer_team_id, endpoint):
    url = endpoint
    params = {'team': soccer_team_id}
    headers = {
        'x-rapidapi-host': "v3.football.api-sports.io",
        'x-rapidapi-key': Variable.get('api_key_soccer')
    }
    response = requests.get(url, params=params, headers=headers)
    print(response.text)
    data = json.loads(response.text)['response'][0]['players']
    data = json_normalize(data)
    data.to_csv(f'/opt/airflow/data/{soccer_team_id}.csv', sep='\t', index=False, header=False)
    logging.info(f"The file {soccer_team_id}.csv was generated successfully.")