import requests
import logging
import csv
import gzip
import io
import json

from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

def upload_trx_files_to_s3(**context):
    s3_hook = S3Hook()
    
    s3_hook.load_file(
        filename=f'/opt/airflow/data/transactions/loanTrx_{context["ds"]}.csv',
        key=f'loanTrx/loanTrx_{context["ds"]}.csv',
        bucket_name='raw',
        replace=True
    )
