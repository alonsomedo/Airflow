import logging
from datetime import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.operators.bash import BashOperator

from datetime import datetime

with DAG(
    dag_id='import_trx_data',
    start_date=datetime(2022,9,10),
    end_date=datetime(2022,9,12),
    schedule_interval='@daily',
    catchup=True,
    max_active_runs=1
) as dag:

    def load_loan_trx_tmp(target_date):
        file_path = f"/opt/airflow/data/transactions/loanTrx_{target_date}.csv"
        mysql_hook = MySqlHook()
        mysql_hook.bulk_load_custom('tmp_loan_trx', 
                                    file_path,
                                    extra_options="FIELDS TERMINATED BY ',' IGNORE 1 LINES")
        logging.info("The data was loaded successfully.")

    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')
    
    drop_tmp_trx_table = MySqlOperator(
        task_id='drop_tmp_trx_table',
        sql="DROP TABLE tmp_loan_trx",
    )
    
    create_tmp_tbl_trx = MySqlOperator(
        task_id='create_tmp_tbl_trx',
        sql="""
                CREATE TABLE tmp_loan_trx
                (
                    date date,
                    customerId varchar(20),
                    paymentPeriod int,
                    loanAmount	double, 
                    currencyType varchar(3),
                    evaluationChannel varchar(20),
                    interest_rate double
                );
        """
    )
    
    load_loan_trx_tmp = PythonOperator(
        task_id = 'load_loan_trx_tmp',
        python_callable=load_loan_trx_tmp,
        op_args=["{{ ds }}"]
    )
    
    delete_loan_trx_ods = MySqlOperator(
        task_id='delete_loan_trx_ods',
        sql="DELETE FROM ods_loan_trx WHERE date = '{{ ds }}'",
    )
    
    insert_loan_trx_ods = MySqlOperator(
        task_id='insert_loan_trx_ods',
        sql=""" 
            INSERT INTO ods_loan_trx 
            SELECT 
                date,
                customerId,
                paymentPeriod,
                loanAmount, 
                currencyType,
                evaluationChannel,
                interest_rate,
                {{ params.etl_insert_timestamp }}
            FROM tmp_loan_trx
            WHERE date = {{ ds }}
        """,
        params = {'etl_insert_timestamp': datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')}
    )
    
  
       
    start >> drop_tmp_trx_table >> create_tmp_tbl_trx >> load_loan_trx_tmp >> delete_loan_trx_ods >> insert_loan_trx_ods >> end 
    
