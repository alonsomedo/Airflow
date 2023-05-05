from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.decorators import task


from random import uniform
from datetime import datetime

default_args = {
    'owner': 'DataOperations',
}

def _training_model(ti):
    accuracy = uniform(0.1, 10.0)
    print("Accuracy of the model: {accuracy}".format(accuracy=accuracy))
    ti.xcom_push(key='model_accuracy', value=accuracy)


with DAG(
    dag_id='xcom_dag',
    start_date= datetime(2023,4,19),
    schedule='@daily',
    catchup=False,
    default_args=default_args
) as dag:
        
    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')
    
    downloading_data = BashOperator(
        task_id='downloading_data',
        bash_command='sleep 5'
    )
    
    with TaskGroup('processing_model') as processing_model:
        training_model_A = PythonOperator(
            task_id='training_model_A',
            python_callable=_training_model
        )
            
        training_model_B = PythonOperator(
            task_id='training_model_B',
            python_callable=_training_model
        )
        
        training_model_C = PythonOperator(
            task_id='training_model_C',
            python_callable=_training_model
        )
    
    
    @task(task_id='choose_model')
    def choose_best_model(**context):
        best_value = context["ti"].xcom_pull(key='model_accuracy', task_ids='processing_model.training_model_B')
        print(f"El mejor valor es: {best_value}")
        return best_value
    
    # choose_model = PythonOperator(
    #     task_id='choose_best_model',
    #     python_callable=
    # )
    
    #choose = choose_best_model()
    
    start >> downloading_data >> processing_model >> choose_best_model() >> end
    

