doc_md = """
### General:
---
- **Author**: {X.X@offerup.com}, {Team}, {position}
- **Creation date**: `{date yyyy-mm-dd}`
- **Owner/Requester**: {X.X@offerup.com}, {Team}, {position}
- **Last reviewed date**: `{date yyyy-mm-dd}`
---
### SLA:
- **Delivery:** `{XX:XX PDT Everyday | XX:XX UTC}`
- **Latency:** `{XX:XX PDT Everyday | XX:XX UTC}`
 
### Purpose:
{One or two paragraphs explaining what you are doing with the data and why you need it.}
 
- **Business**: {TEXT eg: More revenue ...}
- **Technical**: {TEXT eg: Automate sales team ...}
 
### Support:
- In case you need help, reach out to:
   - **Internal Customer (OU)**: {TEXT: Include emails and description on how reach out}
   - **External users/support** : {TEXT: It mean third party data, vendor, etc}
 
### How does the DAG work?
{TEXT: A brief list explaining the steps that the DAG uses to accomplish the ETL.}
 
- {TEXT}
- {TEXT}
- {TEXT: the quantity depends on how many bullets you need}
 
### Documentation:
{Put here only the links to documentation, keep the format and add them as you need}
 
- **User guide (OU):**: [Link name]({url})
- **Technical guide (OU):**: [Link name]({url})
- **Vendors**: [Link name]({url})
- **Training**: [Link name]({url})
- **Link to API docs**: [Link name]({url})
- **DataDog Dashboards**: [Link name]({url})
 
### Failure/Rollback/Notify Strategy:
##### Rollback Strategy:
- {TEXT: A brief explanation of how the failures and rollbacks have to be handled.}
##### Notify strategy:
- {TEXT: Also should note any notification strategies, Like "please contact the x team(s) if this fails twice in a row or doesn't recover within 2 hours."}
### Notes
 
{TEXT: Considerations, special cases, etc. Whatever you consider the most important to know about the DAG. API and airflow requirements to my documentation. For example, if OU account, api key, or variables are needed. ADD ALL THE TOPICS THAT DO NOT FIT IN THE OTHER SECTIONS}.
 
- **{WHATEVER}**: {TEXT}
     - **{WHATEVER}**: {TEXT}
 
### Change log:
[comment]: <> (Keep the table format and quotes at ticket number and dates)
 
| Date                | Author            | Jira Ticket  | Summary              |
| :---------:         |    :--------:     |  :--------:  |  :------             |
| `{date yyyy-mm-dd}` |  {User name}      | `{ticket}`   |  {TEXT: eg: First release}    |
| `{date yyyy-mm-dd}` |  {User name}      | `{ticket}`   |  {TEXT: like a commit comment} . |
"""


from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.decorators import task
from airflow.utils.trigger_rule import TriggerRule


from random import uniform
from datetime import datetime

default_args = {
    'owner': 'DataOperations',
}

def _training_model(ti):
    accuracy = uniform(0.1, 10.0)
    print("Accuracy of the model: {accuracy}".format(accuracy=accuracy))
    ti.xcom_push(key='model_accuracy', value=accuracy)

def choose_best_model(**context):
    accuracies = context["ti"].xcom_pull(key='model_accuracy', 
                                            task_ids=['processing_model.training_model_A',
                                                    'processing_model.training_model_B',
                                                    'processing_model.training_model_C'])
    best_accuracy = max(accuracies)
    print("Accuracy", best_accuracy)
    if best_accuracy > 5:
        return ['accurate', 'best_performance']
    return 'inaccurate'

with DAG(
    dag_id='branching',
    start_date= datetime(2023,4,19),
    schedule='@daily',
    catchup=False,
    default_args=default_args,
    doc_md=doc_md
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
    
    accurate = DummyOperator(
        task_id='accurate'
    )
    
    inaccurate = DummyOperator(
        task_id='inaccurate'
    )
    
    best_performance = DummyOperator(
        task_id='best_performance'
    )
    
    execute = DummyOperator(
        task_id='execute',
        trigger_rule='none_failed_or_skipped'
    )
    
    # @task(task_id='choose_model')
    # def choose_best_model(**context):
    #     best_value = context["ti"].xcom_pull(key='model_accuracy', task_ids='processing_model.training_model_B')
    #     print(f"El mejor valor es: {best_value}")
    #     return best_value
    
    
    choose_model = BranchPythonOperator(
        task_id='choose_best_model',
        python_callable=choose_best_model
    )
    
    #choose = choose_best_model()
    
    start >> downloading_data >> processing_model >> choose_model >> [accurate, inaccurate, best_performance] >> execute >> end
    

