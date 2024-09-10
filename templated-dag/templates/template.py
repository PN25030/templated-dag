from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
}

# {{ Dataset }}, {{ Table_name }}, and {{ Target_gcs_loc }} are placeholders
def select_data_from_table(ds, **kwargs):
    dataset = '{{ Dataset }}'
    table = '{{ Table_name }}'
    gcs_location = '{{ Target_gcs_loc }}'
    
    sql_query = f"SELECT * FROM {dataset}.{table}"
    print(f"Executing query: {sql_query}")
    print(f"Target GCS Location: {gcs_location}")

with DAG('dynamic_dag', default_args=default_args, schedule_interval=None) as dag:
    
    run_query = PythonOperator(
        task_id='run_select_query',
        python_callable=select_data_from_table,
        provide_context=True,
    )

run_query
