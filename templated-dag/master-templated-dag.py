from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 9, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the master DAG
with DAG(
    dag_id='master_templated_dag',
    default_args=default_args,
    schedule_interval=None,
    catchup=False
) as dag:

    # Task 1: Run the templated-dag.py to generate the rendered DAG
    generate_dag_task = BashOperator(
        task_id='generate_templated_dag',
        bash_command='python /path/to/your/templated-dag.py',
        dag=dag,
    )

    # Task 2: Print a message indicating that the DAG was successfully generated
    def print_success():
        print("Templated DAG successfully generated and ready for execution.")

    success_task = PythonOperator(
        task_id='print_success',
        python_callable=print_success,
        dag=dag,
    )

    # Define the task dependencies
    generate_dag_task >> success_task