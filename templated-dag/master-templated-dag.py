from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from path.to.templated_dag import main as generate_templated_dag  # Import the main function from templated-dag.py

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 9, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG(
    dag_id='master_template_dag',
    default_args=default_args,
    schedule_interval=None,  # Run manually, or change as needed
    catchup=False
) as dag:

    # Task 1: Call the templated-dag.py script to generate and upload the rendered DAG to GCS
    generate_dag_task = PythonOperator(
        task_id='generate_templated_dag',
        python_callable=generate_templated_dag,  # Calls the main function from templated-dag.py
        dag=dag,
    )

    # Task 2: Print success message once the DAG is generated
    def print_success():
        print("Templated DAG successfully generated and uploaded to GCS!")

    success_task = PythonOperator(
        task_id='print_success',
        python_callable=print_success,
        dag=dag,
    )

    # Define the task dependencies
    generate_dag_task >> success_task