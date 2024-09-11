from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import yaml
from jinja2 import Template

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 9, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the function to load config and generate the templated DAG
def load_config(file_path):
    with open(file_path, 'r') as file:
        config = yaml.safe_load(file)
    return config

def render_template(config, template_path, output_path):
    with open(template_path, 'r') as file:
        template_content = file.read()
    
    template = Template(template_content)
    rendered_content = template.render(config)
    
    # Write the rendered content to a new Python file (DAG)
    with open(output_path, 'w') as output_file:
        output_file.write(rendered_content)

# Define the function to be executed in the PythonOperator
def generate_templated_dag(**kwargs):
    # Load configuration and render the template
    config = load_config('/path/to/your/config.yaml')
    render_template(config, '/path/to/your/template.jinja', '/path/to/output/rendered_dag.py')

# Define the master DAG
with DAG(
    dag_id='master_templated_dag',
    default_args=default_args,
    schedule_interval=None,
    catchup=False
) as dag:

    # Task 1: Generate the templated DAG by rendering the Jinja template with the config
    generate_dag_task = PythonOperator(
        task_id='generate_templated_dag',
        python_callable=generate_templated_dag,
        provide_context=True,
        dag=dag,
    )

    # Task 2: Print a success message
    def print_success():
        print("Templated DAG successfully generated and ready for execution.")

    success_task = PythonOperator(
        task_id='print_success',
        python_callable=print_success,
        dag=dag,
    )

    # Define task dependencies
    generate_dag_task >> success_task