from google.cloud import storage
import yaml
from jinja2 import Template

# Function to load the YAML configuration
def load_config(file_path):
    with open(file_path, 'r') as file:
        config = yaml.safe_load(file)
    return config

# Function to upload a file to GCS
def upload_to_gcs(bucket_name, destination_blob_name, content):
    # Initialize the GCS client
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    
    # Upload the rendered content to GCS
    blob.upload_from_string(content)
    print(f"File successfully uploaded to gs://{bucket_name}/{destination_blob_name}")

# Function to render the Jinja template and upload to GCS
def render_template(config, template_path, bucket_name, destination_blob_name):
    with open(template_path, 'r') as file:
        template_content = file.read()
    
    template = Template(template_content)
    rendered_content = template.render(config)
    
    # Upload the rendered content directly to GCS
    upload_to_gcs(bucket_name, destination_blob_name, rendered_content)

def main():
    # Paths and GCS bucket information
    config_path = '/path/to/your/config.yaml'  # Update this path
    template_path = '/path/to/your/template.jinja'  # Update this path
    bucket_name = 'your-gcs-bucket-name'  # Replace with your GCS bucket name
    destination_blob_name = 'path/in/gcs/rendered_dag.py'  # Specify the destination path in GCS
    
    # Load the configuration and render the template, then upload to GCS
    config = load_config(config_path)
    render_template(config, template_path, bucket_name, destination_blob_name)

# This allows the script to be run both directly and imported
if __name__ == "__main__":
    main()