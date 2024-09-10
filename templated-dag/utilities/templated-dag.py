import yaml
from jinja2 import Template

# Load the YAML configuration
def load_config(file_path):
    with open(file_path, 'r') as file:
        config = yaml.safe_load(file)
    return config

# Load the template file and render it with the YAML config
def render_template(config, template_path):
    with open(template_path, 'r') as file:
        template_content = file.read()
    
    template = Template(template_content)
    rendered_content = template.render(config)
    
    # Write the rendered content to a new Python file (DAG)
    with open('rendered_dag.py', 'w') as output_file:
        output_file.write(rendered_content)

# Main logic
if __name__ == "__main__":
    # Load configuration from config.yaml
    config = load_config('config.yaml')

    # Render the template with the config
    render_template(config, 'template.py')
