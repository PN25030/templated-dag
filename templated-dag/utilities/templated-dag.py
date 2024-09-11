import yaml
from jinja2 import Template

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

def main():
    # Define the paths to your configuration and templates
    config_path = '/path/to/your/config.yaml'
    template_path = '/path/to/your/template.jinja'
    output_path = '/path/to/output/rendered_dag.py'
    
    # Load the configuration and render the template
    config = load_config(config_path)
    render_template(config, template_path, output_path)

# This allows the script to be run both directly and imported
if __name__ == "__main__":
    main()