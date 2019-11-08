import os
import sys

from zoltpy.connection import ZoltarConnection


def create_model_app():
    """Application that demonstrates model creation and deletion.

    App args:
    - zoltar_host: host to pass to ZoltarConnection()
    - project_name: name of Project to work with
    - model_name: name of a ForecastModel to create and delete. it will be given demonstration data

    Required environment variables:
    - 'Z_USERNAME': username of the account that has permission to access the resources in above app args
    - 'Z_PASSWORD': password ""
    """
    host = sys.argv[1]
    project_name = sys.argv[2]
    model_name = sys.argv[3]

    conn = ZoltarConnection(host)
    conn.authenticate(os.environ.get('Z_USERNAME'), os.environ.get('Z_PASSWORD'))

    # delete existing model if found
    project = [project for project in conn.projects if project.name == project_name]
    if not project:
        print(f"could not find project with model_name={project_name!r}")
        return

    # show all models prior to delete/create
    project = project[0]
    print(f'\n* "before" models in {project}')
    for model in project.models:
        print(f'- {model}')

    existing_model = [model for model in project.models if model.name == model_name]
    if existing_model:
        existing_model = existing_model[0]
        print(f"deleting existing model: {existing_model}")
        existing_model.delete()
        print("delete done")

    # create new model
    model_config = {'name': 'a model_name', 'abbreviation': 'an abbreviation', 'team_name': 'a team_name',
                    'description': 'a description', 'home_url': 'http://example.com/',
                    'aux_data_url': 'http://example.com/'}
    print(f"creating new model. project={project}, model_config={model_config}")
    new_model = project.create_model(model_config)
    print(f"created new model: {new_model}")

    # show all models after delete/create
    project.refresh()
    print(f'\n* "after" models in {project}')
    for model in project.models:
        print(f'- {model}')


if __name__ == '__main__':
    create_model_app()
