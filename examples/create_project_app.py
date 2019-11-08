import json
import os
import sys

import requests

from zoltpy.connection import ZoltarConnection, Project


def create_project_app():
    """Application that demonstrates project creation and deletion.

    App args:
    - zoltar_host: host to pass to ZoltarConnection()
    - project_config_file: configuration json file for the project of interest. see zoltar documentation for details,
        esp. utils.project.create_project_from_json()

    Required environment variables:
    - 'Z_USERNAME': username of the account that has permission to access the resources in above app args
    - 'Z_PASSWORD': password ""
    """
    #host = sys.argv[1]
    project_config_file = sys.argv[1]

    conn = ZoltarConnection()
    conn.authenticate(os.environ.get('Z_USERNAME'), os.environ.get('Z_PASSWORD'))

    with open(project_config_file) as fp:
        project_dict = json.load(fp)

    # delete existing project if found
    project_dict = project_dict[0]
    existing_project = [project for project in conn.projects if project.name == project_dict['name']]
    if existing_project:
        existing_project = existing_project[0]
        print(f"deleting existing project: {existing_project}")
        existing_project.delete()
        print("delete done")

    # create new project
    print(f"creating new project. project name={project_dict['name']}")
    response = requests.post(f'{conn.host}/api/projects/',
                             headers={'Authorization': f'JWT {conn.session.token}'},
                             json={'project_config': project_dict})
    if response.status_code != 200:  # HTTP_200_OK
        raise RuntimeError(f"status_code was not 200. status_code={response.status_code}, text={response.text}")

    new_project_json = response.json()
    new_project = Project(conn, new_project_json['url'])
    print(f"created new project: {new_project}")


if __name__ == '__main__':
    create_project_app()
