import json
import os

from zoltpy.cdc import json_io_dict_from_cdc_csv_file
from zoltpy.connection import ZoltarConnection
from zoltpy.quantile import json_io_dict_from_quantile_csv_file
from zoltpy.util import busy_poll_upload_file_job, create_project, dataframe_from_json_io_dict, dataframe_from_rows


def zoltar_connection_app():
    """
    Application demonstrating use of the library at the ZoltarConnection level (rather than using the package's
    higher-level functions such as delete_forecast(), etc.)

    - App args: None
    - Required environment variables:
      - 'Z_HOST': Zoltar host to connect to. typically "https://www.zoltardata.com"
      - 'Z_USERNAME': username of the account that has permission to access the resources in above app args
      - 'Z_PASSWORD': password ""
    """
    host = os.environ.get('Z_HOST')
    username = os.environ.get('Z_USERNAME')
    password = os.environ.get('Z_PASSWORD')

    #
    # try out non-destructive functions
    #

    # work with a connection
    conn = ZoltarConnection(host)
    conn.authenticate(username, password)
    print('\n* projects')
    for project in conn.projects:
        print(f'- {project}, {project.id}, {project.name}')

    # work with a project
    project = [project for project in conn.projects if project.name == 'Docs Example Project'][0]
    print(f'\n* working with {project}')
    print(f"- objects in {project}:\n"
          f"  = units: {project.units}\n"
          f"  = targets: {project.targets}\n"
          f"  = timezeros: {project.timezeros}\n"
          f"  = models: {project.models}")

    # get the project's truth detail and data as both rows and a dataframe
    truth_data_rows = project.truth_data()
    truth_data_df = dataframe_from_rows(truth_data_rows)
    print(f'\n* truth for {project}')
    print(f'- truth_csv_filename: {project.truth_csv_filename}')
    print(f'- truth data as rows: {len(truth_data_rows)} rows')
    print(f'- truth data as df:\n{truth_data_df.describe()}')

    # get the project's score data as both rows and a dataframe
    score_data_rows = project.score_data()
    score_data_df = dataframe_from_rows(score_data_rows)
    print(f'\n* scores for {project}')
    print(f'- score data as rows: {len(score_data_rows)} rows')
    print(f'- score data as df:\n{score_data_df.describe()}')

    # work with a model
    model = [model for model in project.models if model.name == 'docs forecast model'][0]
    print(f'\n* working with {model}')
    print(f'- forecasts: {model.forecasts}')

    # work with a forecast
    forecast = model.forecasts[0]
    print(f'\n* working with {forecast}')

    forecast_data = forecast.data()
    print(f"- data: {len(forecast_data['predictions'])} predictions")  # 26 predictions

    # work with a cdc csv file
    cdc_csv_file = "tests/EW01-2011-ReichLab_kde_US_National.csv"
    print(f'\n* working with a cdc csv file: {cdc_csv_file}')
    with open(cdc_csv_file) as fp:
        json_io_dict = json_io_dict_from_cdc_csv_file(2011, fp)
    print(f"- converted cdc data to json: {len(json_io_dict['predictions'])} predictions")  # 154 predictions

    # work with a quantile csv file
    quantile_csv_file = "tests/quantile-predictions.csv"
    print(f'\n* working with a quantile csv file: {quantile_csv_file}')
    with open(quantile_csv_file) as fp:
        json_io_dict, error_messages = json_io_dict_from_quantile_csv_file(fp)
    print(f"- converted quantile data to json: {len(json_io_dict['predictions'])} predictions")  # 5 predictions

    # convert to a Pandas DataFrame
    print(f'\n* working with a pandas data frame')
    dataframe = dataframe_from_json_io_dict(forecast_data)
    print(f'- dataframe: {dataframe}')

    #
    # try out destructive functions
    #

    # create a sandbox project to play with, deleting the existing one if any: docs-project.json
    project = [project for project in conn.projects if project.name == 'My project']
    project = project[0] if project else None
    if project:
        print(f"\n* deleting project {project}")
        project.delete()
        print("- deleted project")

    print(f"\n* creating project")
    project = create_project(conn, "examples/docs-project.json")  # "name": "My project"
    print(f"- created project: {project}")

    # create a model and then upload a forecast
    print(f"\n* creating model")
    with open("examples/example-model-config.json") as fp:
        model = project.create_model(json.load(fp))
    print(f"- created model: {model}")

    print(f"\n* uploading forecast. pre-upload forecasts: {model.forecasts}")
    with open("examples/docs-predictions.json") as fp:
        json_io_dict = json.load(fp)
        upload_file_job = model.upload_forecast(json_io_dict, "docs-predictions.json", "2011-10-02", "some predictions")
    busy_poll_upload_file_job(upload_file_job)
    print(f"- uploaded forecast: {upload_file_job.created_forecast()}")

    model.refresh()
    print(f'\n* post-upload forecasts: {model.forecasts}')

    # clean up by deleting the sandbox project. NB: This will delete all of the data associated with the project without
    # warning, including models and forecasts
    print(f"\n* deleting project {project}")
    project.delete()
    print("- deleted project")

    print("\n* app done!")


if __name__ == '__main__':
    zoltar_connection_app()
