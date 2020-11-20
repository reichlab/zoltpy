import json
import os

from zoltpy.cdc_io import json_io_dict_from_cdc_csv_file
from zoltpy.connection import ZoltarConnection, QueryType
from zoltpy.quantile_io import json_io_dict_from_quantile_csv_file
from zoltpy.util import busy_poll_job, create_project, dataframe_from_json_io_dict, dataframe_from_rows


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

    # get the project's truth detail
    print(f'\n* truth for {project}')
    print(f'- truth_csv_filename, truth_updated_at: {project.truth_csv_filename}, {project.truth_updated_at}')

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
        json_io_dict, error_messages = \
            json_io_dict_from_quantile_csv_file(fp, ['1 wk ahead cum death', '1 day ahead inc hosp'])
    print(f"- converted quantile data to json: {len(json_io_dict['predictions'])} predictions")  # 5 predictions

    # convert to a Pandas DataFrame
    print(f'\n* working with a pandas data frame')
    dataframe = dataframe_from_json_io_dict(forecast_data)
    print(f'- dataframe: {dataframe}')

    # query forecast data
    print(f"\n* querying forecast data")
    query = {'targets': ['pct next week', 'cases next week'], 'types': ['point']}
    job = project.submit_query(QueryType.FORECASTS, query)
    busy_poll_job(job)  # does refresh()
    rows = job.download_data()
    print(f"- got {len(rows)} forecast rows. as a dataframe:")
    print(dataframe_from_rows(rows))

    # query score data
    print(f"\n* querying score data")
    query = {'targets': ['pct next week', 'cases next week'], 'scores': ['abs_error', 'pit']}
    job = project.submit_query(QueryType.SCORES, query)
    busy_poll_job(job)  # does refresh()
    rows = job.download_data()
    print(f"- got {len(rows)} score rows. as a dataframe:")
    print(dataframe_from_rows(rows))

    # query score data
    print(f"\n* querying truth data")
    query = {'targets': ['pct next week', 'cases next week']}
    job = project.submit_query(QueryType.TRUTH, query)
    busy_poll_job(job)  # does refresh()
    rows = job.download_data()
    print(f"- got {len(rows)} truth rows. as a dataframe:")
    print(dataframe_from_rows(rows))

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

    # upload truth
    print(f"\n* uploading truth")
    with open('tests/docs-ground-truth.csv') as csv_fp:
        job = project.upload_truth_data(csv_fp)
    busy_poll_job(job)
    print(f"- upload truth done")

    # create a model, upload a forecast, query the project, then delete it
    print(f"\n* creating model")
    with open("examples/example-model-config.json") as fp:
        model = project.create_model(json.load(fp))
    print(f"- created model: {model}")

    print(f"\n* uploading forecast. pre-upload forecasts: {model.forecasts}")
    with open("examples/docs-predictions.json") as fp:
        json_io_dict = json.load(fp)
        job = model.upload_forecast(json_io_dict, "docs-predictions.json", "2011-10-02", "some predictions")
    busy_poll_job(job)
    new_forecast = job.created_forecast()
    print(f"- uploaded forecast: {new_forecast}")

    model.refresh()
    print(f'\n* post-upload forecasts: {model.forecasts}')

    print(f"\n* deleting forecast: {new_forecast}")
    job = new_forecast.delete()
    busy_poll_job(job)
    print(f"- deleting forecast: done")

    # clean up by deleting the sandbox project. NB: This will delete all of the data associated with the project without
    # warning, including models and forecasts
    print(f"\n* deleting project {project}")
    project.delete()
    print("- deleted project")

    print("\n* app done!")


if __name__ == '__main__':
    zoltar_connection_app()
