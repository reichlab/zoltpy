import csv
import io
import os
import json
import logging
import tempfile
import time
import requests
import pandas as pd
from pathlib import Path
from zoltpy.connection import ZoltarConnection, Project
from zoltpy.cdc import cdc_csv_rows_from_json_io_dict, json_io_dict_from_cdc_csv_file
from zoltpy.csv_util import csv_rows_from_json_io_dict

logger = logging.getLogger(__name__)


#
# This file defines some higher-level functions that try to simplify zoltpy use, rather than having to call individual
# ZoltarConnection operations.
#

def create_project(conn, project_json):
    """Creates a project from a json file.

    :param conn: a ZoltarConnection
    :param project_json: configuration json file for the project of interest. see zoltar documentation for details,
        esp. utils.project.create_project_from_json()
    """
    conn.re_authenticate_if_necessary()
    with open(project_json) as fp:
        project_dict = json.load(fp)

    # delete existing project if found
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


def delete_forecast(conn, project_name, model_name, timezero_date):
    """Deletes the forecast corresponding to the args.

    :param conn: a ZoltarConnection
    :param project_name: name of the Project that contains model_name
    :param model_name: name of the Model that contains a Forecast for timezero_date
    :param timezero_date: YYYYMMDD_DATE_FORMAT, e.g., '20181203'
    """
    conn.re_authenticate_if_necessary()
    project = [project for project in conn.projects if project.name == project_name][0]
    model = [model for model in project.models if model.name == model_name][0]
    forecast_for_tz_date = [forecast for forecast in model.forecasts if forecast.timezero_date == timezero_date]
    if forecast_for_tz_date:
        existing_forecast = forecast_for_tz_date[0]
        logger.info(f'delete_forecast(): deleting existing forecast. model={model.id}, timezero_date={timezero_date}, '
                    f'existing_forecast={existing_forecast.id}')
        existing_forecast.delete()
        logger.info(f'delete_forecast(): delete done')
    else:
        logger.info(f'delete_forecast(): no existing forecast. model={model.id}, timezero_date={timezero_date}')


def upload_forecast(conn, json_io_dict, forecast_filename, project_name, model_name, timezero_date, 
        data_version_date=None, overwrite=False):
    """Uploads the passed JSON dictionary file to the model corresponding to
    the args.

    :param conn: a ZoltarConnection
    :param json_io_dict: a JSON dictionary
    :param forecast_filename: filename of original forecast
    :param project_name: name of the Project that contains model_name
    :param model_name: name of the Model that contains a Forecast for timezero_date
    :param timezero_date: YYYYMMDD_DATE_FORMAT, e.g., '20181203'
    :param data_version_date: optional for the upload. same format as timezero_date
    :param overwrite: True if you would like to overwrite the existing forecast for that timezero_date. Default is False
    :return: an UploadFileJob. it can be polled for status via busy_poll_upload_file_job(), and then the new forecast
        can be obtained via upload_file_job.output_json['forecast_pk']
    """
    conn.re_authenticate_if_necessary()
    if overwrite==True:
        delete_forecast(conn, project_name, model_name, timezero_date)
    project = [project for project in conn.projects if project.name == project_name][0]
    model = [model for model in project.models if model.name == model_name][0]

    # note that this app accepts a *.cdc.csv file, but zoltar requires a native json file. so we first convert to a
    # temp json file and then pass it
    with tempfile.TemporaryFile('r+') as json_fp:
        json.dump(json_io_dict, json_fp)
        json_fp.seek(0)
        upload_file_job = model.upload_forecast(json_fp, forecast_filename, timezero_date, data_version_date)

    return upload_file_job


def upload_forecast_batch(conn, json_io_dict_batch, forecast_filename_batch, project_name, model_name, timezero_date_batch, 
        data_version_date=None, overwrite=False):
    """Uploads a batch (list) of JSON dictionaries to the model corresponding
    to the args. This only iterates through timezeros, not models or projects.

    :param conn: a ZoltarConnection
    :param json_io_dict_batch: an list of a JSON dictionaries
    :param forecast_filename_batch: a list of filenames of original forecast
    :param project_name: name of the Project that contains model_name
    :param model_name: name of the Model that contains a Forecast for timezero_date
    :param timezero_date_batch: an list of YYYYMMDD_DATE_FORMAT, e.g., '20181203'
    :param data_version_date: optional for the upload. same format as timezero_date
    :param overwrite: True if you would like to overwrite the existing forecast for that timezero_date. Default is False
    :return: an UploadFileJob. it can be polled for status via busy_poll_upload_file_job(), and then the new forecast
        can be obtained via upload_file_job.output_json['forecast_pk']
    """
    print(model_name)
    conn.re_authenticate_if_necessary()
    project = [project for project in conn.projects if project.name == project_name][0]
    model = [model for model in project.models if model.name == model_name][0]
    
    print('uploading %i forecasts...' % len(forecast_filename_batch))
    
    if len(json_io_dict_batch) > 0:
        for i in range(len(json_io_dict_batch)):
            print('uploading %s project, %s model, %s timezero...' % (project_name, 
                            model_name, timezero_date_batch[i]))
            if overwrite == True:
                delete_forecast(conn, project_name, model_name, timezero_date_batch[i])

            # note that this app accepts a *.cdc.csv file, but zoltar requires a native json file. so we first convert to a
            # temp json file and then pass it
            with tempfile.TemporaryFile('r+') as json_fp:
                json.dump(json_io_dict_batch[i], json_fp)
                json_fp.seek(0)
                upload_file_job = model.upload_forecast(json_fp, forecast_filename_batch[i], 
                                        timezero_date_batch[i], data_version_date)
            print('upload complete')
        return upload_file_job


def download_forecast(conn, project_name, model_name, timezero_date):
    """Downloads the data for the forecast corresponding to the args, in
    Zoltar's native json format, AKA a "json_io_dict". The resulting dict can
    then be passed to dataframe_from_json_io_dict(), or to either of the
    underlying csv utility functions:csv_rows_from_json_io_dict(),
    cdc_csv_rows_from_json_io_dict().

    :param conn: a ZoltarConnection
    :param project_name: name of the Project that contains model_name
    :param project_name: name of the Project that contains model_name
    :param model_name: name of the Model that contains a Forecast for timezero_date
    :param timezero_date: YYYYMMDD_DATE_FORMAT, e.g., '20181203'
    :return: a json_io_dict
    """
    conn.re_authenticate_if_necessary()
    project = [project for project in conn.projects if project.name == project_name][0]
    model = [model for model in project.models if model.name == model_name][0]
    forecast_for_tz_date = [forecast for forecast in model.forecasts if forecast.timezero_date == timezero_date]
    if not forecast_for_tz_date:
        raise RuntimeError(f'forecast not found. project_name={project_name}, model_name={model_name}, '
                           f'timezero_date={timezero_date}')

    existing_forecast = forecast_for_tz_date[0]
    return existing_forecast.data()


def dataframe_from_json_io_dict(json_io_dict, is_cdc_format=False):
    """Converts the passed native Zoltar json_io_dict to CSV data, returned as
    a Pandas DataFrame.

    :param json_io_dict: a json_io_dict as returned by download_forecast()
    :param is_cdc_format: flag that specifies CDC CSV format (if True) or generic Zoltar format o/w
    :return: a Pandas DataFrame
    """
    string_io = io.StringIO()
    csv_writer = csv.writer(string_io, delimiter=',')
    csv_rows = cdc_csv_rows_from_json_io_dict(json_io_dict) if is_cdc_format \
        else csv_rows_from_json_io_dict(json_io_dict)
    for row in csv_rows:
        csv_writer.writerow(row)
    string_io.seek(0)
    dataset = pd.read_csv(string_io, delimiter=",")
    return dataset


def busy_poll_upload_file_job(upload_file_job):
    """A simple utility that polls upload_file_job's status every second until
    either success or failure."""
    print(f'\n* polling for status change. upload_file_job: {upload_file_job}')
    while True:
        status = upload_file_job.status_as_str
        print(f'- {status}')
        if status == 'FAILED':
            print('x failed')
            break
        if status == 'SUCCESS':
            break
        time.sleep(1)
        upload_file_job.refresh()


def authenticate(env_user='Z_USERNAME', env_pass='Z_PASSWORD'):
    """Authenticate the user ID and password for connection to Zoltar.

    :param Z_USERNAME environment variable: username of account in Zoltar
    :param Z_PASSWORD environment variable: password for
    """
    # Ensure environment variables exist
    env_vars = [env_user, env_pass]
    for var in env_vars:
        if os.environ.get(var) == None:
            print("\nERROR: Cannot locate environment variable:  %s" % var)
            print("\nPC users, try the command: set %s='<your zoltar username>'" % var)
            print("Mac users, try the command: export %s=<your zoltar username>" % var)
            print("Then, Refresh the command window\n")
            return
    # Authenticate Zoltar connection
    try:
        conn = ZoltarConnection()
        conn.authenticate(os.environ.get(
            env_user), os.environ.get(env_pass))
        return conn
    except:
        print("ERROR: Cannot authenticate zoltar credentials")
        print("Ensure the environment variables for your username and password are correct")
    return print("ERROR")
    

def print_projects():
    """A simple utility that outputs a list of projects within Zoltar."""
    print('* projects')
    zoltar = authenticate()
    for project in zoltar.projects:
        print('-', project, project.id, project.name)


def convert_cdc_csv_to_json_io_dict(filepath):
    """Converts the passed cdc forecast file to native Zoltar json_io_dict.

    :param filepath: a file path to the forecast file that needs to be convereted
    :return: a tuple of the json_io_dict and the filename of original forecast
    """
    with open(filepath) as cdc_file:
        json_io_dict = json_io_dict_from_cdc_csv_file(cdc_file)
        forecast_file = Path(filepath)
        forecast_filename = forecast_file.name
        return json_io_dict, forecast_filename