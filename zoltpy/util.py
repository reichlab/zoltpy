import csv
import io
import json
import logging
import os
import sys
import time
from pathlib import Path

import pandas as pd
import requests

from zoltpy.cdc_io import json_io_dict_from_cdc_csv_file
from zoltpy.connection import ZoltarConnection, Project
from zoltpy.csv_io import csv_rows_from_json_io_dict


logger = logging.getLogger(__name__)


#
# This file defines some higher-level functions that try to simplify zoltpy use, rather than having to call individual
# ZoltarConnection operations.
#

def create_project(conn, project_json):
    """
    Creates a project from a json configuration file.

    :param conn: a ZoltarConnection
    :param project_json: configuration json file for the project of interest. see zoltar documentation for details
    """
    conn.re_authenticate_if_necessary()
    with open(project_json) as fp:
        project_dict = json.load(fp)

    # delete existing project if found
    existing_project = [project for project in conn.projects if project.name == project_dict["name"]]
    if existing_project:
        existing_project = existing_project[0]
        logger.info(f"deleting existing project: {existing_project}")
        existing_project.delete()
        logger.info("delete done")

    # create new project
    logger.info(f"creating new project. project name={project_dict['name']}")
    response = requests.post(f'{conn.host}/api/projects/',
                             headers={'Authorization': f'JWT {conn.session.token}'},
                             json={'project_config': project_dict})
    if response.status_code != 200:  # HTTP_200_OK
        raise RuntimeError(f"status_code was not 200. status_code={response.status_code}, text={response.text}")

    new_project_json = response.json()
    new_project = Project(conn, new_project_json["url"], new_project_json)
    logger.info(f"created new project: {new_project}")
    return new_project


def delete_forecast(conn, project_name, model_name, timezero_date):
    """Deletes the forecast corresponding to the args.

    :param conn: a ZoltarConnection
    :param project_name: name of the Project that contains model_name
    :param model_name: name of the Model that contains a Forecast for timezero_date
    :param timezero_date: YYYY-MM-DD DATE FORMAT, e.g., '2018-12-03'
    """
    conn.re_authenticate_if_necessary()
    project = [project for project in conn.projects if project.name == project_name][0]
    model = [model for model in project.models if model.name == model_name][0]
    forecast_for_tz_date = [forecast for forecast in model.forecasts if forecast.timezero.timezero_date == timezero_date]
    if forecast_for_tz_date:
        existing_forecast = forecast_for_tz_date[0]
        logger.info(
            f"delete_forecast(): deleting existing forecast. model={model.id}, timezero_date={timezero_date}, "
            f"existing_forecast={existing_forecast.id}")
        existing_forecast.delete()
        logger.info(f"delete_forecast(): delete done")
    else:
        logger.info(f"delete_forecast(): no existing forecast. model={model.id}, timezero_date={timezero_date}")


def delete_model(conn, project_name, model_name):
    """Deletes a model corresponding to the args.

    :param conn: a ZoltarConnection
    :param project_name: name of the Project that contains model_name
    :param model_name: name of the Model that contains a Forecast for timezero_date
    """
    conn.re_authenticate_if_necessary()
    project = [project for project in conn.projects if project.name == project_name][0]
    model = [model for model in project.models if model.name == model_name][0]
    # num_forecasts = len(model.forecasts) - TODO
    if model:
        proceed = input("%s may have forecasts - these WILL BE DELETED.\nReturn Y to Proceed, N to Cancel: "
                        % (model_name))
        if proceed == "Y":
            logger.info(
                f"delete_model(): deleting existing model. model={model.id}, ")
            model.delete()
            logger.info(f"delete_model(): delete done")
    else:
        logger.info(f"delete_model(): no existing model. model={model_name}")


def upload_forecast(conn, json_io_dict, forecast_filename, project_name, model_name, timezero_date, notes='',
                    overwrite=False):
    """
    Uploads the passed JSON dictionary file to the model corresponding to the args.

    :param conn: a ZoltarConnection
    :param json_io_dict: a JSON dictionary
    :param forecast_filename: filename of original forecast
    :param project_name: name of the Project that contains model_name
    :param model_name: name of the Model that contains a Forecast for timezero_date
    :param timezero_date: YYYY-MM-DD DATE FORMAT, e.g., '2018-12-03'
    :param notes: optional user notes for the new forecast
    :param overwrite: True if you would like to overwrite the existing forecast for that timezero_date. Default is False
    :return: an UploadFileJob. it can be polled for status via busy_poll_upload_file_job(), and then the new forecast
        can be obtained via upload_file_job.output_json['forecast_pk']
    """
    conn.re_authenticate_if_necessary()

    if overwrite:
        delete_forecast(conn, project_name, model_name, timezero_date)

    # get projects
    projects = conn.projects
    project = [project for project in projects if project.name == project_name][0]

    # get models for project
    models = project.models
    model = [model for model in models if model.name == model_name][0]

    # check json formatting before upload
    # accepts either string or dictionary
    if isinstance(json_io_dict, str):
        try:
            with open(json_io_dict) as jsonfile:
                json_io_dict = json.load(jsonfile)
        except:
            print("""\nERROR - cannot read JSON Format. 
            Uploading a CSV? Consider converting to json Predx style with:
            predx_json, forecast_filename = util.convert_cdc_csv_to_json_io_dict(forecast_file_path)""")
            sys.exit(1)

    upload_file_job = model.upload_forecast(json_io_dict, forecast_filename, timezero_date, notes)
    return busy_poll_upload_file_job(upload_file_job)


def upload_forecast_batch(conn, json_io_dict_batch, forecast_filename_batch, project_name, model_name,
                          timezero_date_batch, overwrite=False):
    """
    Uploads a batch (list) of JSON dictionaries to the model corresponding
    to the args. This only iterates through timezeros, not models or projects.

    :param conn: a ZoltarConnection
    :param json_io_dict_batch: an list of a JSON dictionaries,
    :param forecast_filename_batch: a list of filenames of original forecast, paired with json_io_dict_batch
    :param project_name: name of the Project that contains model_name
    :param model_name: name of the Model that contains a Forecast for timezero_date
    :param timezero_date_batch: an list of YYYY-MM-DD DATE FORMAT, e.g., '2018-12-03', , paired with json_io_dict_batch
    :param overwrite: True if you would like to overwrite the existing forecast for that timezero_date. Default is False
    :return: the last UploadFileJob. it can be polled for status via busy_poll_upload_file_job(), and then the new
        forecast can be obtained via upload_file_job.output_json['forecast_pk']. returns None if no uploads were done
    """
    if not (len(json_io_dict_batch) == len(forecast_filename_batch) == len(timezero_date_batch)):
        raise RuntimeError(f"batch args had different lengths: json_io_dict_batch, forecast_filename_batch, "
                           f"timezero_date_batch: {len(json_io_dict_batch)}, {len(forecast_filename_batch)}, "
                           f"{len(timezero_date_batch)}")
    elif not json_io_dict_batch:
        raise RuntimeError(f"no forecasts to upload")

    conn.re_authenticate_if_necessary()
    project = [project for project in conn.projects if project.name == project_name][0]
    models = project.models
    model = [model for model in models if model.name == model_name][0]

    print(f"uploading {len(json_io_dict_batch)} forecasts...")
    upload_file_jobs = []
    for json_io_dict, forecast_filename, timezero_date in \
            zip(json_io_dict_batch, forecast_filename_batch, timezero_date_batch):
        print(f"uploading {project_name!r} project, {model_name!r} model, {timezero_date !r} timezero...")
        if overwrite:
            delete_forecast(conn, project_name, model_name, timezero_date)
        upload_file_job = model.upload_forecast(json_io_dict, forecast_filename, timezero_date)
        upload_file_jobs.append(upload_file_job)
        print("upload complete")
    return upload_file_jobs[-1] if upload_file_jobs else None


def download_forecast(conn, project_name, model_name, timezero_date):
    """
    Downloads the data for the forecast corresponding to the args, in Zoltar's native json format, AKA a "json_io_dict".
    The resulting dict can then be passed to dataframe_from_json_io_dict(), or to the underlying csv utility function
    `csv_rows_from_json_io_dict()`

    :param conn: a ZoltarConnection
    :param project_name: name of the Project that contains model_name
    :param project_name: name of the Project that contains model_name
    :param model_name: name of the Model that contains a Forecast for timezero_date
    :param timezero_date: YYYY-MM-DD DATE FORMAT, e.g., '2018-12-03'
    :return: a json_io_dict
    """
    conn.re_authenticate_if_necessary()
    project = [project for project in conn.projects if project.name == project_name][0]
    model = [model for model in project.models if model.name == model_name][0]
    forecast_for_tz_date = [forecast for forecast in model.forecasts if forecast.timezero_date == timezero_date]
    if not forecast_for_tz_date:
        raise RuntimeError(f"forecast not found. project_name={project_name}, model_name={model_name}, "
                           f"timezero_date={timezero_date}")

    existing_forecast = forecast_for_tz_date[0]
    return existing_forecast.data()


def dataframe_from_rows(rows):
    string_io = io.StringIO()
    csv_writer = csv.writer(string_io, delimiter=",")
    for row in rows:
        csv_writer.writerow(row)
    string_io.seek(0)
    # dtype=str -> don't convert any numbers. o/w it only coverts columns with uniform type:
    return pd.read_csv(string_io, delimiter=",", dtype=str)


def dataframe_from_json_io_dict(json_io_dict):
    """
    Converts the passed native Zoltar json_io_dict to CSV data, returned as a Pandas DataFrame. Does not cast cells to
    their appropriate type based on target types b/c we do not have target information here.

    :param json_io_dict: a json_io_dict as returned by download_forecast()
    :return: a Pandas DataFrame
    """
    return dataframe_from_rows(csv_rows_from_json_io_dict(json_io_dict))


def busy_poll_upload_file_job(upload_file_job):
    """A simple utility that polls upload_file_job's status every second until
    either success or failure."""
    print(f"\n* polling for status change. upload_file_job: {upload_file_job}")
    while True:
        status = upload_file_job.status_as_str
        failure_message = upload_file_job.json["failure_message"]
        print(f"- {status}")
        if status == "FAILED":
            print("x FAILED")
            print("\n", failure_message)
            break
        if status == "SUCCESS":
            break
        time.sleep(1)
        upload_file_job.refresh()


def authenticate(env_user="Z_USERNAME", env_pass="Z_PASSWORD"):
    """Authenticate the user ID and password for connection to Zoltar.

    :param env_user: username of account in Zoltar
    :param env_pass: password ""
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
        conn.authenticate(os.environ.get(env_user), os.environ.get(env_pass))
        return conn
    except:
        print("ERROR: Cannot authenticate zoltar credentials")
        print("Ensure the environment variables for your username and password are correct")
    return print("ERROR")


def print_projects():
    """A simple utility that outputs a list of projects within Zoltar."""
    print("* projects")
    zoltar = authenticate()
    for project in zoltar.projects:
        print("-", project, project.id, project.name)


def print_models(conn, project_name):
    """A simple utility that outputs a list of models a Zoltar project."""
    print("* models in %s" % project_name)
    zoltar = authenticate()
    project = [project for project in conn.projects if project.name == project_name][0]
    for model in project.models:
        print("-", model)


def convert_cdc_csv_to_json_io_dict(season_start_year, filepath):
    """Converts the passed cdc forecast file to native Zoltar json_io_dict.

    :param season_start_year: the start year of the season (as an integer)
    :param filepath: a file path to the forecast file that needs to be convereted
    :return: a tuple of the json_io_dict and the filename of original forecast
    """
    with open(filepath) as cdc_file:
        json_io_dict = json_io_dict_from_cdc_csv_file(season_start_year, cdc_file)
        forecast_file = Path(filepath)
        forecast_filename = forecast_file.name
        return json_io_dict, forecast_filename
