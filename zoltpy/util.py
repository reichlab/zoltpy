import csv
import io
import os
import json
import logging
import tempfile
import time
from pathlib import Path
from zoltpy.connection import ZoltarConnection
from zoltpy.cdc import cdc_csv_rows_from_json_io_dict, json_io_dict_from_cdc_csv_file
from zoltpy.csv_util import csv_rows_from_json_io_dict


logger = logging.getLogger(__name__)


#
# This file defines some higher-level functions that try to simplify zoltpy use, rather than having to call individual
# ZoltarConnection operations.
#

def delete_forecast(conn, project_name, model_name, timezero_date):
    """
    Deletes the forecast corresponding to the args.

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


def upload_forecast(conn, forecast_csv_file, project_name, model_name, timezero_date, data_version_date=None):
    """
    Uploads the passed CDC CSV forecast file to the model corresponding to the args.

    :param conn: a ZoltarConnection
    :param forecast_csv_file: a CDC CSV file
    :param project_name: name of the Project that contains model_name
    :param model_name: name of the Model that contains a Forecast for timezero_date
    :param timezero_date: YYYYMMDD_DATE_FORMAT, e.g., '20181203'
    :param data_version_date: optional for the upload. same format as timezero_date
    :return: an UploadFileJob. it can be polled for status via busy_poll_upload_file_job(), and then the new forecast
        can be obtained via upload_file_job.output_json['forecast_pk']
    """
    conn.re_authenticate_if_necessary()
    forecast_csv_file = Path(forecast_csv_file)
    project = [project for project in conn.projects if project.name == project_name][0]
    model = [model for model in project.models if model.name == model_name][0]

    # note that this app accepts a *.cdc.csv file, but zoltar requires a native json file. so we first convert to a
    # temp json file and then pass it
    with tempfile.TemporaryFile('r+') as json_fp, \
            open(forecast_csv_file, 'r') as csv_fp:
        json_io_dict = json_io_dict_from_cdc_csv_file(csv_fp)
        json.dump(json_io_dict, json_fp)
        json_fp.seek(0)
        upload_file_job = model.upload_forecast(json_fp, forecast_csv_file.name, timezero_date, data_version_date)

    return upload_file_job


def download_forecast(conn, project_name, model_name, timezero_date):
    """
    Downloads the data for the forecast corresponding to the args, in Zoltar's native json format, AKA a "json_io_dict".
    The resulting dict can then be passed to dataframe_from_json_io_dict(), or to either of the underlying csv utility
    functions:csv_rows_from_json_io_dict(), cdc_csv_rows_from_json_io_dict().

    :param conn: a ZoltarConnection
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
    """
    Converts the passed native Zoltar json_io_dict to CSV data, returned as a Pandas DataFrame.

    :param json_io_dict: a json_io_dict as returned by download_forecast()
    :param is_cdc_format: flag that specifies CDC CSV format (if True) or generic Zoltar format o/w
    :return: a Pandas DataFrame
    """
    import pandas as pd


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
    """
    A simple utility that polls upload_file_job's status every second until either success or failure.
    """
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


def authenticate(env_user='USERNAME', env_pass='PASSWORD'):
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
        Connection = ZoltarConnection()
        Connection.authenticate(os.environ.get(
            env_user), os.environ.get(env_pass))
        return Connection
    except:
        print("ERROR: Cannot authenticate zoltar credentials")
        print("Ensure the environment variables for your username and password are correct")
    return print("ERROR")
    


def print_projects():
    print('* projects')
    zoltar = authenticate()
    for project in zoltar.projects:
        print('-', project, project.id, project.name)