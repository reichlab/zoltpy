'''
  ______      _ _
 |___  /     | | |
    / /  ___ | | |_ _ __  _   _
   / /  / _ \| | __| '_ \| | | |
  / /__| (_) | | |_| |_) | |_| |
 /_____|\___/|_|\__| .__/ \__, |
                   | |     __/ |
                   |_|    |___/

'''
import csv
import io
import os

from examples.app import busy_poll_upload_file_job
from zoltpy.cdc import cdc_csv_rows_from_json_io_dict
from zoltpy.connection import ZoltarConnection


def authenticate(env_user='Z_USERNAME', env_pass='Z_PASSWORD'):
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
        connection = ZoltarConnection()
        connection.authenticate(os.environ.get(env_user), os.environ.get(env_pass))
        return connection
    except:
        print("ERROR: Cannot authenticate zoltar credentials")
        print("Ensure the environment variables for your username and password are correct")


def delete_forecast(project_name, model_name, timezero_date):
    # for a particular TimeZero, delete existing Forecast, if any
    conn = authenticate()
    project = [project for project in conn.projects if project.name == project_name][0]
    model = [model for model in project.models if model.name == model_name][0]
    print('* working with', model)
    print('* pre-delete forecasts', model.forecasts)
    forecast_for_tz_date = [forecast for forecast in model.forecasts if forecast.timezero_date == timezero_date]
    if forecast_for_tz_date:
        existing_forecast = forecast_for_tz_date[0]
        print('- deleting existing forecast')
        existing_forecast.delete()
    else:
        print('- no existing forecast')

    model.refresh()  # o/w model.forecasts errors b/c the just-deleted forecast is still cached in model
    print('* post-delete forecasts')


# todo xx needs to create intermediate json file as is done by zoltar_connection_app()! also, should take a fp, and not open() it
def upload_forecast(forecast_csv_file, project_name, model_name, timezero_date, data_version_date=None):
    # timezero_date = '20181203'  # YYYYMMDD_DATE_FORMAT
    conn = authenticate()
    project = [project for project in conn.projects if project.name == project_name][0]
    model = [model for model in project.models if model.name == model_name][0]
    print('* working with', model)

    # upload a new forecast
    upload_file_job = model.upload_forecast(forecast_csv_file, timezero_date, data_version_date)
    busy_poll_upload_file_job(upload_file_job)

    # get the new forecast from the upload_file_job by parsing the generic 'output_json' field
    new_forecast_pk = upload_file_job.output_json['forecast_pk']
    new_forecast = model.forecast_for_pk(new_forecast_pk)
    print('* new_forecast', new_forecast)

    model.refresh()


def forecast_to_dataframe(project_name, model_name, timezero_date):
    import pandas as pd


    conn = authenticate()
    project = [project for project in conn.projects if project.name == project_name][0]
    model = [model for model in project.models if model.name == model_name][0]
    forecast_for_tz_date = [forecast for forecast in model.forecasts if forecast.timezero_date == timezero_date]
    existing_forecast = forecast_for_tz_date[0] if forecast_for_tz_date else None

    # convert native json to cdc csv
    string_io = io.StringIO()
    csv_writer = csv.writer(string_io, delimiter=',')
    data_json = existing_forecast.data()
    csv_rows = cdc_csv_rows_from_json_io_dict(data_json)
    for row in csv_rows:
        csv_writer.writerow(row)
    string_io.seek(0)
    dataset = pd.read_csv(string_io, delimiter=",")
    return dataset


if __name__ == '__main__':
    authenticate()
