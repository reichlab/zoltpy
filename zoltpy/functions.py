import os
import sys
import time
from zoltpy.client import ZoltarClient


def authenticate(env_user = 'Z_USERNAME', env_pass = 'Z_PASSWORD'):
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
        client = ZoltarClient()
        client.authenticate(os.environ.get(env_user), os.environ.get(env_pass))
    except:
        print("ERROR: Cannot authenticate zoltar credentials")
        print("Ensure the environment variables for your username and password are correct")
    return client

def print_projects():
    print('* projects')
    zoltar = authenticate()
    for project in zoltar.projects:
        print('-', project, project.id, project.name)

def print_models(project_name):
    zoltar = authenticate()
    project = [project for project in zoltar.projects if project.name == project_name][0]
    print('* models in', project)
    for model in project.models:
        print('-', model)


''' 
#
# Work in progress
#

def delete_forecast(model_name, timezero_date):
    # for a particular TimeZero, delete existing Forecast, if any
    model = [model for model in project.models if model.name == model_name][0]
    print('* working with', model)
    print('* pre-delete forecasts', model.forecasts)
    forecast_for_tz_date = [forecast for forecast in model.forecasts if forecast.timezero_date == timezero_date]
    if forecast_for_tz_date:
        existing_forecast = forecast_for_tz_date[0]
        print('- deleting existing forecast', timezero_date, existing_forecast)
        existing_forecast.delete()
    else:
        print('- no existing forecast', timezero_date)

    model.refresh()  # o/w model.forecasts errors b/c the just-deleted forecast is still cached in model
    print('* post-delete forecasts', model.forecasts)

def upload_forecast(model_name, timezero_date, forecast_csv_file):
    model = [model for model in project.models if model.name == model_name][0]
    print('* working with', model)

    # upload a new forecast
    upload_file_job = model.upload_forecast(timezero_date, forecast_csv_file)
    busy_poll_upload_file_job(upload_file_job)

    # get the new forecast from the upload_file_job by parsing the generic 'output_json' field
    new_forecast_pk = upload_file_job.output_json['forecast_pk']
    new_forecast = model.forecast_for_pk(new_forecast_pk)
    print('* new_forecast', new_forecast)

    model.refresh()
    print('* post-upload forecasts', model.forecasts)


def main_app(model_name, timezero_date):
    model = [model for model in project.models if model.name == model_name][0]
    forecast = model.forecast_for_pk(new_forecast_pk)
    data_json = new_forecast.data(is_json=True)
    data_csv = new_forecast.data(is_json=False)
    print('- data_json', data_json)
    print('- data_csv', data_csv)
'''

def busy_poll_upload_file_job(upload_file_job):
    # get the updated status via polling (busy wait every 1 second)
    print('- polling for status change. upload_file_job:', upload_file_job)
    while True:
        status = upload_file_job.status_as_str
        print('  =', status)
        if status == 'FAILED':
            print('  x failed')
            break
        if status == 'SUCCESS':
            break
        time.sleep(1)
        upload_file_job.refresh()


if __name__ == '__main__':
    authenticate()