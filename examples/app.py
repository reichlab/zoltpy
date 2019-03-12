import os
import sys
import time

from zoltpy.client import ZoltarClient


def main_app():
    """
    App demonstrating use of the library. passed one arg: forecast_csv_file: the cdc.csv data file to load.
    """
    forecast_csv_file = sys.argv[1]

    client = ZoltarClient()
    client.authenticate(os.environ.get('USERNAME'), os.environ.get('PASSWORD'))

    print('* projects')
    for project in client.projects:
        print('-', project, project.id, project.name)

    project = [project for project in client.projects if project.name == 'public project'][0]
    print('* models in', project)
    for model in project.models:
        print('-', model)

    # for a particular TimeZero, delete existing Forecast, if any
    model = [model for model in project.models if model.name == 'Test ForecastModel1'][0]
    print('* working with', model)
    print('* pre-delete forecasts', model.forecasts)
    timezero_date = '20170117'  # YYYYMMDD_DATE_FORMAT
    forecast_for_tz_date = [forecast for forecast in model.forecasts if forecast.timezero_date == timezero_date]
    if forecast_for_tz_date:
        existing_forecast = forecast_for_tz_date[0]
        print('- deleting existing forecast', timezero_date, existing_forecast)
        existing_forecast.delete()
    else:
        print('- no existing forecast', timezero_date)

    model.refresh()  # o/w model.forecasts errors b/c the just-deleted forecast is still cached in model
    print('* post-delete forecasts', model.forecasts)

    # upload a new forecast
    upload_file_job = model.upload_forecast(timezero_date, forecast_csv_file)
    busy_poll_upload_file_job(upload_file_job)

    # get the new forecast from the upload_file_job by parsing the generic 'output_json' field
    new_forecast_pk = upload_file_job.output_json['forecast_pk']
    new_forecast = model.forecast_for_pk(new_forecast_pk)
    print('* new_forecast', new_forecast)

    model.refresh()
    print('* post-upload forecasts', model.forecasts)

    # GET its data (default format is JSON)
    print('* data for forecast', new_forecast)
    data_json = new_forecast.data(is_json=True)
    data_csv = new_forecast.data(is_json=False)
    print('- data_json', data_json)
    print('- data_csv', data_csv)


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
    main_app()
