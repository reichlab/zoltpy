import csv
import json
import os
import sys
import tempfile
import time
from pathlib import Path

from zoltpy.cdc import cdc_csv_rows_from_json_io_dict, CDC_CSV_FILENAME_EXTENSION, json_io_dict_from_cdc_csv_file
from zoltpy.connection import ZoltarConnection
from zoltpy.csv_util import csv_rows_from_json_io_dict


def zoltar_connection_app():
    """
    Application demonstrating use of the library at the ZoltarConnection level (rather than using the package's
    higher-level functions such as delete_forecast(), etc.)

    App args:
    - zoltar_host: host to pass to ZoltarConnection(). should *not* have a trailing '/'
    - project_name: name of Project to work with. assumptions: must have a model named in below arg, and must have a
      timezero_date named in below arg. must be a CDC project (locations, targets, forecasts, etc.)
    - model_name: name of a ForecastModel to work with - upload files, etc.
    - timezero_date: in YYYYMMDD format, e.g., '20181203'
    - forecast_csv_file: the cdc.csv data file to load

    Required environment variables:
    - 'USERNAME': username of the account that has permission to access the resources in above app args
    - 'PASSWORD': password ""
    """
    host = sys.argv[1]
    project_name = sys.argv[2]
    model_name = sys.argv[3]
    timezero_date = sys.argv[4]
    forecast_csv_file = sys.argv[5]

    conn = ZoltarConnection(host)
    conn.authenticate(os.environ.get('USERNAME'), os.environ.get('PASSWORD'))

    print('\n* projects')
    for project in conn.projects:
        print(f'- {project}, {project.id}, {project.name}')

    project = [project for project in conn.projects if project.name == project_name][0]
    print(f'\n* models in {project}')
    for model in project.models:
        print(f'- {model}')

    # for a particular TimeZero, delete existing Forecast, if any
    model = [model for model in project.models if model.name == model_name][0]
    print(f'\n* working with {model}')
    print(f'\n* pre-delete forecasts: {model.forecasts}')
    forecast_for_tz_date = [forecast for forecast in model.forecasts if forecast.timezero_date == timezero_date]
    if forecast_for_tz_date:
        existing_forecast = forecast_for_tz_date[0]
        print(f'- deleting existing forecast. timezero_date={timezero_date}, existing_forecast={existing_forecast}')
        existing_forecast.delete()
    else:
        print(f'- no existing forecast. timezero_date={timezero_date}')

    model.refresh()  # o/w model.forecasts errors b/c the just-deleted forecast is still cached in model
    print(f'\n* post-delete forecasts: {model.forecasts}')

    # upload a new forecast. note that this app accepts a *.cdc.csv file, but zoltar requires a native json file. so
    # we first convert to a temp json file and then pass it
    with tempfile.TemporaryFile('r+') as json_fp, \
            open(forecast_csv_file, 'r') as csv_fp:
        json_io_dict = json_io_dict_from_cdc_csv_file(csv_fp)
        json.dump(json_io_dict, json_fp)
        json_fp.seek(0)
        upload_file_job = model.upload_forecast(json_fp, timezero_date, timezero_date)  # 2nd date: data_version_date
        busy_poll_upload_file_job(upload_file_job)

    # get the new forecast from the upload_file_job by parsing the generic 'output_json' field
    new_forecast_pk = upload_file_job.output_json['forecast_pk']
    new_forecast = model.forecast_for_pk(new_forecast_pk)
    print(f'\n* new_forecast: {new_forecast}')

    model.refresh()
    print(f'\n* post-upload forecasts: {model.forecasts}')

    # download the just-uploaded forecast data as native json
    data_json = new_forecast.data()
    print(f'\n* data:')
    print(f'- json: #predictions={len(data_json["predictions"])}')
    with open(Path(tempfile.gettempdir()) / (str(new_forecast_pk) + '.json'), 'w') as fp:
        print(f'- writing json data to {fp.name}')
        json.dump(data_json, fp, indent=4)

    # convert native json to cdc csv
    csv_rows = cdc_csv_rows_from_json_io_dict(data_json)
    print(f'- cdc csv rows: #rows = {len(csv_rows)}')
    with open(Path(tempfile.gettempdir()) / (str(new_forecast_pk) + '.' + CDC_CSV_FILENAME_EXTENSION), 'w') as fp:
        print(f'- writing cdc csv data to {fp.name}')
        csv_writer = csv.writer(fp, delimiter=',')
        for row in csv_rows:
            csv_writer.writerow(row)

    # convert native json to zoltar2 csv
    csv_rows = csv_rows_from_json_io_dict(data_json)
    print(f'- zoltar2 csv rows: #rows = {len(csv_rows)}')
    with open(Path(tempfile.gettempdir()) / (str(new_forecast_pk) + '.csv'), 'w') as fp:
        print(f'- writing zoltar2 csv data to {fp.name}')
        csv_writer = csv.writer(fp, delimiter=',')
        for row in csv_rows:
            csv_writer.writerow(row)


def busy_poll_upload_file_job(upload_file_job):
    # get the updated status via polling (busy wait every 1 second)
    print(f'- polling for status change. upload_file_job: {upload_file_job}')
    while True:
        status = upload_file_job.status_as_str
        print(f'  ={status}')
        if status == 'FAILED':
            print('  x failed')
            break
        if status == 'SUCCESS':
            break
        time.sleep(1)
        upload_file_job.refresh()


if __name__ == '__main__':
    zoltar_connection_app()
