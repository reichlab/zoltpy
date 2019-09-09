import csv
import json
import os
import sys
import tempfile
from pathlib import Path

from zoltpy.cdc import cdc_csv_rows_from_json_io_dict, CDC_CSV_FILENAME_EXTENSION
from zoltpy.connection import ZoltarConnection
from zoltpy.csv_util import csv_rows_from_json_io_dict
from zoltpy.util import delete_forecast, busy_poll_upload_file_job, upload_forecast, download_forecast, \
    dataframe_from_json_io_dict


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
    forecast_csv_file = sys.argv[5]  # CDC CSV format

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
    delete_forecast(conn, project_name, model_name, timezero_date)
    model.refresh()  # o/w model.forecasts errors b/c the just-deleted forecast is still cached in model
    print(f'\n* post-delete forecasts: {model.forecasts}')

    # upload a new forecast and wait for success
    upload_file_job = upload_forecast(conn, forecast_csv_file, project_name, model_name, timezero_date)
    busy_poll_upload_file_job(upload_file_job)

    # get the new forecast from the upload_file_job by parsing the generic 'output_json' field
    new_forecast_pk = upload_file_job.output_json['forecast_pk']
    new_forecast = model.forecast_for_pk(new_forecast_pk)
    print(f'\n* new_forecast: {new_forecast}')

    model.refresh()
    print(f'\n* post-upload forecasts: {model.forecasts}')

    # download the just-uploaded forecast data as native json
    data_json = download_forecast(conn, project_name, model_name, timezero_date)
    print(f'\n* data:')
    print(f'- json: #predictions={len(data_json["predictions"])}')
    with open(Path(tempfile.gettempdir()) / (str(new_forecast_pk) + '.json'), 'w') as fp:
        print(f'- writing json data to {fp.name}')
        json.dump(data_json, fp, indent=4)

        # convert native json to cdc csv
    csv_rows = cdc_csv_rows_from_json_io_dict(data_json)
    print(f'- cdc csv rows: #rows={len(csv_rows)}')
    with open(Path(tempfile.gettempdir()) / (str(new_forecast_pk) + '.' + CDC_CSV_FILENAME_EXTENSION), 'w') as fp:
        print(f'- writing cdc csv data to {fp.name}')
        csv_writer = csv.writer(fp, delimiter=',')
        for row in csv_rows:
            csv_writer.writerow(row)

    # convert native json to zoltar2 csv
    csv_rows = csv_rows_from_json_io_dict(data_json)
    print(f'- zoltar2 csv rows: #rows={len(csv_rows)}')
    with open(Path(tempfile.gettempdir()) / (str(new_forecast_pk) + '.csv'), 'w') as fp:
        print(f'- writing zoltar2 csv data to {fp.name}')
        csv_writer = csv.writer(fp, delimiter=',')
        for row in csv_rows:
            csv_writer.writerow(row)

    # convert to a Pandas DataFrame
    dataframe = dataframe_from_json_io_dict(data_json)
    print(f'- pandas zoltar2 csv:\n{dataframe}')

    dataframe = dataframe_from_json_io_dict(data_json, is_cdc_format=True)
    print(f'- pandas cdc csv csv:\n{dataframe}')


if __name__ == '__main__':
    zoltar_connection_app()
