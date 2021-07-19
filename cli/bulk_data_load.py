import csv
import logging
import pathlib
import shutil
import sqlite3
import tempfile
from pathlib import Path

import click


logging.basicConfig(level=logging.INFO,  # level=logging.ERROR,
                    format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s')

logger = logging.getLogger(__name__)

# maps csv_file_name -> 2-tuple: (table_name, columns)
CSV_FILE_NAME_TO_TABLE_NAME_COLUMNS = {
    'project.csv': (
        'forecast_app_project',
        ['id', 'core_data', 'description', 'home_url', 'is_public', 'logo_url', 'name', 'time_interval_type',
         'visualization_y_label']),
    'unit.csv': (
        'forecast_app_unit', ['id', 'project_id', 'name']),
    'target.csv': (
        'forecast_app_target',
        ['id', 'project_id', 'description', 'is_step_ahead', 'name', 'step_ahead_increment', 'type', 'unit']),
    'timezero.csv': (
        'forecast_app_timezero',
        ['id', 'project_id', 'data_version_date', 'is_season_start', 'season_name', 'timezero_date']),
    'forecastmodel.csv': (
        'forecast_app_forecastmodel',
        ['id', 'project_id', 'abbreviation', 'aux_data_url', 'citation', 'contributors', 'description', 'home_url',
         'is_oracle', 'license', 'methods', 'name', 'notes', 'team_name']),
    'forecast.csv': (
        'forecast_app_forecast',
        ['id', 'forecast_model_id', 'time_zero_id', 'created_at', 'issued_at', 'notes', 'source']),
    'predictionelement.csv': (
        'forecast_app_predictionelement',
        ['id', 'forecast_id', 'target_id', 'unit_id', 'data_hash', 'is_retract', 'pred_class']),
    'predictiondata.csv': ('forecast_app_predictiondata', ['pred_ele_id', 'data']),
}


@click.command()
@click.argument('zip_file', type=click.Path(dir_okay=False, exists=True, path_type=pathlib.Path))
@click.argument('sqlite_file', type=click.Path(dir_okay=False, path_type=pathlib.Path))
def bulk_data_load_app(zip_file, sqlite_file):
    """
    A prototype bulk database dump loader utility that's the twin of the Zoltar server's `bulk_data_dump_app()`. Creates
    an sqlite database file and then loads the data found in the eight csv files in the passed zip file. These files are
    as created by that dump program.

    :param zip_file: the zip file as created by the dump program
    :param sqlite_file: sqlite file to save to. must not exist
    """
    # validate inputs
    if sqlite_file.exists():
        raise RuntimeError(f"sqlite_file already exists. please delete and re-run. sqlite_file={sqlite_file}")

    # create the database file, load the schema file, and then load the data files
    logger.info(f"creating sqlite database file")
    schema_file = Path('cli/bulk_data_load_schema.sql')  # todo hard-coded. use same dir as this source file
    with sqlite3.connect(sqlite_file) as connection, \
            open(schema_file) as schema_fp, \
            tempfile.TemporaryDirectory() as temp_csv_dir:
        logger.info(f"loading the schema file. schema_fp={schema_fp}")
        connection.executescript(schema_fp.read())

        logger.info(f"loading data files. zip_file={zip_file}")
        shutil.unpack_archive(zip_file, temp_csv_dir)
        for csv_file_name, (table_name, columns) in CSV_FILE_NAME_TO_TABLE_NAME_COLUMNS.items():
            logger.info(f"loading csv file: {csv_file_name}")
            with open(Path(temp_csv_dir) / csv_file_name) as csv_fp:
                csv_reader = csv.reader(csv_fp, delimiter=',')
                column_names = (', '.join(columns))
                values_percent_qmark = ', '.join(['?'] * len(columns))
                connection.executemany(f"INSERT INTO {table_name}({column_names}) VALUES ({values_percent_qmark})",
                                       csv_reader)
    logger.info(f"done. sqlite_file={sqlite_file}")


if __name__ == '__main__':
    bulk_data_load_app()
