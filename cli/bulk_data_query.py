import csv
import json
import logging
import pathlib

import click
import django


logging.basicConfig(level=logging.INFO,  # level=logging.ERROR,
                    format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s')

# set up django. must be done before loading models. NB: requires DJANGO_SETTINGS_MODULE to be set
django.setup()
from utils.project_queries import query_forecasts_for_project
from forecast_app.models import Project
from forecast_repo.settings.local_sqlite3 import DATABASES


logger = logging.getLogger(__name__)


@click.command()
@click.argument('sqlite_file', type=click.Path(dir_okay=False, exists=True, path_type=pathlib.Path))
@click.argument('query_file', type=click.Path(dir_okay=False, exists=True, path_type=pathlib.Path))
@click.argument('csv_file', type=click.Path(dir_okay=False, path_type=pathlib.Path))
def bulk_data_query_app(sqlite_file, query_file, csv_file):
    """
    Runs a forecast query in `query_file` against the dumped zoltar database `sqlite_file` and the saves the resulting
    CSV file to `output_dir`. Output format is as documented at https://docs.zoltardata.com/fileformats/#forecast-data-format-csv .

    :param sqlite_file: the sqlite database file as created by `bulk_data_load_app()`
    :param query_file: a JSON file that contains the query as defined at https://docs.zoltardata.com/forecastqueryformat/
    :param csv_file: where to save the CSV file to. NB: OVERWRITTEN if exists.
    """
    DATABASES['default']['NAME'] = sqlite_file

    # validate inputs
    project = Project.objects.first()  # s/be only one
    if not project:
        raise RuntimeError(f"no project in sqlite_file={sqlite_file}")

    try:
        with open(query_file) as query_file_fp:
            query = json.load(query_file_fp)
    except json.decoder.JSONDecodeError as jde:
        raise RuntimeError(f"invalid query_file: was not valid JSON. query_file={query_file}, error={jde!r}")

    # do the query
    logger.info(f"bulk_data_query_app(): running query. sqlite_file={sqlite_file}, query={query}")
    rows = query_forecasts_for_project(project, query)

    with open(csv_file, 'w') as csv_file_fp:
        logger.info(f"bulk_data_query_app(): writing query. csv_file={csv_file}")
        csv_writer = csv.writer(csv_file_fp, delimiter=",")
        csv_writer.writerows(rows)

    # done!
    logger.info(f"bulk_data_query_app(): done")


if __name__ == '__main__':
    bulk_data_query_app()
