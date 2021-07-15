import pathlib

import click
import django


# set up django. must be done before loading models. NB: requires DJANGO_SETTINGS_MODULE to be set
django.setup()

from forecast_repo.settings.local_sqlite3 import DATABASES

from utils.project_truth import truth_data_qs, oracle_model_for_project
from forecast_app.models import Project


@click.command()
@click.argument('sqlite_file', type=click.Path(dir_okay=False, exists=True, path_type=pathlib.Path))
@click.argument('verbosity', type=click.Choice(['1', '2', '3', '4']), default='1')
def main(sqlite_file, verbosity):
    """
    :param sqlite_file: the sqlite database file to print info from. as created by `bulk_data_load_app()`
    :param verbosity: increasing from 1 (minimal verbosity) to 3 (maximal)
    """
    DATABASES['default']['NAME'] = sqlite_file

    projects = Project.objects.order_by('name')
    if len(projects) != 0:
        click.echo(f"Found {len(projects)} projects: {projects}")
        for project in projects:
            print_project_info(project, int(verbosity))
    else:
        click.echo("<No Projects>")


def print_project_info(project, verbosity):
    # verbosity == 1
    oracle_model = oracle_model_for_project(project)
    first_truth_forecast = oracle_model.forecasts.first() if oracle_model else None
    click.echo(f"\n\n* {project}. truth: # predictions={truth_data_qs(project).count()}, "
               f"source={repr(first_truth_forecast.source) if first_truth_forecast else '<no truth>'}, "
               f"created_at={first_truth_forecast.created_at if first_truth_forecast else '<no truth>'}. "
               f"(num_models, num_forecasts): {project.num_models_forecasts()}")
    if verbosity == 1:
        return

    # verbosity == 2
    click.echo(f"\n** Targets ({project.targets.count()})")
    for target in project.targets.all():
        click.echo(f"- {target}")

    click.echo(f"\n** Units ({project.units.count()})")
    for unit in project.units.all().order_by('name'):
        click.echo(f"- {unit}")

    click.echo(f"\n** TimeZeros ({project.timezeros.count()})")
    for timezero in project.timezeros.all():
        click.echo(f"- {timezero}")

    if verbosity == 2:
        return

    # verbosity == 3
    click.echo(f"\n** ForecastModels ({project.models.count()})")
    for forecast_model in project.models.all():
        if verbosity == 3:
            click.echo(f"- {forecast_model}")
        else:
            click.echo(f"*** {forecast_model} ({forecast_model.forecasts.count()} forecasts)")
        if verbosity == 4:
            for forecast in forecast_model.forecasts.order_by('time_zero', 'issued_at'):
                click.echo(f"- {forecast}: {forecast.pred_eles.count()} rows")


if __name__ == '__main__':
    main()
