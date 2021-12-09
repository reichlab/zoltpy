import click

from zoltpy.covid19 import validate_quantile_csv_file


@click.command()
@click.argument('quantile_csv_file', type=click.Path(file_okay=True, exists=True))
def validate_quantile_csv_file_app(quantile_csv_file):
    """
    Simple CLI wrapper of `validate_quantile_csv_file()`

    :param csv_path: as passed to `json_io_dict_from_quantile_csv_file()`
    :return:
    """
    print(validate_quantile_csv_file(quantile_csv_file))


if __name__ == '__main__':
    validate_quantile_csv_file_app()
