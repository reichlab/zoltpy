from pathlib import Path

import click

from zoltpy.quantile import json_io_dict_from_quantile_csv_file


@click.command()
@click.argument('quantile_csv_file', type=click.Path(file_okay=True, exists=True))
def validate_quantile_csv_file_app(quantile_csv_file):
    """
    App to validate files in the quantile CSV format specified in `json_io_dict_from_quantile_csv_file()`. Prints a
    report.
    """
    quantile_csv_file = Path(quantile_csv_file)
    click.echo(f"* validating quantile_csv_file={quantile_csv_file}...")
    with open(quantile_csv_file) as cdc_csv_fp:
        _, error_messages = json_io_dict_from_quantile_csv_file(cdc_csv_fp)  # toss json_io_dict
        if error_messages:
            click.echo(f"found {len(error_messages)} errors:")
            for error_message in error_messages:
                click.echo(error_message, err=True)
        else:
            click.echo(f"no errors")
    click.echo(f"validating done. quantile_csv_file={quantile_csv_file}")


if __name__ == '__main__':
    validate_quantile_csv_file_app()
