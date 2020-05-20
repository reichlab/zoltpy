import datetime
from pathlib import Path

import click

from zoltpy.quantile_io import json_io_dict_from_quantile_csv_file, summarized_error_messages


#
# functions specific to the COVID19 project
#

# from https://github.com/reichlab/covid19-forecast-hub/blob/master/template/state_fips_codes.csv
# (probably via https://en.wikipedia.org/wiki/Federal_Information_Processing_Standard_state_code ):
FIPS_STATE_CODES = ['01', '02', '04', '05', '06', '08', '09', '10', '11', '12', '13', '15', '16', '17', '18',
                    '19', '20', '21', '22', '23', '24', '25', '26', '27', '28', '29', '30', '31', '32', '33',
                    '34', '35', '36', '37', '38', '39', '40', '41', '42', '44', '45', '46', '47', '48', '49',
                    '50', '51', '53', '54', '55', '56', '60', '66', '69', '72', '74', '78', 'US']  # 'US' is extra

# b/c there are so many possible targets, we generate using a range
VALID_TARGET_NAMES = [f"{_} day ahead inc death" for _ in range(131)] + \
                     [f"{_} day ahead cum death" for _ in range(131)] + \
                     [f"{_} wk ahead inc death" for _ in range(1, 21)] + \
                     [f"{_} wk ahead cum death" for _ in range(1, 21)] + \
                     [f"{_} day ahead inc hosp" for _ in range(131)]

VALID_QUANTILES = [0.010, 0.025, 0.050, 0.100, 0.150, 0.200, 0.250, 0.300, 0.350, 0.400, 0.450, 0.500, 0.550, 0.600,
                   0.650, 0.700, 0.750, 0.800, 0.850, 0.900, 0.950, 0.975, 0.990]  # incoming must be a subset of these


#
# validate_quantile_csv_file()
#

def validate_quantile_csv_file(csv_fp):
    """
    A simple wrapper of `json_io_dict_from_quantile_csv_file()` that tosses the json_io_dict and just prints validation
    error_messages.

    :param csv_fp: as passed to `json_io_dict_from_quantile_csv_file()`
    :return: error_messages: a list of strings
    """
    quantile_csv_file = Path(csv_fp)
    click.echo(f"* validating quantile_csv_file '{quantile_csv_file}'...")
    with open(quantile_csv_file) as cdc_csv_fp:
        # toss json_io_dict:
        _, error_messages = json_io_dict_from_quantile_csv_file(cdc_csv_fp, VALID_TARGET_NAMES, covid19_row_validator,
                                                                ['forecast_date', 'target_end_date'])
        if error_messages:
            return summarized_error_messages(error_messages)
        else:
            return "no errors"


#
# `json_io_dict_from_quantile_csv_file()` row validator
#

def covid19_row_validator(column_index_dict, row):
    """
    Does COVID19-specific row validation. Notes:

    - expects these `valid_target_names` passed to `json_io_dict_from_quantile_csv_file()`: VALID_TARGET_NAMES
    - expects these `addl_req_cols` passed to `json_io_dict_from_quantile_csv_file()`: ['forecast_date', 'target_end_date']
    """
    from zoltpy.cdc_io import _parse_date  # avoid circular imports


    error_messages = []  # returned value. filled next

    # validate location (FIPS code)
    location = row[column_index_dict['location']]
    if location not in FIPS_STATE_CODES:
        error_messages.append(f"invalid FIPS location: {location!r}. row={row}")

    # validate quantiles. recall at this point all row values are strings, but VALID_QUANTILES is numbers
    quantile = row[column_index_dict['quantile']]
    value = row[column_index_dict['value']]
    if row[column_index_dict['type']] == 'quantile':
        try:
            if float(quantile) not in VALID_QUANTILES:
                error_messages.append(f"invalid quantile: {quantile!r}. row={row}")
        except ValueError:
            pass  # ignore here - it will be caught by `json_io_dict_from_quantile_csv_file()`
        try:
            if float(value) < 0:
                error_messages.append(f"invalid quantile value: was not >= 0. value='{value}'. row={row}")
        except ValueError:
            pass  # ignore here - it will be caught by `json_io_dict_from_quantile_csv_file()`

    # validate forecast_date and target_end_date date formats
    forecast_date = row[column_index_dict['forecast_date']]
    target_end_date = row[column_index_dict['target_end_date']]
    forecast_date = _parse_date(forecast_date)  # None if invalid format
    target_end_date = _parse_date(target_end_date)  # ""
    if not forecast_date or not target_end_date:
        error_messages.append(f"invalid forecast_date or target_end_date format. forecast_date={forecast_date!r}. "
                              f"target_end_date={target_end_date}. row={row}")
        return error_messages  # terminate - remaining validation depends on valid dates

    # formats are valid. next: validate "__ day ahead" or "__ week ahead" increment - must be an int
    target = row[column_index_dict['target']]
    target_day_ahead_split = target.split('day ahead')
    target_week_ahead_split = target.split('wk ahead')
    is_day_or_week_ahead_target = (len(target_day_ahead_split) == 2) or (len(target_week_ahead_split) == 2)
    try:
        if is_day_or_week_ahead_target:  # valid day or week ahead target
            step_ahead_increment = int(target_day_ahead_split[0].strip()) if len(target_day_ahead_split) == 2 \
                else int(target_week_ahead_split[0].strip())
        else:  # invalid target. don't add error message b/c caught by caller `_validated_rows_for_quantile_csv()`
            return error_messages  # terminate - remaining validation depends on valid step_ahead_increment
    except ValueError:
        error_messages.append(f"non-integer 'ahead' number in target: {target!r}. row={row}")
        return error_messages  # terminate - remaining validation depends on valid step_ahead_increment

    # validate date alignment
    # 1/4) for x day ahead targets the target_end_date should be forecast_date + x
    if 'day ahead' in target:
        if (target_end_date - forecast_date).days != step_ahead_increment:
            error_messages.append(f"invalid target_end_date: was not {step_ahead_increment} day(s) after "
                                  f"forecast_date. diff={(target_end_date - forecast_date).days}, "
                                  f"forecast_date={forecast_date}, target_end_date={target_end_date}. row={row}")
    else:  # 'wk ahead' in target
        # NB: we convert `weekdays()` (Monday is 0 and Sunday is 6) to a Sunday-based numbering to get the math to work:
        weekday_to_sun_based = {i: i + 2 if i != 6 else 1 for i in range(7)}  # Sun=1, Mon=2, ..., Sat=7
        # 2/4) for x week ahead targets, weekday(target_end_date) should be a Sat
        if weekday_to_sun_based[target_end_date.weekday()] != 7:  # Sat
            error_messages.append(f"target_end_date was not a Saturday: {target_end_date}. row={row}")
            return error_messages  # terminate - remaining validation depends on valid target_end_date

        # set exp_target_end_date and then validate it
        weekday_diff = datetime.timedelta(days=(abs(weekday_to_sun_based[target_end_date.weekday()] -
                                                    weekday_to_sun_based[forecast_date.weekday()])))
        if weekday_to_sun_based[forecast_date.weekday()] <= 2:  # Sun or Mon
            # 3/4) (Sun or Mon) for x week ahead targets, ensure that the 1-week ahead forecast is for the next Sat
            delta_days = weekday_diff + datetime.timedelta(days=(7 * (step_ahead_increment - 1)))
            exp_target_end_date = forecast_date + delta_days
        else:  # Tue through Sat
            # 4/4) (Tue on) for x week ahead targets, ensures that the 1-week ahead forecast is for the Sat after next
            delta_days = weekday_diff + datetime.timedelta(days=(7 * step_ahead_increment))
            exp_target_end_date = forecast_date + delta_days
        if target_end_date != exp_target_end_date:
            error_messages.append(f"target_end_date was not the expected Saturday. forecast_date={forecast_date}, "
                                  f"target_end_date={target_end_date}. exp_target_end_date={exp_target_end_date}, "
                                  f"row={row}")

    # done!
    return error_messages
