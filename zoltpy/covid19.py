import csv
import datetime
import math
import os
from pathlib import Path

import click

import numbers

from zoltpy.quantile_io import json_io_dict_from_quantile_csv_file, summarized_error_messages, MESSAGE_FORECAST_CHECKS, \
    MESSAGE_DATE_ALIGNMENT


#
# functionality specific to the COVID19 project
#


#
# columns in addition to REQUIRED_COLUMNS
#

COVID_ADDL_REQ_COLS = ['forecast_date', 'target_end_date']


#
# FIPS codes (locations)
#

def load_fips_codes(file):
    fips_codes_state = []
    fips_codes_county = []
    with open(file) as fp:
        csv_reader = csv.reader(fp, delimiter=',')
        next(csv_reader)  # skip header
        for abbreviation, location, location_name in csv_reader:
            if abbreviation:
                fips_codes_state.append(location)
            else:
                fips_codes_county.append(location)
    return fips_codes_state, fips_codes_county


# file per https://stackoverflow.com/questions/10174211/how-to-make-an-always-relative-to-current-module-file-path
# FIPS_CODES_STATE: '01', '02', ..., 'US'
# FIPS_CODES_COUNTY: '01001', '01003', ..., '56045'
FIPS_CODES_STATE, FIPS_CODES_COUNTY = load_fips_codes(os.path.join(os.path.dirname(__file__), 'locations.csv'))

#
# targets
#

COVID_TARGETS_NON_CASE = [f"{_} day ahead inc hosp" for _ in range(131)] + \
                         [f"{_} wk ahead inc death" for _ in range(1, 21)] + \
                         [f"{_} wk ahead cum death" for _ in range(1, 21)]
COVID_TARGETS_CASE = [f"{_} wk ahead inc case" for _ in range(1, 9)]
COVID_TARGETS = COVID_TARGETS_NON_CASE + COVID_TARGETS_CASE

#
# quantiles
#

#
# these are non-overlapping, which makes the below logic simpler. case targets is complete, but the full non-case
# targets list is the combination of both, i.e.,
# NC:      0.01, 0.05,  0.15, 0.2,   0.3, 0.35, 0.4, 0.45,  0.55, 0.6, 0.65, 0.7,   0.8, 0.85,  0.95,    0.99  (16) = 23
# C:  0.025,         0.1,        0.25,                   0.5,                   0.75,        0.9,   0.975       (7)
#
COVID_QUANTILES_NON_CASE = [0.01, 0.05, 0.15, 0.2, 0.3, 0.35, 0.4, 0.45, 0.55, 0.6, 0.65, 0.7, 0.8, 0.85, 0.95, 0.99]
COVID_QUANTILES_CASE = [0.025, 0.1, 0.25, 0.5, 0.75, 0.9, 0.975]


#
# validate_quantile_csv_file()
#

def validate_quantile_csv_file(csv_fp, silent=False):
    """
    A simple wrapper of `json_io_dict_from_quantile_csv_file()` that tosses the json_io_dict and just prints validation
    error_messages.

    :param csv_fp: as passed to `json_io_dict_from_quantile_csv_file()`
    :return: error_messages: a list of strings
    """
    quantile_csv_file = Path(csv_fp)
    if not silent:
        click.echo(f"* validating quantile_csv_file '{quantile_csv_file}'...")
    with open(quantile_csv_file) as cdc_csv_fp:
        # toss json_io_dict:
        _, error_messages = json_io_dict_from_quantile_csv_file(cdc_csv_fp, COVID_TARGETS, covid19_row_validator,
                                                                COVID_ADDL_REQ_COLS)
        if error_messages:
            return summarized_error_messages(error_messages)  # summarizes and orders, converting 2-tuples to strings
        else:
            return "no errors"


#
# `json_io_dict_from_quantile_csv_file()` row validator
#

def covid19_row_validator(column_index_dict, row, is_valid_target):
    """
    Does COVID19-specific row validation. Notes:

    - expects these `valid_target_names` passed to `json_io_dict_from_quantile_csv_file()`: COVID_TARGETS_NON_CASE
    - expects these `addl_req_cols` passed to `json_io_dict_from_quantile_csv_file()`: COVID_ADDL_REQ_COLS
    """
    from zoltpy.cdc_io import _parse_date  # avoid circular imports


    error_messages = []  # return value. filled next

    location = row[column_index_dict['location']]
    target = row[column_index_dict['target']]
    is_county_location = location in FIPS_CODES_COUNTY
    is_state_location = location in FIPS_CODES_STATE
    is_case_target = target in COVID_TARGETS_CASE
    is_non_case_target = target in COVID_TARGETS_NON_CASE

    # validate location (FIPS code)
    if is_valid_target and not ((is_case_target and is_state_location) or
                                (is_case_target and is_county_location) or
                                (is_non_case_target and is_state_location)):
        error_messages.append((MESSAGE_FORECAST_CHECKS, f"invalid location for target. location={location!r}, "
                                                        f"target={target!r}. row={row}"))

    # validate quantiles. recall at this point all row values are strings, but COVID_QUANTILES_NON_CASE is numbers
    quantile = row[column_index_dict['quantile']]
    value = row[column_index_dict['value']]

    try:
        if float(value) < 0:  # value must always be non-negative regardless of row type
            error_messages.append((MESSAGE_FORECAST_CHECKS, f"entries in the `value` column must be non-negative. "
                                                            f"value='{value}'. row={row}"))
    except ValueError:
        pass  # ignore here - it will be caught by `json_io_dict_from_quantile_csv_file()`

    if row[column_index_dict['type']] == 'quantile':
        try:
            quantile_float = float(quantile)
            is_case_quantile = quantile_float in COVID_QUANTILES_CASE
            is_non_case_quantile = quantile_float in COVID_QUANTILES_CASE + COVID_QUANTILES_NON_CASE
            if is_valid_target and not ((is_case_target and is_case_quantile) or
                                        (is_non_case_target and is_case_quantile) or
                                        (is_non_case_target and is_non_case_quantile)):
                error_messages.append((MESSAGE_FORECAST_CHECKS, f"invalid quantile for target. quantile={quantile!r}, "
                                                                f"target={target!r}. row={row}"))
        except ValueError:
            pass  # ignore here - it will be caught by `json_io_dict_from_quantile_csv_file()`

    # check if point rows have empty quantile column.
    if row[column_index_dict['type']] == 'point' and quantile is not None:
        try:
            # try parsing the quantile to a number
            quantile_float = float(quantile)
            # if quantile is not finite
            if math.isfinite(quantile_float):
                # if it successfully parsed the value, throw an error
                error_messages.append((MESSAGE_FORECAST_CHECKS,
                                       f"entries in the `quantile` column must be empty for `point` "
                                       f"entries. Current value is: "
                                       f"{quantile_float}. row={row}"))
        except ValueError:
            # if parsing the quantile fails, do nothing as it is what we expect
            pass


    # validate forecast_date and target_end_date date formats
    forecast_date = row[column_index_dict['forecast_date']]
    target_end_date = row[column_index_dict['target_end_date']]
    forecast_date = _parse_date(forecast_date)  # None if invalid format
    target_end_date = _parse_date(target_end_date)  # ""
    if not forecast_date or not target_end_date:
        error_messages.append((MESSAGE_FORECAST_CHECKS,
                               f"invalid forecast_date or target_end_date format. forecast_date={forecast_date!r}. "
                               f"target_end_date={target_end_date}. row={row}"))
        return error_messages  # terminate - remaining validation depends on valid dates

    # formats are valid. next: validate "__ day ahead" or "__ week ahead" increment - must be an int
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
        error_messages.append((MESSAGE_FORECAST_CHECKS, f"non-integer 'ahead' number in target: {target!r}. row={row}"))
        return error_messages  # terminate - remaining validation depends on valid step_ahead_increment

    # validate date alignment
    # 1/4) for x day ahead targets the target_end_date should be forecast_date + x
    if 'day ahead' in target:
        if (target_end_date - forecast_date).days != step_ahead_increment:
            error_messages.append((MESSAGE_FORECAST_CHECKS,
                                   f"invalid target_end_date: was not {step_ahead_increment} day(s) after "
                                   f"forecast_date. diff={(target_end_date - forecast_date).days}, "
                                   f"forecast_date={forecast_date}, target_end_date={target_end_date}. row={row}"))
    else:  # 'wk ahead' in target
        # NB: we convert `weekdays()` (Monday is 0 and Sunday is 6) to a Sunday-based numbering to get the math to work:
        weekday_to_sun_based = {i: i + 2 if i != 6 else 1 for i in range(7)}  # Sun=1, Mon=2, ..., Sat=7
        # 2/4) for x week ahead targets, weekday(target_end_date) should be a Sat
        if weekday_to_sun_based[target_end_date.weekday()] != 7:  # Sat
            error_messages.append((MESSAGE_DATE_ALIGNMENT, f"target_end_date was not a Saturday: {target_end_date}. "
                                                           f"row={row}"))
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
            error_messages.append((MESSAGE_DATE_ALIGNMENT,
                                   f"target_end_date was not the expected Saturday. forecast_date={forecast_date}, "
                                   f"target_end_date={target_end_date}. exp_target_end_date={exp_target_end_date}, "
                                   f"row={row}"))

    # done!
    return error_messages
