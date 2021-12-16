import datetime
import math
import numbers
from pathlib import Path

import click

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
# validate_quantile_csv_file()
#

def validate_quantile_csv_file(csv_path, validation_config, silent=False):
    """
    A simple wrapper of `json_io_dict_from_quantile_csv_file()` that tosses the json_io_dict and just prints validation
    error_messages.

    :param csv_path: as passed to `json_io_dict_from_quantile_csv_file()`
    :param validation_config: a dict with validation information as documented at __ <- todo url for doc. summary:
        the only key we use is "target_groups", which is a list of target_group objects, each of which has these fields:
        - 'name': target group name
        - 'targets': a list of specific target names (strings) in the group
        - 'locations': a list of valid locations (strings) for the group
        - 'quantiles': a list of valid quantiles (numbers) for the group
    :param silent: True if should echo starting message
    :return: error_messages: a list of strings
    """
    try:
        validate_config_dict(validation_config)
    except RuntimeError as rte:
        return [f"invalid validation_config. rte={rte}"]

    quantile_csv_file = Path(csv_path)
    if not silent:
        click.echo(f"* validating quantile_csv_file '{quantile_csv_file}'...")
    with open(quantile_csv_file) as cdc_csv_fp:
        # toss json_io_dict:
        _, error_messages = json_io_dict_from_quantile_csv_file(cdc_csv_fp, validation_config, hub_row_validator,
                                                                hub_quantile_prediction_dict_validator,
                                                                COVID_ADDL_REQ_COLS)
        if error_messages:
            return summarized_error_messages(error_messages)  # summarizes and orders, converting 2-tuples to strings
        else:
            return "no errors"


#
# validate_config_dict()
#

def validate_config_dict(validation_config):
    """
    Validates validation_config
    :param validation_config: as passed to validate_quantile_csv_file()
    :raises: RuntimeError if validation_config is invalid
    """
    if not isinstance(validation_config, dict):
        raise RuntimeError(f"validation_config was not a dict: {validation_config}, type={type(validation_config)}")
    elif 'target_groups' not in validation_config:
        raise RuntimeError(f"validation_config did not contain 'target_groups' key: {validation_config}")
    elif not isinstance(validation_config['target_groups'], list):
        raise RuntimeError(f"'target_groups' was not a list: {validation_config['target_groups']}")

    # validate each target_group
    for target_group in validation_config['target_groups']:
        expected_keys = ['outcome_variable', 'targets', 'locations', 'quantiles']
        actual_keys = list(target_group.keys())
        if actual_keys != expected_keys:
            raise RuntimeError(f"one or more target group keys was missing. expected keys={expected_keys}, "
                               f"actual keys={actual_keys}")
        elif (not isinstance(target_group['targets'], list)) \
                or (not isinstance(target_group['locations'], list)) \
                or (not isinstance(target_group['quantiles'], list)):
            raise RuntimeError(f"one of these fields was not a list: 'targets', 'locations', or 'quantiles'. "
                               f"target_group={target_group}")
        elif not isinstance(target_group['outcome_variable'], str):
            raise RuntimeError(f"'outcome_variable' field was not a string: {target_group['outcome_variable']!r}")
        elif (not all([isinstance(target, str) for target in target_group['targets']])) or \
                (not all([isinstance(target, str) for target in target_group['locations']])):
            raise RuntimeError(f"one of these fields contained non-strings: 'targets' or 'locations'"
                               f"target_group={target_group}")
        elif not all([isinstance(quantile, numbers.Number) for quantile in target_group['quantiles']]):
            raise RuntimeError(f"'quantiles' field contained non-numbers. target_group={target_group}")


#
# `json_io_dict_from_quantile_csv_file()` row validator
#

def hub_row_validator(column_index_dict, row, target_group_dict):
    """
    Does hub row validation as documented in `json_io_dict_from_quantile_csv_file()`. Notes: recall that
    target_group_dict is none if row's target is invalid.
    """
    from zoltpy.cdc_io import _parse_date  # avoid circular imports


    error_messages = []  # return value. filled next

    location = row[column_index_dict['location']]
    target = row[column_index_dict['target']]

    # validate location (FIPS code)
    if target_group_dict and (location not in target_group_dict['locations']):
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
            if target_group_dict and (quantile_float not in target_group_dict['quantiles']):
                error_messages.append((MESSAGE_FORECAST_CHECKS, f"invalid quantile for target. quantile={quantile!r}, "
                                                                f"target={target!r}. row={row}"))
        except ValueError:
            pass  # ignore here - it will be caught by `json_io_dict_from_quantile_csv_file()`

    # check if point rows have empty quantile column.
    if (row[column_index_dict['type']] == 'point') and (quantile is not None):
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
            numeric_horizon = int(target_day_ahead_split[0].strip()) if len(target_day_ahead_split) == 2 \
                else int(target_week_ahead_split[0].strip())
        else:  # invalid target. don't add error message b/c caught by caller `_validated_rows_for_quantile_csv()`
            return error_messages  # terminate - remaining validation depends on valid numeric_horizon
    except ValueError:
        error_messages.append((MESSAGE_FORECAST_CHECKS, f"non-integer 'ahead' number in target: {target!r}. row={row}"))
        return error_messages  # terminate - remaining validation depends on valid numeric_horizon

    # validate date alignment
    # 1/4) for x day ahead targets the target_end_date should be forecast_date + x
    if 'day ahead' in target:
        if (target_end_date - forecast_date).days != numeric_horizon:
            error_messages.append((MESSAGE_FORECAST_CHECKS,
                                   f"invalid target_end_date: was not {numeric_horizon} day(s) after "
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
            delta_days = weekday_diff + datetime.timedelta(days=(7 * (numeric_horizon - 1)))
            exp_target_end_date = forecast_date + delta_days
        else:  # Tue through Sat
            # 4/4) (Tue on) for x week ahead targets, ensures that the 1-week ahead forecast is for the Sat after next
            delta_days = weekday_diff + datetime.timedelta(days=(7 * numeric_horizon))
            exp_target_end_date = forecast_date + delta_days
        if target_end_date != exp_target_end_date:
            error_messages.append((MESSAGE_DATE_ALIGNMENT,
                                   f"target_end_date was not the expected Saturday. forecast_date={forecast_date}, "
                                   f"target_end_date={target_end_date}. exp_target_end_date={exp_target_end_date}, "
                                   f"row={row}"))

    # done!
    return error_messages


#
# hub_quantile_prediction_dict_validator()
#

def hub_quantile_prediction_dict_validator(target_group_dict, prediction_dict):
    """
    Does hub prediction_dict validation as documented in `json_io_dict_from_quantile_csv_file()`
    """
    error_messages = []  # return value. filled next
    valid_quantiles = target_group_dict['quantiles']
    prediction_quantiles = prediction_dict['prediction']['quantile']
    if set(valid_quantiles) != set(prediction_quantiles):
        error_messages.append(f"prediction_dict quantiles != valid_quantiles. valid_quantiles={valid_quantiles}, "
                              f"prediction_quantiles={prediction_quantiles}")
    return error_messages
