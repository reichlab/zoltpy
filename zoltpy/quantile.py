import csv
import datetime
import math
from collections import defaultdict
from itertools import groupby
from pathlib import Path

import click


#
# project-independent variables
#

# prediction classes for use in "JSON IO dict" conversion
BIN_DISTRIBUTION_CLASS = 'bin'
NAMED_DISTRIBUTION_CLASS = 'named'
POINT_PREDICTION_CLASS = 'point'
SAMPLE_PREDICTION_CLASS = 'sample'
QUANTILE_PREDICTION_CLASS = 'quantile'

# quantile csv I/O
REQUIRED_COLUMNS = ('location', 'target', 'type', 'quantile', 'value')

#
# variables specific to the COVID19 project
#

# b/c there are so many possible targets, we generate using a range
COVID19_TARGET_NAMES = [f"{_} day ahead inc death" for _ in range(1, 131)] + \
                       [f"{_} day ahead cum death" for _ in range(1, 131)] + \
                       [f"{_} wk ahead inc death" for _ in range(21)] + \
                       [f"{_} wk ahead cum death" for _ in range(21)] + \
                       [f"{_} day ahead inc hosp" for _ in range(131)]

# from https://github.com/reichlab/covid19-forecast-hub/blob/master/template/state_fips_codes.csv
# (probably via https://en.wikipedia.org/wiki/Federal_Information_Processing_Standard_state_code )
VALID_FIPS_STATE_CODES = ['01', '02', '04', '05', '06', '08', '09', '10', '11', '12', '13', '15', '16', '17', '18',
                          '19', '20', '21', '22', '23', '24', '25', '26', '27', '28', '29', '30', '31', '32', '33',
                          '34', '35', '36', '37', '38', '39', '40', '41', '42', '44', '45', '46', '47', '48', '49',
                          '50', '51', '53', '54', '55', '56', '60', '66', '69', '72', '74', '78', 'US']  # 'US' is extra


def covid19_row_validator(column_index_dict, row):
    """
    Does COVID19-specific row validation. Notes:

    - expects these `valid_target_names` passed to `json_io_dict_from_quantile_csv_file()`: COVID19_TARGET_NAMES
    - expects these `addl_req_cols` passed to `json_io_dict_from_quantile_csv_file()`: ['forecast_date', 'target_end_date']
    """
    from zoltpy.cdc import _parse_date  # avoid circular imports


    error_messages = []  # returned value. filled next

    # validate location (FIPS code)
    location = row[column_index_dict['location']]
    if location not in VALID_FIPS_STATE_CODES:
        error_messages.append(f"invalid FIPS location: {location!r}. row={row}")

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
    try:
        step_ahead_increment = int(target.split('day ahead')[0].strip()) if 'day ahead' in target \
            else int(target.split('wk ahead')[0].strip())
    except ValueError:
        error_messages.append(f"non-integer number of weeks ahead in 'wk ahead' target: {target!r}. row={row}")
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


#
# Notes:
# - below code is a temporary solution to validation during COVID-19 crunch time. todo it will be refactored later
# - as such, we hard-code target information: all targets are: "type": "discrete", "is_step_ahead": true
# - validation functions return lists of error messages, formatted for output during processing. processing continues
#   as long as possible (ideally the entire file) so that all errors can be reported to the user. however, catastrophic
#   errors (such as an invalid header) must terminate immediately
#


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
        _, error_messages = json_io_dict_from_quantile_csv_file(cdc_csv_fp, COVID19_TARGET_NAMES, covid19_row_validator,
                                                                ['forecast_date', 'target_end_date'])
        if error_messages:
            return error_messages
        else:
            return "no errors"


#
# json_io_dict_from_quantile_csv_file()
#

def json_io_dict_from_quantile_csv_file(csv_fp, valid_target_names, row_validator=None, addl_req_cols=()):
    """
    Utility that validates and extracts the two types of predictions found in quantile CSV files (PointPredictions and
    QuantileDistributions), returning them as a "JSON IO dict" suitable for loading into the database (see
    `load_predictions_from_json_io_dict()`). Note that the returned dict's "meta" section is empty. This function is
    flexible with respect to the inputted column contents and order: It allows the required columns to be in any
    position, and it ignores all other columns. The required columns are:

    - `target`: a unique id for the target
    - `location`: translated to Zoltar's `unit` concept.
    - `type`: one of either `point` or `quantile`
    - `quantile`: a value between 0 and 1 (inclusive), representing the quantile displayed in this row. if
        `type=="point"` then `NULL`.
    - `value`: a numeric value representing the value of the cumulative distribution function evaluated at the specified
        `quantile`

    :param csv_fp: an open quantile csv file-like object. the quantile CSV file format is documented at
        https://docs.zoltardata.com/
    :param valid_target_names: list of strings of valid targets to validate against
    :param row_validator: an optional function of these args that is run to perform additional project-specific
        validations. returns a list of `error_messages`.
        - column_index_dict: as returned by _validate_header(): a dict that maps column_name -> its index in header (row)
        - row: the raw row being validated. NB: the order of columns is variable, but callers can use column_index_dict
            to index into row
    :param addl_req_cols: an optional list of strings naming columns in addition to REQUIRED_COLUMNS that are required
    :return 2-tuple: (json_io_dict, error_messages) where the former is a "JSON IO dict" (aka 'json_io_dict' by callers)
        that contains the two types of predictions. see https://docs.zoltardata.com/ for details. json_io_dict is None
        if there were errors
    """
    # load and validate the rows (validation step 1/2). error_messages is one of the the return values (filled next)
    rows, error_messages = _validated_rows_for_quantile_csv(csv_fp, valid_target_names, row_validator, addl_req_cols)

    if error_messages:
        return None, error_messages  # terminate processing b/c we can't proceed to step 1/2 with invalid rows

    # step 1/3: process rows, validating and collecting point and quantile values for each row. then add the actual
    # prediction dicts. each point row has its own dict, but quantile rows are grouped into one dict.
    prediction_dicts = []  # the 'predictions' section of the returned value. filled next
    rows.sort(key=lambda _: (_[0], _[1], _[2]))  # sorted for groupby()
    for (target_name, location, is_point_row), quantile_val_grouper in \
            groupby(rows, key=lambda _: (_[0], _[1], _[2])):
        # fill values for points and bins
        point_values = []
        quant_quantiles, quant_values = [], []
        for _, _, _, quantile, value in quantile_val_grouper:
            if is_point_row:
                point_values.append(value)  # quantile is NA
            else:
                quant_quantiles.append(quantile)
                quant_values.append(value)

        # add the actual prediction dicts
        for point_value in point_values:
            prediction_dicts.append({'unit': location,
                                     'target': target_name,
                                     'class': POINT_PREDICTION_CLASS,  # PointPrediction
                                     'prediction': {
                                         'value': point_value}})
        if quant_quantiles:
            prediction_dicts.append({'unit': location,
                                     'target': target_name,
                                     'class': QUANTILE_PREDICTION_CLASS,  # QuantileDistribution
                                     'prediction': {
                                         'quantile': quant_quantiles,
                                         'value': quant_values}})

    # step 2/3: validate individual prediction_dicts. along the way fill loc_targ_to_pred_classes, which helps to do
    # "prediction"-level validations at the end of this function. it maps 2-tuples to a list of prediction classes
    # (strs):
    loc_targ_to_pred_classes = defaultdict(list)  # (unit_name, target_name) -> [prediction_class1, ...]
    for prediction_dict in prediction_dicts:
        unit_name = prediction_dict['unit']
        target_name = prediction_dict['target']
        prediction_class = prediction_dict['class']
        loc_targ_to_pred_classes[(unit_name, target_name)].append(prediction_class)
        if prediction_dict['class'] == QUANTILE_PREDICTION_CLASS:
            pred_dict_error_messages = _validate_quantile_prediction_dict(prediction_dict)  # raises o/w
            error_messages.extend(pred_dict_error_messages)

    # step 3/3: do "prediction"-level validations
    # validate: "Within a Prediction, there cannot be more than 1 Prediction Element of the same type".
    duplicate_unit_target_tuples = [(unit, target, pred_classes) for (unit, target), pred_classes
                                    in loc_targ_to_pred_classes.items()
                                    if len(pred_classes) != len(set(pred_classes))]
    if duplicate_unit_target_tuples:
        error_messages.append(f"Within a Prediction, there cannot be more than 1 Prediction Element of the same class. "
                              f"Found these duplicate unit/target tuples: {duplicate_unit_target_tuples}")

    # done
    return {'meta': {}, 'predictions': prediction_dicts}, error_messages


def _validated_rows_for_quantile_csv(csv_fp, valid_target_names, row_validator, addl_req_cols):
    """
    `json_io_dict_from_quantile_csv_file()` helper function.

    :return: 2-tuple: (validated_rows, error_messages)
    """
    from zoltpy.cdc import CDC_POINT_ROW_TYPE, _parse_value  # avoid circular imports


    error_messages = []  # list of strings. return value. set below if any issues

    csv_reader = csv.reader(csv_fp, delimiter=',')
    header = next(csv_reader)
    try:
        column_index_dict = _validate_header(header, addl_req_cols)
    except RuntimeError as re:
        error_messages.append(re.args[0])
        return [], error_messages  # terminate processing

    error_targets = set()  # output set of invalid target names

    rows = []  # list of parsed and validated rows. filled next
    for row in csv_reader:
        if len(row) != len(header):
            error_messages.append(f"invalid number of items in row. len(header)={len(header)} but len(row)={len(row)}. "
                                  f"row={row}")
            return [], error_messages  # terminate processing

        # do optional application-specific row validation. NB: error_messages is modified in-place as a side-effect
        location, target_name, row_type, quantile, value = [row[column_index_dict[column]] for column in
                                                            REQUIRED_COLUMNS]
        if row_validator:
            error_messages.extend(row_validator(column_index_dict, row))

        # validate target_name
        if target_name not in valid_target_names:
            error_targets.add(target_name)

        row_type = row_type.lower()
        is_point_row = (row_type == CDC_POINT_ROW_TYPE.lower())
        quantile = _parse_value(quantile)  # None if 'NA'
        value = _parse_value(value)

        # convert parsed date back into string suitable for JSON.
        # NB: recall all targets are "type": "discrete", so we only accept ints and floats
        # if isinstance(value, datetime.date):
        #     value = value.strftime(YYYY_MM_DD_DATE_FORMAT)
        rows.append([target_name, location, is_point_row, quantile, value])

    # Add invalid targets to errors
    if len(error_targets) > 0:
        error_messages.append(f"invalid target name(s): {error_targets!r}")

    return rows, error_messages


def _validate_header(header, addl_req_cols):
    """
    `json_io_dict_from_quantile_csv_file()` helper function.

    :param header: first row from the csv file
    :param addl_req_cols: an optional list of strings naming columns in addition to REQUIRED_COLUMNS that are required
    :return: column_index_dict: a dict that maps column_name -> its index in header
    """
    required_columns = list(REQUIRED_COLUMNS)
    required_columns.extend(addl_req_cols)
    counts = [header.count(required_column) == 1 for required_column in required_columns]
    if not all(counts):
        raise RuntimeError(f"invalid header. did not contain the required columns. header={header}, "
                           f"required_columns={required_columns}")

    return {column: header.index(column) for column in header}


def _validate_quantile_prediction_dict(prediction_dict):
    """
    `json_io_dict_from_quantile_csv_file()` helper function. Implements the quantile checks at
    https://docs.zoltardata.com/validation/#quantile-prediction-elements . NB: this function is a copy/paste (with
    simplifications) of Zoltar's `utils.forecast._validate_quantile_prediction_dict()`

    :param prediction_dict: as documented at https://docs.zoltardata.com/
    :return list of strings, one per error. [] if prediction_dict is valid
    """
    error_messages = []  # list of strings. return value. set below if any issues

    # validate: "The number of elements in the `quantile` and `value` vectors should be identical."
    prediction_data = prediction_dict['prediction']
    pred_data_quantiles = prediction_data['quantile']
    pred_data_values = prediction_data['value']
    if len(pred_data_quantiles) != len(pred_data_values):
        # note that this error must stop processing b/c subsequent steps rely on their being the same lengths
        # (e.g., `zip()`)
        error_messages.append(f"The number of elements in the `quantile` and `value` vectors should be identical. "
                              f"|quantile|={len(pred_data_quantiles)}, |value|={len(pred_data_values)}, "
                              f"prediction_dict={prediction_dict}")
        return error_messages  # terminate processing

    # validate: "Entries in the database rows in the `quantile` column must be numbers in [0, 1].
    quantile_types_set = set(map(type, pred_data_quantiles))
    if not (quantile_types_set <= {int, float}):
        error_messages.append(f"wrong data type in `quantile` column, which should only contain ints or floats. "
                              f"quantile column={pred_data_quantiles}, quantile_types_set={quantile_types_set}, "
                              f"prediction_dict={prediction_dict}")
    elif (min(pred_data_quantiles) < 0.0) or (max(pred_data_quantiles) > 1.0):
        error_messages.append(f"Entries in the database rows in the `quantile` column must be numbers in [0, 1]. "
                              f"quantile column={pred_data_quantiles}, prediction_dict={prediction_dict}")

    # validate: `quantile`s must be unique."
    if len(set(pred_data_quantiles)) != len(pred_data_quantiles):
        error_messages.append(f"`quantile`s must be unique. quantile column={pred_data_quantiles}, "
                              f"prediction_dict={prediction_dict}")

    # validate: "The data format of `value` should correspond or be translatable to the `type` as in the target
    # definition."
    # NB: recall all targets are "type": "discrete", so we only accept ints and floats

    prob_values_set = set(map(type, pred_data_values))
    if not (prob_values_set <= {int, float}):
        error_messages.append(f"The data format of `value` should correspond or be translatable to the `type` as "
                              f"in the target definition, but one of the value values was not. "
                              f"values={pred_data_values}, prediction_dict={prediction_dict}")

    # validate: "Entries in `value` must be non-decreasing as quantiles increase." (i.e., are monotonic).
    # note: there are no date targets, so we format as strings for the comparison (incoming are strings).
    # note: we do not assume quantiles are sorted, so we first sort before checking for non-decreasing

    # per https://stackoverflow.com/questions/7558908/unpacking-a-list-tuple-of-pairs-into-two-lists-tuples
    pred_data_quantiles, pred_data_values = zip(*sorted(zip(pred_data_quantiles, pred_data_values), key=lambda _: _[0]))


    def le_with_tolerance(a, b):  # a <= b ?
        return True if math.isclose(a, b, rel_tol=1e-05) else a <= b  # default: rel_tol=1e-09


    if not all([le_with_tolerance(a, b) for a, b in zip(pred_data_values, pred_data_values[1:])]):
        error_messages.append(f"Entries in `value` must be non-decreasing as quantiles increase. "
                              f"value column={pred_data_values}, prediction_dict={prediction_dict}")

    # validate: "Entries in `value` must obey existing ranges for targets." recall: "The range is assumed to be
    # inclusive on the lower bound and open on the upper bound, # e.g. [a, b)."
    # NB: range is not tested per @nick: "All of these should be [0, Inf]"

    # done
    return error_messages
