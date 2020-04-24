import csv
import math
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
REQUIRED_COLUMNS = ['location', 'target', 'type', 'quantile', 'value']

#
# variables specific to the COVID19 project
#

# b/c there are so many possible targets, we generate using a range
COVID19_TARGET_NAMES = [f"{_} day ahead inc death" for _ in range(1, 131)] + \
                       [f"{_} day ahead cum death" for _ in range(1, 131)] + \
                       [f"{_} wk ahead inc death" for _ in range(21)] + \
                       [f"{_} wk ahead cum death" for _ in range(21)] + \
                       [f"{_} day ahead inc hosp" for _ in range(131)]


def covid19_row_validator(row, error_messages, target_name, location, row_type, quantile, value):
    """
    Does COVID19-specific row validation.
    """
    # validate location - https://en.wikipedia.org/wiki/Federal_Information_Processing_Standard_state_code
    # - '01' through '95', and 'US'
    if len(location) != 2:
        error_messages.append(f"invalid FIPS: not two characters: {location!r}. row={row}")

    if location != 'US':  # must be a number b/w 1 and 95 inclusive
        fips_min, fips_max = 1, 95
        try:
            fips_int = int(location)
            if (fips_int < fips_min) or (fips_int > fips_max):
                error_messages.append(f"invalid FIPS: two character int but out of range {fips_min}-{fips_max}: "
                                      f"{location!r}")
        except ValueError:
            error_messages.append(f"invalid FIPS: two characters but not an int: {location!r}. row={row}")


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
        _, error_messages = json_io_dict_from_quantile_csv_file(cdc_csv_fp, COVID19_TARGET_NAMES, covid19_row_validator)
        if error_messages:
            return error_messages
        else:
            return "no errors"


#
# json_io_dict_from_quantile_csv_file()
#

# a FIPS code: https://en.wikipedia.org/wiki/Federal_Information_Processing_Standard_state_code -
#         that is, '01' through '95', and 'US'

def json_io_dict_from_quantile_csv_file(csv_fp, valid_target_names, row_validator=None):
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
    :param row_validator: an optional function of these seven args that is run to perform additional project-specific
        validations:
        - row: the raw row being validated. NB: the order of columns is variable. however, the five application-
            independent ones are passed separately: target_name, location, row_type, quantile, and value
        - error_messages: a list of messages that can be appended to as a side effect. callers should be well-behaved
            and not make any other changes to this list
        - target_name: application-independent value extracted from row for convenience
        - location: ""
        - row_type: ""
        - quantile: ""
        - value: ""
    :return 2-tuple: (json_io_dict, error_messages) where the former is a "JSON IO dict" (aka 'json_io_dict' by callers)
        that contains the two types of predictions. see https://docs.zoltardata.com/ for details. json_io_dict is None
        if there were errors
    """
    # load and validate the rows (validation step 1/2). error_messages is one of the the return values (filled next)
    rows, error_messages = _validated_rows_for_quantile_csv(csv_fp, valid_target_names, row_validator)

    if error_messages:
        return None, error_messages  # terminate processing b/c we can't proceed to step 1/2 with invalid rows

    # collect point and quantile values for each row and then add the actual prediction dicts. each point row has its
    # own dict, but quantile rows are grouped into one dict
    prediction_dicts = []  # the 'predictions' section of the returned value. filled next
    rows.sort(key=lambda _: (_[0], _[1], _[2]))  # sorted for groupby()
    for (target_name, location, is_point_row), quantile_val_grouper in \
            groupby(rows, key=lambda _: (_[0], _[1], _[2])):
        # fill values for points and bins. NB: should only be one point row per location/target pair
        point_values = []  # should be at most one, but use a list to help validate
        quant_quantiles, quant_values = [], []
        for _, _, _, quantile, value in quantile_val_grouper:
            if is_point_row and not point_values:
                point_values.append(value)  # quantile is NA
            elif is_point_row:
                error_messages.append(f"found more than one point value for the same target_name, location. "
                                      f"target_name={target_name!r}, location={location!r}, "
                                      f"this point value={value}, previous point_value={point_values[0]}")
            else:
                quant_quantiles.append(quantile)
                quant_values.append(value)

        # add the actual prediction dicts
        if point_values:
            if len(point_values) > 1:
                error_messages.append(f"len(point_values) > 1: {point_values}")

            point_value = point_values[0]
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

    # validate prediction_dicts (validation step 2/2)
    for prediction_dict in prediction_dicts:
        if prediction_dict['class'] == QUANTILE_PREDICTION_CLASS:
            pred_dict_error_messages = _validate_quantile_predictions(prediction_dict)  # raises o/w
            error_messages.extend(pred_dict_error_messages)

    # done
    return {'meta': {}, 'predictions': prediction_dicts}, error_messages


def _validated_rows_for_quantile_csv(csv_fp, valid_target_names, row_validator):
    """
    `json_io_dict_from_quantile_csv_file()` helper function.

    :param csv_fp: as passed to caller
    :param valid_target_names: as passed to caller
    :param row_validator: ""
    :return: 2-tuple: (validated_rows, error_messages)
    """
    from zoltpy.cdc import CDC_POINT_ROW_TYPE, parse_value  # avoid circular imports


    error_messages = []  # list of strings. return value. set below if any issues

    csv_reader = csv.reader(csv_fp, delimiter=',')
    header = next(csv_reader)
    try:
        location_idx, target_idx, row_type_idx, quantile_idx, value_idx = _validate_header(header)
    except RuntimeError as re:
        error_messages.append(re.args)
        return [], error_messages  # terminate processing

    error_targets = set()  # output set of invalid target names

    rows = []  # list of parsed and validated rows. filled next
    for row in csv_reader:
        if len(row) != len(header):
            error_messages.append(f"invalid number of items in row. len(header)={len(header)} but len(row)={len(row)}. "
                                  f"row={row}")
            return [], error_messages  # terminate processing

        target_name, location, row_type, quantile, value = \
            row[target_idx], row[location_idx], row[row_type_idx], row[quantile_idx], row[value_idx]

        # do optional application-specific row validation. NB: error_messages is modified in-place as a side-effect
        if row_validator:
            row_validator(row, error_messages, target_name, location, row_type, quantile, value)

        # validate target_name
        if target_name not in valid_target_names:
            error_targets.add(target_name)

        row_type = row_type.lower()
        is_point_row = (row_type == CDC_POINT_ROW_TYPE.lower())
        quantile = parse_value(quantile)  # None if 'NA'
        value = parse_value(value)

        # convert parsed date back into string suitable for JSON.
        # NB: recall all targets are "type": "discrete", so we only accept ints and floats
        # if isinstance(value, datetime.date):
        #     value = value.strftime(YYYY_MM_DD_DATE_FORMAT)
        rows.append([target_name, location, is_point_row, quantile, value])

    # Add invalid targets to errors
    if len(error_targets) > 0:
        error_messages.append(f"invalid target name(s): {error_targets!r}")

    return rows, error_messages


def _validate_header(header):
    """
    `json_io_dict_from_quantile_csv_file()` helper function.

    :param header: first row from the csv file
    :return: location_idx, target_idx, row_type_idx, quantile_idx, value_idx
    """
    counts = [header.count(required_column) == 1 for required_column in REQUIRED_COLUMNS]
    if not all(counts):
        raise RuntimeError(f"invalid header. did not contain the required columns. header={header}, "
                           f"REQUIRED_COLUMNS={REQUIRED_COLUMNS}")

    return [header.index(required_column) for required_column in REQUIRED_COLUMNS]


def _validate_quantile_predictions(prediction_dict):
    """
    `json_io_dict_from_quantile_csv_file()` helper function. Implements the quantile checks at
    https://docs.zoltardata.com/validation/#quantile-prediction-elements . NB: this function is a copy/paste (with
    simplifications) of Zoltar's `utils.forecast._validate_quantile_predictions()`

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
