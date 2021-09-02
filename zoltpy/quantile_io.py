import csv
import datetime
import math
from collections import defaultdict
from itertools import groupby

#
# project-independent variables
#

# prediction classes for use in "JSON IO dict" conversion
from numbers import Number


BIN_DISTRIBUTION_CLASS = 'bin'
NAMED_DISTRIBUTION_CLASS = 'named'
POINT_PREDICTION_CLASS = 'point'
SAMPLE_PREDICTION_CLASS = 'sample'
QUANTILE_PREDICTION_CLASS = 'quantile'

# quantile csv I/O
REQUIRED_COLUMNS = ('location', 'target', 'type', 'quantile', 'value')
POINT_ROW_TYPE = 'point'
QUANTILE_ROW_TYPE = 'quantile'
RETRACTION_VALUE = 'NULL'  # point or quantile value that represents a retraction

#
# Note: The following code is a somewhat temporary solution to validation during COVID-19 crunch time. As such, we
# hard-code target information: all targets are: "type": "discrete", "is_step_ahead": true. Also, all validation
# functions return lists of error messages, formatted for output during processing. Processing continues as long as
# possible (ideally the entire file) so that all errors can be reported to the user.
#


#
# json_io_dict_from_quantile_csv_file()
#

# these vars are used to order error messages according to the list here:
# https://github.com/reichlab/covid19-forecast-hub/wiki/Forecast-Checks
MESSAGE_FORECAST_CHECKS = 0  # 2. forecast checks
MESSAGE_DATE_ALIGNMENT = 1  # 3. validates date alignment as documented in the issue add additional validations
MESSAGE_QUANTILES_AND_VALUES = 2  # 4. validates quantiles and values (i.e,. at the prediction level)
MESSAGE_QUANTILES_AS_A_GROUP = 3  # 5. validates quantiles as a group


def json_io_dict_from_quantile_csv_file(csv_fp, valid_target_names, row_validator=None, addl_req_cols=()):
    """
    Utility that validates and extracts the two types of predictions found in quantile CSV files (points and quantiles),
    returning them as a "JSON IO dict" suitable for loading into the database (see
    `load_predictions_from_json_io_dict()`). Note that the returned dict's "meta" section is empty. This function is
    flexible with respect to the inputted column contents and order: It allows the required columns to be in any
    position. The base required columns are (from REQUIRED_COLUMNS):

    - `target`: a unique id for the target
    - `location`: translated to Zoltar's `unit` concept
    - `type`: one of either `point` or `quantile`
    - `quantile`: a value between 0 and 1 (inclusive), representing the quantile displayed in this row. if
        `type=="point"` then `NULL`
    - `value`: a numeric value representing the value of the cumulative distribution function evaluated at the specified
        `quantile`

    :param csv_fp: an open quantile csv file-like object. the quantile CSV file format is documented at
        https://docs.zoltardata.com/
    :param valid_target_names: list of strings of valid targets to validate against
    :param row_validator: an optional function that takes the following args and that is run to perform additional
        project-specific validations. returns `error_messages` (a list of strings).
        - column_index_dict: as returned by _validate_header(): a dict that maps column_name -> its index in header (row)
        - row: the raw row being validated. NB: the order of columns is variable, but callers can use column_index_dict
            to index into row
        - is_target_valid: True or False depending on whether the row's target was in valid_target_names
    :param addl_req_cols: an optional list of strings naming columns in addition to REQUIRED_COLUMNS that are required
    :return 2-tuple: (json_io_dict, error_messages) where the former is a "JSON IO dict" (aka 'json_io_dict' by callers)
        that contains the two types of predictions. see https://docs.zoltardata.com/ for details. json_io_dict is None
        if there were errors. the second arg is a list of 2-tuples: (priority, error_message). priority is an int that's
        used by callers to sort the messages
    """
    # load and validate the rows (validation step 1/4). error_messages is one of the the return values (filled next)
    rows, error_messages = _validated_rows_for_quantile_csv(csv_fp, valid_target_names, row_validator, addl_req_cols)

    # step 2/4: process rows, collecting point and quantile values for each row. then add the actual prediction dicts.
    # each point row has its own dict, but quantile rows are grouped into one dict.
    prediction_dicts = []  # the 'predictions' section of the returned value. filled next
    rows.sort(key=lambda _: (_[0], _[1], _[2]))  # sorted for groupby()
    for (target, location, is_point_row), quantile_val_grouper in groupby(rows, key=lambda _: (_[0], _[1], _[2])):
        # fill values for points and bins
        point_values = []
        quant_quantiles, quant_values = [], []
        for _, _, _, quantile, value in quantile_val_grouper:
            if is_point_row:
                point_values.append(value)  # quantile is NA
            else:
                quant_quantiles.append(quantile)
                quant_values.append(value)

        # add the actual prediction dicts, converting retraction flags (value = NULL) into actual retractions as needed
        for point_value in point_values:
            prediction_dicts.append({'unit': location,
                                     'target': target,
                                     'class': POINT_PREDICTION_CLASS,
                                     'prediction': None if point_value is None else {'value': point_value}})
        if quant_quantiles:
            is_all_vals_none = all(map(lambda _: _ is None, quant_values))
            is_any_vals_none = any(map(lambda _: _ is None, quant_values))
            if is_any_vals_none and not is_all_vals_none:  # some but not all
                error_messages.append((MESSAGE_QUANTILES_AND_VALUES,
                                       f"Retracted quantile values must all be {RETRACTION_VALUE!r}, but only some "
                                       f"were. quant_values={quant_values}"))
            else:
                prediction_dicts.append({'unit': location,
                                         'target': target,
                                         'class': QUANTILE_PREDICTION_CLASS,
                                         'prediction': None if is_all_vals_none else {'quantile': quant_quantiles,
                                                                                      'value': quant_values}})

    # step 3/4: validate individual prediction_dicts. along the way fill loc_targ_to_pred_classes, which helps to do
    # "prediction"-level validations at the end of this function. it maps 2-tuples to a list of prediction classes
    # (strs):
    loc_targ_to_pred_classes = defaultdict(list)  # (unit, target) -> [prediction_class1, ...]
    for prediction_dict in prediction_dicts:
        unit = prediction_dict['unit']
        target = prediction_dict['target']
        prediction_class = prediction_dict['class']
        is_retraction = prediction_dict['prediction'] is None
        loc_targ_to_pred_classes[(unit, target)].append(prediction_class)
        if (prediction_dict['class'] == QUANTILE_PREDICTION_CLASS) and (not is_retraction):
            pred_dict_error_messages = _validate_quantile_prediction_dict(prediction_dict)
            error_messages.extend(pred_dict_error_messages)

    # step 4/4: do "prediction"-level validations
    # validate: "Within a Prediction, there cannot be more than 1 Prediction Element of the same type".
    duplicate_unit_target_tuples = [(unit, target, pred_classes) for (unit, target), pred_classes
                                    in loc_targ_to_pred_classes.items()
                                    if len(pred_classes) != len(set(pred_classes))]
    if duplicate_unit_target_tuples:
        if len(duplicate_unit_target_tuples) > 10:  # pick first 10 tuples to reduce output
            duplicate_unit_target_tuples = duplicate_unit_target_tuples[:10] + ['...']
        error_messages.append((MESSAGE_QUANTILES_AND_VALUES,
                               f"Within a Prediction, there cannot be more than 1 Prediction Element of the same "
                               f"class. Found these duplicate unit/target/classes tuples: "
                               f"{duplicate_unit_target_tuples}"))

    # validate: "There must be exactly one point prediction for each location/target pair"
    unit_target_point_count = [(unit, target, pred_classes.count('point')) for (unit, target), pred_classes
                               in loc_targ_to_pred_classes.items()
                               if pred_classes.count('point') > 1]
    if unit_target_point_count:
        if len(unit_target_point_count) > 10:  # pick first 10 tuples to reduce output
            unit_target_point_count = unit_target_point_count[:10] + ['...']
        error_messages.append((MESSAGE_QUANTILES_AS_A_GROUP,
                               f"There must be zero or one point prediction for each location/target pair. Found "
                               f"these unit, target, point counts tuples did not have exactly one point: "
                               f"{unit_target_point_count}"))

    # done
    return {'meta': {}, 'predictions': prediction_dicts}, error_messages


def _validated_rows_for_quantile_csv(csv_fp, valid_target_names, row_validator, addl_req_cols):
    """
    `json_io_dict_from_quantile_csv_file()` helper function

    :return: 2-tuple: (validated_rows, error_messages). the latter is the same as
        `json_io_dict_from_quantile_csv_file()`
    """
    from zoltpy.cdc_io import _parse_value  # avoid circular imports


    error_messages = []  # return value. set below if any issues

    csv_reader = csv.reader(csv_fp, delimiter=',')
    header = next(csv_reader)
    column_index_dict, error_message = _validate_header(header, addl_req_cols)
    if error_message is not None:
        error_messages.append((MESSAGE_FORECAST_CHECKS, error_message))

    if column_index_dict is None:
        return [], error_messages  # terminate processing b/c column_index_dict is required to get columns

    error_targets = set()  # output set of invalid target names

    rows = []  # list of parsed and validated rows. filled next
    for row in csv_reader:
        if len(row) != len(header):
            error_messages.append((MESSAGE_FORECAST_CHECKS, f"invalid number of items in row. len(header)="
                                                            f"{len(header)} but len(row)={len(row)}. row={row}"))
            return [], error_messages  # terminate processing b/c column_index_dict requires correct number of rows

        location, target, row_type, quantile, value = [row[column_index_dict[column]] for column in REQUIRED_COLUMNS]

        # validate target
        is_valid_target = target in valid_target_names
        if not is_valid_target:
            error_targets.add(target)

        # validate row_type, quantile, and value
        if (not row_type == POINT_ROW_TYPE) and (not row_type == QUANTILE_ROW_TYPE):
            error_messages.append((MESSAGE_FORECAST_CHECKS, f"entries in the `type` column must be either "
                                                            f"{POINT_ROW_TYPE!r} or {QUANTILE_ROW_TYPE!r}: "
                                                            f"{row_type!r}. row={row}"))
            return [], error_messages  # terminate processing b/c we don't know how to handle the row type

        is_point_row = (row_type == POINT_ROW_TYPE)
        is_retraction = (value == RETRACTION_VALUE)
        quantile = _parse_value(quantile)  # None if not an int, float, or Date. float might be inf or nan
        parsed_value = _parse_value(value)  # ""
        if (not is_point_row) and ((quantile is None) or
                                   (isinstance(quantile, datetime.date)) or
                                   (not math.isfinite(quantile)) or  # inf, nan
                                   not (0 <= quantile <= 1)):
            error_messages.append((MESSAGE_FORECAST_CHECKS, f"entries in the `quantile` column must be an int or "
                                                            f"float in [0, 1]: {quantile}. row={row}"))
        elif (not is_retraction) and ((parsed_value is None) or
                                      (isinstance(parsed_value, datetime.date)) or
                                      (not math.isfinite(parsed_value))):  # inf, nan
            error_messages.append((MESSAGE_FORECAST_CHECKS, f"entries in the `value` column must be an int or float: "
                                                            f"{parsed_value}. row={row}"))

        # do optional application-specific row validation. NB: error_messages is modified in-place as a side-effect
        if row_validator:
            error_messages.extend(row_validator(column_index_dict, row, is_valid_target))

        # convert parsed date back into string suitable for JSON.
        # NB: recall all targets are "type": "discrete", so we only accept ints and floats
        # if isinstance(parsed_value, datetime.date):
        #     parsed_value = parsed_value.strftime(YYYY_MM_DD_DATE_FORMAT)
        rows.append([target, location, is_point_row, quantile, parsed_value])

    # Add invalid targets to errors
    if len(error_targets) > 0:
        error_messages.append((MESSAGE_FORECAST_CHECKS, f"invalid target name(s): {error_targets!r}"))

    # validate the rule "No uploaded forecast can have no rows of data."
    if not rows:
        error_messages.append((MESSAGE_FORECAST_CHECKS, f"no data rows in file"))

    return rows, error_messages


def _validate_header(header, addl_req_cols):
    """
    `json_io_dict_from_quantile_csv_file()` helper function.

    :param header: first row from the csv file
    :param addl_req_cols: an optional list of strings naming columns in addition to REQUIRED_COLUMNS that are required
    :return 2-tuple: (column_index_dict, error_message) where the former is a dict that maps column_name -> its index
        in header. column_index_dict is None if there was a *terminal* error, in this case when any of the required
        columns were not present or when there are duplicate column names. error_message is a str if there was an error,
        and None o/w
    """
    header_set = set(header)
    if len(header_set) != len(header):
        return None, f"invalid header. found duplicate column(s): header={header}"  # terminal error

    required_columns = list(REQUIRED_COLUMNS) + list(addl_req_cols)
    req_cols_set = set(required_columns)
    if not (req_cols_set <= header_set):
        return None, \
               f"invalid header. did not contain the required column(s). diff={header_set ^ req_cols_set}, " \
               f"header={header_set}, required_columns={req_cols_set}"  # non-terminal error

    column_index_dict = {column: header.index(column) for column in required_columns}
    if header_set != req_cols_set:
        return column_index_dict, \
               f"invalid header. contained extra columns(s). diff={header_set ^ req_cols_set}, " \
               f"header={header_set}, required_columns={req_cols_set}"  # non-terminal error
    else:
        return column_index_dict, None  # no error


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
        error_messages.append((MESSAGE_QUANTILES_AND_VALUES,
                               f"The number of elements in the `quantile` and `value` vectors should be identical. "
                               f"|quantile|={len(pred_data_quantiles)}, |value|={len(pred_data_values)}, "
                               f"prediction_dict={prediction_dict}"))

    # validate: `quantile`s must be unique."
    if len(set(pred_data_quantiles)) != len(pred_data_quantiles):
        error_messages.append((MESSAGE_QUANTILES_AND_VALUES,
                               f"`quantile`s must be unique. quantile column={pred_data_quantiles}, "
                               f"prediction_dict={prediction_dict}"))

    # validate: "Entries in `value` must be non-decreasing as quantiles increase." (i.e., are monotonic).
    # note: there are no date targets, so we format as strings for the comparison (incoming are strings).
    # note: we do not assume quantiles are sorted, so we first sort before checking for non-decreasing

    # per https://stackoverflow.com/questions/7558908/unpacking-a-list-tuple-of-pairs-into-two-lists-tuples
    pred_data_quantiles, pred_data_values = zip(*sorted(zip(pred_data_quantiles, pred_data_values), key=lambda _: _[0]))


    def le_with_tolerance(a, b):  # a <= b ?
        if (not isinstance(a, Number)) or (not isinstance(b, Number)):
            # handles case where testing an invalid value that's == None
            return False
        elif math.isclose(a, b, rel_tol=1e-05):  # default: rel_tol=1e-09
            return True
        else:
            return a <= b


    is_le_values = [le_with_tolerance(a, b) for a, b in zip(pred_data_values, pred_data_values[1:])]
    if not all(is_le_values):
        error_messages.append((MESSAGE_QUANTILES_AND_VALUES,
                               f"Entries in `value` must be non-decreasing as quantiles increase. "
                               f"value column={pred_data_values}, is_le_values={is_le_values}, "
                               f"prediction_dict={prediction_dict}"))

    # validate: "Entries in `value` must obey existing ranges for targets." recall: "The range is assumed to be
    # inclusive on the lower bound and open on the upper bound, # e.g. [a, b)."
    # NB: range is not tested per @nick: "All of these should be [0, Inf]"

    # done
    return error_messages


#
# quantile_csv_rows_from_json_io_dict()
#

def quantile_csv_rows_from_json_io_dict(json_io_dict):
    """
    The same as `csv_rows_from_json_io_dict()`, but only returns data in REQUIRED_COLUMNS ('location', 'target', 'type',
    'quantile', 'value').

    :param json_io_dict: a "JSON IO dict" to load from. see docs for details. the "meta" section is ignored
    :return: a list of CSV rows including header - see CSV_HEADER
    """
    from zoltpy.csv_io import csv_rows_from_json_io_dict  # avoid circular imports


    # since we've already implemented `csv_rows_from_json_io_dict()`, our approach is to use it, transforming as needed
    csv_rows = csv_rows_from_json_io_dict(json_io_dict)
    csv_rows.pop(0)  # skip header
    rows = [list(REQUIRED_COLUMNS)]  # add header. rename the 'class' column to 'type'
    for location, target, pred_class, value, cat, prob, sample, quantile, family, param1, param2, param3 in csv_rows:
        if pred_class not in ['point', 'quantile']:  # keep only rows whose 'type' is 'point' or 'quantile'
            continue

        rows.append([location, target, pred_class, quantile, value])  # keep only quantile-related columns
    return rows


#
# summarized_error_messages()
#

def summarized_error_messages(error_messages, max_num_dups=10):
    """
    Utility function that does two things: 1) shortens error_messages list by removing all but a small number of similar
    messages. "similar" is determined by simply looking at the first 20 characters being equal. adds '...' if any were
     omitted, and 2) orders the messages according to the first item in each 2-tuple.

    :param error_messages: list of 2-tuples as returned by `json_io_dict_from_quantile_csv_file()`
    :param max_num_dups: integer maximum number of duplicated lines to return
    :return: shortened and sorted copy of the second tuple item in error_messages
    """
    error_messages = [_[1] for _ in sorted(error_messages)]
    error_key_to_max_messages = defaultdict(list)  # key: first N chars of any unique message
    for error_message in error_messages:
        error_key = error_message[:20]
        if (error_key not in error_key_to_max_messages) or (len(error_key_to_max_messages[error_key]) < max_num_dups):
            error_key_to_max_messages[error_key].append(error_message)

    # per https://stackoverflow.com/questions/952914/how-to-make-a-flat-list-out-of-list-of-lists :
    # return [item for sublist in error_key_to_max_messages.values() for item in sublist]

    error_messages = []  # return value
    for error_key, max_messages in error_key_to_max_messages.items():
        error_messages.extend(max_messages)
        # note that this adds '...' in the case of exactly max_num_dups, which may be misleading b/c it's max + 1 total
        if len(max_messages) == max_num_dups:
            error_messages.append(error_key + '...')
    return error_messages
