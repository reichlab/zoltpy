import csv
import datetime
from itertools import groupby

from zoltpy.cdc import CDC_POINT_ROW_TYPE, parse_value, YYYY_MM_DD_DATE_FORMAT


REQUIRED_COLUMNS = ['location', 'target', 'type', 'quantile', 'value']


def json_io_dict_from_quantile_csv_file(csv_fp):
    """
    Utility that validates and extracts the two types of predictions found in quantile CSV files (PointPredictions and
    QuantileDistributions), returning them as a "JSON IO dict" suitable for loading into the database (see
    `load_predictions_from_json_io_dict()`). Note that the returned dict's "meta" section is empty. This function is
    flexible with respect to the inputted column contents and order: It allows the required columns to be in any
    position, and it ignores all other columns. The required columns are:

    - `target`: a unique id for the target
    - `location`: a FIPS code: https://en.wikipedia.org/wiki/Federal_Information_Processing_Standard_state_code -
        that is, '01' through '95', and 'US'
    - `type`: one of either `point` or `quantile`
    - `quantile`: a value between 0 and 1 (inclusive), representing the quantile displayed in this row. if
        `type=="point"` then `NULL`.
    - `value`: a numeric value representing the value of the cumulative distribution function evaluated at the specified
        `quantile`

    :param csv_fp: an open quantile csv file-like object. the quantile CSV file format is documented at
        https://docs.zoltardata.com/
    :return a "JSON IO dict" (aka 'json_io_dict' by callers) that contains the two types of predictions. see
        https://docs.zoltardata.com/ for details
    """
    # load and validate the rows
    csv_reader = csv.reader(csv_fp, delimiter=',')
    header = next(csv_reader)
    location_idx, target_idx, row_type_idx, quantile_idx, value_idx = _validate_header(header)

    rows = []  # list of parsed and validated rows. filled next
    for row in csv_reader:  # either 5 or 6 columns
        if len(row) != len(header):
            raise RuntimeError(f"invalid number of items in row. expected: {len(header)} but got {len(row)}. row={row}")

        target_name, location_fips, row_type, quantile, value = \
            row[target_idx], row[location_idx], row[row_type_idx], row[quantile_idx], row[value_idx]

        # validate location_fips - https://en.wikipedia.org/wiki/Federal_Information_Processing_Standard_state_code
        # - '01' through '95', and 'US'
        if len(location_fips) != 2:
            raise RuntimeError(f"invalid FIPS: not two characters: {location_fips!r}")

        if location_fips != 'US':  # must be a number b/w 1 and 95 inclusive
            FIPS_MIN = 1
            FIPS_MAX = 95
            try:
                fips_int = int(location_fips)
                if (fips_int < FIPS_MIN) or (fips_int > FIPS_MAX):
                    raise RuntimeError(f"invalid FIPS: two character int but out of range {FIPS_MIN}-{FIPS_MAX}: "
                                       f"{location_fips!r}")
            except ValueError as ve:
                raise RuntimeError(f"invalid FIPS: two characters but not an int: {location_fips!r}")

        row_type = row_type.lower()
        is_point_row = (row_type == CDC_POINT_ROW_TYPE.lower())
        quantile = parse_value(quantile)
        value = parse_value(value)
        # convert parsed date back into string suitable for JSON
        if isinstance(value, datetime.date):
            value = value.strftime(YYYY_MM_DD_DATE_FORMAT)
        rows.append([target_name, location_fips, is_point_row, quantile, value])

    # collect point and quantile values for each row and then add the actual prediction dicts. each point row has its
    # own dict, but quantile rows are grouped into one dict
    prediction_dicts = []  # the 'predictions' section of the returned value. filled next
    rows.sort(key=lambda _: (_[0], _[1], _[2]))  # sorted for groupby()
    for (target_name, location_fips, is_point_row), quantile_val_grouper in \
            groupby(rows, key=lambda _: (_[0], _[1], _[2])):
        # fill values for points and bins. NB: should only be one point row per location/target pair
        point_values = []  # should be at most one, but use a list to help validate
        quant_quantiles, quant_values = [], []
        for _, _, _, quantile, value in quantile_val_grouper:
            if is_point_row and not point_values:
                point_values.append(value)  # quantile is NA
            elif is_point_row:
                raise RuntimeError(f"found more than one point value for the same target_name, location_fips. "
                                   f"target_name={target_name!r}, location_fips={location_fips!r}, "
                                   f"this point value={value}, previous point_value={point_values[0]}")
            else:
                quant_quantiles.append(quantile)
                quant_values.append(value)

        # add the actual prediction dicts
        if point_values:
            if len(point_values) > 1:
                raise RuntimeError(f"len(point_values) > 1: {point_values}")

            point_value = point_values[0]
            prediction_dicts.append({"unit": location_fips,
                                     "target": target_name,
                                     'class': 'point',  # PointPrediction
                                     'prediction': {
                                         'value': point_value}})
        if quant_quantiles:
            prediction_dicts.append({"unit": location_fips,
                                     "target": target_name,
                                     'class': 'quantile',  # QuantileDistribution
                                     'prediction': {
                                         "quantile": quant_quantiles,
                                         "value": quant_values}})

    # done
    return {'meta': {}, 'predictions': prediction_dicts}


def _validate_header(header):
    """
    `json_io_dict_from_quantile_csv_file()` helper function.

    :param header: first rows from the csv file
    :return: location_idx, target_idx, row_type_idx, quantile_idx, value_idx
    """
    counts = [header.count(required_column) == 1 for required_column in REQUIRED_COLUMNS]
    if not all(counts):
        raise RuntimeError(f"invalid header. did not contain the required columns. header={header}, "
                           f"REQUIRED_COLUMNS={REQUIRED_COLUMNS}")

    return [header.index(required_column) for required_column in REQUIRED_COLUMNS]
