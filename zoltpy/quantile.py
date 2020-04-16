import csv
import datetime
from itertools import groupby

from zoltpy.cdc import CDC_POINT_ROW_TYPE, parse_value, YYYY_MM_DD_DATE_FORMAT


QUANTILE_CSV_HEADER = ['target', 'location', 'location_name', 'type', 'quantile', 'value']  # `location_name`: optional


def json_io_dict_from_quantile_csv_file(csv_fp):
    """
    Utility that extracts the two types of predictions found in quantile CSV files (PointPredictions and
    QuantileDistributions), returning them as a "JSON IO dict" suitable for loading into the database (see
    `load_predictions_from_json_io_dict()`). Note that the returned dict's "meta" section is empty.

    Summary of the CSV format's 5 or 6 columns:
    - `target`: a unique id for the target
    - `location`: a unique id for the location (we have standardized to FIPS codes)
    - `location_name`: (optional) if desired to have a human-readable name for the location, this column may be
        specified. Note that the `location` column will be considered to be authoritative and for programmatic reading
        and importing of data, this column will be ignored.
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
    short_header = QUANTILE_CSV_HEADER[:2] + QUANTILE_CSV_HEADER[3:]  # no `location_name`
    if (len(header) != 5) and (len(header) != 6):
        raise RuntimeError(f"invalid header. number of columns was not 5 or 6: {header!r}")
    elif (len(header) == 5) and (header != short_header):
        raise RuntimeError(f"invalid header. had five columns, but not the expected ones. header={header!r}, "
                           f"expected={short_header}")
    elif header != QUANTILE_CSV_HEADER:  # len(header) == 6
        raise RuntimeError(f"invalid header. had six columns, but not the expected ones. header={header!r}, "
                           f"expected={QUANTILE_CSV_HEADER}")

    is_short_header = len(header) == 5
    rows = []  # list of parsed and validated rows. filled next
    for row in csv_reader:  # either 5 or 6 columns
        if len(row) != len(header):
            raise RuntimeError(f"invalid number of items in row. expected: {len(header)} but got {len(row)}. "
                               f"row={row!r}")

        if is_short_header:
            target_name, location_fips, row_type, quantile, value = row  # no location_name
        else:
            target_name, location_fips, _, row_type, quantile, value = row  # skip location_name

        # validate location_fips (state-level) - https://transition.fcc.gov/oet/info/maps/census/fips/fips.txt
        # - '01' through '56', and 'US'
        if len(location_fips) != 2:
            raise RuntimeError(f"invalid FIPS: not two characters: {location_fips!r}")

        if location_fips != 'US':  # must be a number b/w 1 and 56 inclusive
            try:
                fips_int = int(location_fips)
                if (fips_int < 1) or (fips_int > 56):
                    raise RuntimeError(f"invalid FIPS: two characters int but out of range 1-56: {location_fips!r}")
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
        # fill values for points and bins. NB: should only be one point row per location/target pair, but collect all
        # (i.e., don't validate here)
        point_values = []
        quant_quantiles, quant_values = [], []
        for _, _, _, quantile, value in quantile_val_grouper:
            if is_point_row:
                point_values.append(value)  # quantile is NA
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
