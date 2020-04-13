import csv
import datetime
from itertools import groupby

from zoltpy.cdc import CDC_POINT_ROW_TYPE, parse_value, YYYY_MM_DD_DATE_FORMAT


QUANTILE_CSV_HEADER = ['location', 'target', 'type', 'quantile', 'value']  # row type: 'point' or 'quantile'


def json_io_dict_from_quantile_csv_file(csv_fp):
    """
    Utility that extracts the two types of predictions found in quantile CSV files (PointPredictions and
    QuantileDistributions), returning them as a "JSON IO dict" suitable for loading into the database (see
    `load_predictions_from_json_io_dict()`). Note that the returned dict's "meta" section is empty.

    :param csv_fp: an open quantile csv file-like object. the quantile CSV file format is documented at
        https://docs.zoltardata.com/
    :return a "JSON IO dict" (aka 'json_io_dict' by callers) that contains the three types of predictions. see docs for
        details
    """
    # load and validate the rows
    csv_reader = csv.reader(csv_fp, delimiter=',')
    header = next(csv_reader)
    if header != QUANTILE_CSV_HEADER:
        raise RuntimeError(f"invalid header. header={header!r}, expected header={QUANTILE_CSV_HEADER!r}")

    rows = []  # list of parsed and validated rows. filled next
    for row in csv_reader:  # might have 7 or 8 columns, depending on whether there's a trailing ',' in file
        if len(row) != len(QUANTILE_CSV_HEADER):
            raise RuntimeError(f"Invalid number of items in row. expected: {len(QUANTILE_CSV_HEADER)} "
                               f"but got {len(row)}. row={row!r}")

        location_name, target_name, row_type, quantile, value = row
        row_type = row_type.lower()
        is_point_row = (row_type == CDC_POINT_ROW_TYPE.lower())
        quantile = parse_value(quantile)
        value = parse_value(value)
        # convert parsed date back into string suitable for JSON
        if isinstance(value, datetime.date):
            value = value.strftime(YYYY_MM_DD_DATE_FORMAT)
        rows.append([location_name, target_name, is_point_row, quantile, value])

    # collect point and quantile values for each row and then add the actual prediction dicts. each point row has its
    # own dict, but quantile rows are grouped into one dict
    prediction_dicts = []  # the 'predictions' section of the returned value. filled next
    rows.sort(key=lambda _: (_[0], _[1], _[2]))  # sorted for groupby()
    for (location_name, target_name, is_point_row), quantile_val_grouper in \
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
            prediction_dicts.append({"unit": location_name,
                                     "target": target_name,
                                     'class': 'point',  # PointPrediction
                                     'prediction': {
                                         'value': point_value}})
        if quant_quantiles:
            prediction_dicts.append({"unit": location_name,
                                     "target": target_name,
                                     'class': 'quantile',  # QuantileDistribution
                                     'prediction': {
                                         "quantile": quant_quantiles,
                                         "value": quant_values}})

    # done
    return {'meta': {}, 'predictions': prediction_dicts}
