import csv
from ast import literal_eval
from itertools import groupby


#
# This file defines utilities to convert to the CDC's CSV format from Zoltar's native JSON one. NB: this is currently a
# duplicate of https://github.com/reichlab/forecast-repository/blob/master/utils/cdc.py , which also contains the unit
# tests.
#


#
# globals
#

BINCAT_TARGET_NAMES = ['Season onset', 'Season peak week']
BINLWR_TARGET_NAMES = ['Season peak percentage', '1 wk ahead', '2 wk ahead', '3 wk ahead', '4 wk ahead']
CDC_POINT_NA_VALUE = 'NA'
CDC_POINT_ROW_TYPE = 'Point'
CDC_BIN_ROW_TYPE = 'Bin'
CDC_CSV_HEADER = ['location', 'target', 'type', 'unit', 'bin_start_incl', 'bin_end_notincl', 'value']
CDC_CSV_FILENAME_EXTENSION = 'cdc.csv'
TARGET_NAME_TO_UNIT = {'Season peak percentage': 'percent',
                       '1 wk ahead': 'percent',
                       '2 wk ahead': 'percent',
                       '3 wk ahead': 'percent',
                       '4 wk ahead': 'percent',
                       'Season onset': 'week',
                       'Season peak week': 'week'}


#
# cdc_csv_rows_from_json_io_dict()
#

def cdc_csv_rows_from_json_io_dict(json_io_dict):
    """
    A project-specific utility that converts a "JSON IO dict" as returned by zoltar into a list of CDC CSV rows,
    suitable for working with in memory or saving to a file.

    :param json_io_dict: a "JSON IO dict" to load from. see docs for details. The "meta" is ignored.
    :return: a list of CDC CSV rows as documented elsewhere. Does include a column header row. See CDC_CSV_HEADER.
    """
    # do some initial validation
    if 'predictions' not in json_io_dict:
        raise RuntimeError("no predictions section found in json_io_dict")

    rows = [CDC_CSV_HEADER]  # returned value. filled next
    for prediction_dict in json_io_dict['predictions']:
        prediction_class = prediction_dict['class']
        if prediction_class not in ['BinCat', 'BinLwr', 'Point']:
            raise RuntimeError(f"invalid prediction_dict class: {prediction_class}")

        target = prediction_dict['target']
        if target not in TARGET_NAME_TO_UNIT:
            raise RuntimeError(f"prediction_dict target not recognized: {target}. "
                               f"valid targets={list(TARGET_NAME_TO_UNIT.keys())}")

        location = prediction_dict['location']
        row_type = CDC_POINT_ROW_TYPE if prediction_class == 'Point' else CDC_BIN_ROW_TYPE
        unit = TARGET_NAME_TO_UNIT[target]
        prediction = prediction_dict['prediction']
        if row_type == CDC_POINT_ROW_TYPE:  # output the single point row
            rows.append([location, target, row_type, unit, CDC_POINT_NA_VALUE, CDC_POINT_NA_VALUE, prediction['value']])
        elif prediction_class == 'BinCat':  # 'BinCat' CDC_BIN_ROW_TYPE -> output multiple bin rows
            # BinCat targets: unit='week', target='Season onset' or 'Season peak week'
            for cat, prob in zip(prediction['cat'], prediction['prob']):
                rows.append([location, target, row_type, unit, cat, _recode_cat_bin_end_notincl(cat), prob])
        else:  # prediction_class == 'BinLwr' CDC_BIN_ROW_TYPE -> output multiple bin rows
            # BinLwr targets: unit='percent', target='1 wk ahead' ... '4 wk ahead', or 'Season peak percentage'
            for lwr, prob in zip(prediction['lwr'], prediction['prob']):
                rows.append([location, target, row_type, unit, lwr, 100 if lwr == 13 else lwr + 0.1, prob])
    return rows


def _recode_cat_bin_end_notincl(cat):  # from predx: recode_flusight_bin_end_notincl()
    return {
        '40': '41',
        '41': '42',
        '42': '43',
        '43': '44',
        '44': '45',
        '45': '46',
        '46': '47',
        '47': '48',
        '48': '49',
        '49': '50',
        '50': '51',
        '51': '52',
        '52': '53',
        '1': '2',
        '2': '3',
        '3': '4',
        '4': '5',
        '5': '6',
        '6': '7',
        '7': '8',
        '8': '9',
        '9': '10',
        '10': '11',
        '11': '12',
        '12': '13',
        '13': '14',
        '14': '15',
        '15': '16',
        '16': '17',
        '17': '18',
        '18': '19',
        '19': '20',
        '20': '21',
        'none': 'none'}[cat.lower()]


#
# json_io_dict_from_cdc_csv_file()
#

def json_io_dict_from_cdc_csv_file(cdc_csv_file_fp):
    """
    Utility that extracts the three types of predictions found in CDC CSV files (PointPredictions, BinLwrDistributions,
    and BinCatDistributions), returning them as a "JSON IO dict" suitable for loading into the database (see
    load_predictions_from_json_io_dict()). Note that the returned dict's "meta" section is empty.

    :param cdc_csv_file_fp: an open cdc csv file-like object. the CDC CSV file format is documented at
        https://predict.cdc.gov/api/v1/attachments/flusight/flu_challenge_2016-17_update.docx
    :return a "JSON IO dict" (aka 'json_io_dict' by callers) that contains the three types of predictions. see docs for
        details
    """
    return {'meta': {},
            'predictions': _prediction_dicts_for_csv_rows(_cleaned_rows_from_cdc_csv_file(cdc_csv_file_fp))}


def _cleaned_rows_from_cdc_csv_file(cdc_csv_file_fp):
    """
    Loads the rows from cdc_csv_file_fp, cleans them, and then returns them as a list. Does some basic validation,
    but does not check locations and targets. This is b/c Locations and Targets might not yet exist (if they're
    dynamically created by this method's callers). Does *not* skip bin rows where the value is 0.

    :param cdc_csv_file_fp: the *.cdc.csv data file to load
    :return: a list of rows: location_name, target_name, is_point_row, bin_start_incl, bin_end_notincl, value
    """
    csv_reader = csv.reader(cdc_csv_file_fp, delimiter=',')

    # validate header. must be 7 columns (or 8 with the last one being '') matching
    try:
        orig_header = next(csv_reader)
    except StopIteration:  # a kind of Exception, so much come first
        raise RuntimeError("empty file.")
    except Exception as exc:
        raise RuntimeError("error reading from cdc_csv_file_fp={}. exc={}".format(cdc_csv_file_fp, exc))

    header = orig_header
    if (len(header) == 8) and (header[7] == ''):
        header = header[:7]
    header = [h.lower() for h in [i.replace('"', '') for i in header]]
    if header != CDC_CSV_HEADER:
        raise RuntimeError("invalid header: {}".format(', '.join(orig_header)))

    # collect the rows. first we load them all into memory (processing and validating them as we go)
    rows = []
    for row in csv_reader:  # might have 7 or 8 columns, depending on whether there's a trailing ',' in file
        if (len(row) == 8) and (row[7] == ''):
            row = row[:7]

        if len(row) != 7:
            raise RuntimeError("Invalid row (wasn't 7 columns): {!r}".format(row))

        location_name, target_name, row_type, unit, bin_start_incl, bin_end_notincl, value = row  # unit ignored

        # validate row_type
        row_type = row_type.lower()
        if (row_type != CDC_POINT_ROW_TYPE.lower()) and (row_type != CDC_BIN_ROW_TYPE.lower()):
            raise RuntimeError("row_type was neither '{}' nor '{}': "
                               .format(CDC_POINT_ROW_TYPE, CDC_BIN_ROW_TYPE))
        is_point_row = (row_type == CDC_POINT_ROW_TYPE.lower())

        # if blank cell, transform to NA
        bin_start_incl = 'NA' if bin_start_incl == '' else bin_start_incl
        bin_end_notincl = 'NA' if bin_end_notincl == '' else bin_end_notincl
        value = 'NA' if value == '' else value

        # use parse_value() to handle non-numeric cases like 'NA' and 'none'
        bin_start_incl = parse_value(bin_start_incl)
        bin_end_notincl = parse_value(bin_end_notincl)
        value = parse_value(value)
        rows.append([location_name, target_name, is_point_row, bin_start_incl, bin_end_notincl, value])

    return rows


def _prediction_dicts_for_csv_rows(rows):
    """
    json_io_dict_from_cdc_csv_file() helper that returns a list of prediction dicts for the 'predictions' section of the
    exported json. Each dict corresponds to either a PointPrediction, BinLwrDistribution, or BinCatDistribution
    depending on each row in rows. See predictions-example.json for an example.

    :param rows: as returned by _cleaned_rows_from_cdc_csv_file():
        location_name, target_name, is_point_row, bin_start_incl, bin_end_notincl, value
    """
    prediction_dicts = []  # return value
    rows.sort(key=lambda _: (_[0], _[1], _[2]))  # sorted for groupby()
    for (location_name, target_name, is_point_row), bin_start_end_val_grouper in \
            groupby(rows, key=lambda _: (_[0], _[1], _[2])):
        point_values = []  # NB: should only be one point row, but collect all (but don't validate here)
        bincat_cats, bincat_probs = [], []
        binlwr_lwrs, binlwr_probs = [], []
        for _, _, _, bin_start_incl, bin_end_notincl, value in bin_start_end_val_grouper:
            try:
                if is_point_row:
                    # NB: point comes in as a number (see parse_value() below), but should be a string
                    # for Targets whose point_value_type is Target.POINT_TEXT. lower() handles 'None' -> 'none'
                    point_value = str(value).lower() if target_name in BINCAT_TARGET_NAMES else value
                    point_values.append(point_value)
                elif target_name in BINCAT_TARGET_NAMES:
                    bin_start_incl_value = str(bin_start_incl) if target_name in BINCAT_TARGET_NAMES else bin_start_incl
                    bincat_cats.append(bin_start_incl_value.lower())  # lower() ""
                    bincat_probs.append(float(value))
                elif target_name in BINLWR_TARGET_NAMES:
                    binlwr_lwrs.append(float(bin_start_incl))
                    binlwr_probs.append(float(value))
                else:
                    raise RuntimeError(
                        f"unexpected bin target_name. target_name={target_name!r}, "
                        f"BINLWR_TARGET_NAMES={BINLWR_TARGET_NAMES}, "
                        f"BINCAT_TARGET_NAMES={BINCAT_TARGET_NAMES}")
            except ValueError as ve:
                row = [location_name, target_name, is_point_row, bin_start_incl, bin_end_notincl, value]
                raise RuntimeError(f"could not coerce either bin_start_incl or value to float. bin_start_incl="
                                   f"{bin_start_incl}, value={value}, row={row}, error={ve}")

        # add the actual prediction dicts
        if bincat_cats:
            prediction_dicts.append({"unit": location_name,
                                     "target": target_name,
                                     "class": "BinCat",
                                     "prediction": {
                                         "cat": bincat_cats,
                                         "prob": bincat_probs}})
        if binlwr_lwrs:
            prediction_dicts.append({"unit": location_name,
                                     "target": target_name,
                                     "class": "BinLwr",
                                     "prediction": {
                                         "lwr": binlwr_lwrs,
                                         "prob": binlwr_probs}})
        if point_values:
            for point_value in point_values:
                prediction_dicts.append({"unit": location_name,
                                         "target": target_name,
                                         'class': 'Point',
                                         'prediction': {
                                             'value': point_value}})
    return prediction_dicts


def parse_value(value):
    """
    Parses a value numerically as smartly as possible, in order: float, int, None. o/w is an error
    """
    # https://stackoverflow.com/questions/34425583/how-to-check-if-string-is-int-or-float-in-python-2-7
    try:
        return literal_eval(value)
    except ValueError:
        return None
