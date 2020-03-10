import csv
from ast import literal_eval
from itertools import groupby
import pymmwr
import datetime

#
# date formats
#

YYYY_MM_DD_DATE_FORMAT = '%Y-%m-%d'  # e.g., '2017-01-17'


#
# This file defines utilities to convert to the CDC's CSV format from Zoltar's native JSON one. NB: this is currently a
# duplicate of https://github.com/reichlab/forecast-repository/blob/master/utils/cdc.py , which also contains the unit
# tests.
#


#
# *.cdc.csv file variables
#

CDC_POINT_ROW_TYPE = 'Point'
CDC_BIN_ROW_TYPE = 'Bin'
CDC_CSV_HEADER = ['location', 'target', 'type', 'unit', 'bin_start_incl', 'bin_end_notincl', 'value']

# This number is the internal reichlab standard: "We used week 30. I don't think this is a standardized concept outside
# of our lab though. We use separate concepts for a "season" and a "year". So, e.g. the "2016/2017 season" starts with
# EW30-2016 and ends with EW29-2017."
SEASON_START_EW_NUMBER = 30


PointPrediction = 'point'
BinDistribution = 'bin'

#
# cdc_csv_rows_from_json_io_dict()
#

TARGET_NAME_TO_UNIT = {'Season peak percentage': 'percent',
                       '1 wk ahead': 'percent',
                       '2 wk ahead': 'percent',
                       '3 wk ahead': 'percent',
                       '4 wk ahead': 'percent',
                       'Season onset': 'week',
                       'Season peak week': 'week'}

CDC_POINT_NA_VALUE = 'NA'


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

        location = prediction_dict['unit']
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

def json_io_dict_from_cdc_csv_file(season_start_year, cdc_csv_file_fp):
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
            'predictions': _prediction_dicts_for_csv_rows(season_start_year,
                                                          _cleaned_rows_from_cdc_csv_file(cdc_csv_file_fp))}


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


def _prediction_dicts_for_csv_rows(season_start_year, rows):
    """
    json_io_dict_from_cdc_csv_file() helper that returns a list of prediction dicts for the 'predictions' section of the
    exported json. Each dict corresponds to either a PointPrediction or BinDistribution depending on each row in rows.
    Uses season_start_year to convert EWs to YYYY_MM_DD_DATE_FORMAT dates.

    Recall the seven cdc-project.json targets and their types:
    -------------------------+-------------------------------+-----------+-----------+---------------------
    Target name              | target_type                   | unit      | data_type | step_ahead_increment
    -------------------------+-------------------------------+-----------+-----------+---------------------
    "Season onset"           | Target.NOMINAL_TARGET_TYPE    | "week"    | date      | n/a
    "Season peak week"       | Target.DATE_TARGET_TYPE       | "week"    | text      | n/a
    "Season peak percentage" | Target.CONTINUOUS_TARGET_TYPE | "percent" | float     | n/a
    "1 wk ahead"             | Target.CONTINUOUS_TARGET_TYPE | "percent" | float     | 1
    "2 wk ahead"             | ""                            | ""        | ""        | 2
    "3 wk ahead"             | ""                            | ""        | ""        | 3
    "4 wk ahead"             | ""                            | ""        | ""        | 4
    -------------------------+-------------------------------+-----------+-----------+---------------------

    Note that the "Season onset" target is nominal and not date. This is due to how the CDC decided to represent the
    case when predicting no season onset, i.e., the threshold is not exceeded. This is done via a "none" bin where
    both Bin_start_incl and Bin_end_notincl are the strings "none" and not an EW week number. Thus, we have to store
    all bin starts as strings and not dates. At one point the lab was going to represent this case by splitting the
    "Season onset" target into two: "season_onset_binary" (a Target.BINARY that indicates whether there is an onset or
    not) and "season_onset_date" (a Target.DATE_TARGET_TYPE that is the onset date if "season_onset_binary" is true).
    But we dropped that idea and stayed with the original single nominal target.

    :param season_start_year
    :param rows: as returned by _cleaned_rows_from_cdc_csv_file():
        location_name, target_name, is_point_row, bin_start_incl, bin_end_notincl, value
    :return: a list of PointPrediction or BinDistribution prediction dicts
    """
    prediction_dicts = []  # return value
    rows.sort(key=lambda _: (_[0], _[1], _[2]))  # sorted for groupby()
    for (location_name, target_name, is_point_row), bin_start_end_val_grouper in \
            groupby(rows, key=lambda _: (_[0], _[1], _[2])):
        # NB: should only be one point row per location/target pair, but collect all (i.e., don't validate here):
        point_values = []
        bin_cats, bin_probs = [], []
        for _, _, _, bin_start_incl, bin_end_notincl, value in bin_start_end_val_grouper:  # all 3 are numbers or None
            try:
                if is_point_row:  # save value in point_values, possibly converted based on target
                    if target_name == 'Season onset':  # nominal target. value: None or an EW Monday date
                        if value is None:
                            value = 'none'
                        else:  # value is an EW week number (float)
                            # note that value may be a fraction (e.g., 50.0012056690978, 4.96302456525203), so we round
                            # the EW number to get an int, but this could cause boundary issues where the value is
                            # invalid, either:
                            #   1) < 1 (so use last EW in season_start_year), or:
                            #   2) > the last EW in season_start_year (so use EW01 of season_start_year + 1)
                            ew_week = round(value)
                            if ew_week < 1:
                                ew_week = pymmwr.mmwr_weeks_in_year(season_start_year)  # wrap back to previous EW
                            elif ew_week > pymmwr.mmwr_weeks_in_year(season_start_year):  # wrap forward to next EW
                                ew_week = 1
                            monday_date = monday_date_from_ew_and_season_start_year(ew_week, season_start_year)
                            value = monday_date.strftime(YYYY_MM_DD_DATE_FORMAT)
                    elif target_name in ['1_biweek_ahead', '2_biweek_ahead', '3_biweek_ahead', '4_biweek_ahead',
                                         '5_biweek_ahead']:  # thai
                        value = round(value)  # some point predictions are floats
                    elif value is None:
                        raise RuntimeError(f"None point values are only valid for 'Season onset' targets. "
                                           f"target_name={target_name}")
                    elif target_name == 'Season peak week':  # date target. value: an EW Monday date
                        # same 'wrapping' logic as above to handle rounding boundaries
                        ew_week = round(value)
                        if ew_week < 1:
                            ew_week = pymmwr.mmwr_weeks_in_year(season_start_year)  # wrap back to previous EW
                        elif ew_week > pymmwr.mmwr_weeks_in_year(season_start_year):  # wrap forward to next EW
                            ew_week = 1
                        monday_date = monday_date_from_ew_and_season_start_year(ew_week, season_start_year)
                        value = monday_date.strftime(YYYY_MM_DD_DATE_FORMAT)
                    point_values.append(value)
                # is_bin_row:
                elif target_name == 'Season onset':  # nominal target. start: None or an EW Monday date
                    if (bin_start_incl is None) and (bin_end_notincl is None):  # "none" bin (probability of no onset)
                        bin_cat = 'none'  # convert back from None to original 'none' input
                    elif (bin_start_incl is not None) and (bin_end_notincl is not None):  # regular (non-"none") bin
                        monday_date = monday_date_from_ew_and_season_start_year(bin_start_incl, season_start_year)
                        bin_cat = monday_date.strftime(YYYY_MM_DD_DATE_FORMAT)
                    else:
                        raise RuntimeError(f"got 'Season onset' row but not both start and end were None. "
                                           f"bin_start_incl={bin_start_incl}, bin_end_notincl={bin_end_notincl}")
                    bin_cats.append(bin_cat)
                    bin_probs.append(value)
                elif (bin_start_incl is None) or (bin_end_notincl is None):
                    raise RuntimeError(f"None bins are only valid for 'Season onset' targets. "
                                       f"target_name={target_name}. bin_start_incl, bin_end_notincl: "
                                       f"{bin_start_incl}, {bin_end_notincl}")
                elif target_name == 'Season peak week':  # date target. start: an EW Monday date
                    monday_date = monday_date_from_ew_and_season_start_year(bin_start_incl, season_start_year)
                    bin_cats.append(monday_date.strftime(YYYY_MM_DD_DATE_FORMAT))
                    bin_probs.append(value)
                elif target_name in ['Season peak percentage', '1 wk ahead', '2 wk ahead', '3 wk ahead', '4 wk ahead',
                                     '1_biweek_ahead', '2_biweek_ahead', '3_biweek_ahead', '4_biweek_ahead',  # thai
                                     '5_biweek_ahead']:
                    bin_cats.append(bin_start_incl)
                    bin_probs.append(value)
                else:
                    raise RuntimeError(f"invalid target_name: {target_name!r}")
            except ValueError as ve:
                row = [location_name, target_name, is_point_row, bin_start_incl, bin_end_notincl, value]
                raise RuntimeError(f"could not coerce either bin_start_incl or value to float. bin_start_incl="
                                   f"{bin_start_incl}, value={value}, row={row}, error={ve}")

        # add the actual prediction dicts
        if point_values:
            if len(point_values) > 1:
                raise RuntimeError(f"len(point_values) > 1: {point_values}")

            point_value = point_values[0]
            prediction_dicts.append({"unit": location_name,
                                     "target": target_name,
                                     'class': PointPrediction,
                                     'prediction': {
                                         'value': point_value}})
        if bin_cats:
            prediction_dicts.append({"unit": location_name,
                                     "target": target_name,
                                     'class': BinDistribution,
                                     'prediction': {
                                         "cat": bin_cats,
                                         "prob": bin_probs}})
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


#
# ---- CDC EW utilities ----
#

def monday_date_from_ew_and_season_start_year(ew_week, season_start_year):
    """
    :param ew_week: an epi week from within a cdc csv forecast file. e.g., 1, 30, 52
    :param season_start_year
    :return: a datetime.date that is the Monday of the EW corresponding to the args
    """
    if ew_week < SEASON_START_EW_NUMBER:
        sunday_date = pymmwr.mmwr_week_to_date(season_start_year + 1, ew_week)
    else:
        sunday_date = pymmwr.mmwr_week_to_date(season_start_year, ew_week)
    return sunday_date + datetime.timedelta(days=1)
