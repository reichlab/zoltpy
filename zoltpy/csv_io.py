#
# csv_rows_from_json_io_dict()
#

CSV_HEADER = ['unit', 'target', 'class', 'value', 'cat', 'prob', 'sample', 'quantile', 'family', 'param1', 'param2',
              'param3']
RETRACT_VAL = 'NULL'  # value in CSV files that represents a retraction


def csv_rows_from_json_io_dict(json_io_dict):
    """
    A utility that converts a "JSON IO dict" as returned by zoltar to a list rows in zoltar-specific CSV format. The
    columns are: 'unit', 'target', 'class', 'value', 'cat', 'prob', 'sample', 'quantile', 'family', 'param1', 'param2',
    'param3'. They are documented at https://docs.zoltardata.com/fileformats/#forecast-data-format-csv .

    notes:
    - retractions: represented in csv_rows by placing RETRACT_VAL in *all* pred_class-required column(s)

    :param json_io_dict: a "JSON IO dict" to load from. see docs for details. the "meta" section is ignored
    :return: a list of CSV rows including header - see CSV_HEADER
    """
    # do some initial validation
    if 'predictions' not in json_io_dict:
        raise RuntimeError("no predictions section found in json_io_dict")

    rows = [CSV_HEADER]  # return value. filled next
    for prediction_dict in json_io_dict['predictions']:
        prediction_class = prediction_dict['class']
        if prediction_class not in ['bin', 'named', 'point', 'sample', 'quantile', 'mean', 'median', 'mode']:
            raise RuntimeError(f"invalid prediction_dict class: {prediction_class}")

        is_bin_class = prediction_class == 'bin'
        is_named_class = prediction_class == 'named'
        is_point_class = prediction_class == 'point'
        is_sample_class = prediction_class == 'sample'
        is_mean_class = prediction_class == 'mean'
        is_median_class = prediction_class == 'median'
        is_mode_class = prediction_class == 'mode'
        unit = prediction_dict['unit']
        target = prediction_dict['target']
        prediction_data = prediction_dict['prediction']
        is_retraction = prediction_data is None

        # prediction_class-specific columns all default to empty:
        value, cat, prob, sample, quantile, family, param1, param2, param3 = '', '', '', '', '', '', '', '', ''
        if is_retraction:
            rows.append([unit, target, prediction_class, value, cat, prob, sample, quantile,
                         family, param1, param2, param3])
        elif is_bin_class:
            for cat, prob in zip(prediction_data['cat'], prediction_data['prob']):
                rows.append([unit, target, prediction_class, value, cat, prob, sample, quantile,
                             family, param1, param2, param3])
        elif is_named_class:
            rows.append([unit, target, prediction_class, value, cat, prob, sample, quantile,
                         prediction_data['family'],
                         prediction_data['param1'] if 'param1' in prediction_data else '',
                         prediction_data['param2'] if 'param2' in prediction_data else '',
                         prediction_data['param3'] if 'param3' in prediction_data else ''])
        elif is_point_class or is_mean_class or is_median_class or is_mode_class:
            rows.append([unit, target, prediction_class, prediction_data['value'], cat, prob, sample, quantile,
                         family, param1, param2, param3])
        elif is_sample_class:
            for sample in prediction_data['sample']:
                rows.append([unit, target, prediction_class, value, cat, prob, sample, quantile,
                             family, param1, param2, param3])
        else:  # PRED_CLASS_INT_TO_NAME[PredictionElement.QUANTILE_CLASS]
            for quantile, value in zip(prediction_data['quantile'], prediction_data['value']):
                rows.append([unit, target, prediction_class, value, cat, prob, sample, quantile,
                             family, param1, param2, param3])
    return rows
