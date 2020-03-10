#
# csv_rows_from_json_io_dict()
#

CSV_HEADER = ['unit', 'target', 'unit', 'class', 'cat', 'family', 'lwr', 'param1', 'param2', 'param3', 'prob',
              'sample', 'value']


def csv_rows_from_json_io_dict(json_io_dict):
    """
    A utility that converts a "JSON IO dict" as returned by zoltar to a list of zoltar-specific CSV rows. The rows are
    an 'expanded' version of json_io_dict where bin-type classes result in multiple rows: BinCatDistribution,
    BinLwrDistribution, SampleDistribution, and SampleCatDistribution. The 'class' of each row is named according to
    forecast-repository.utils.forecast.PREDICTION_CLASS_TO_JSON_IO_DICT_CLASS. Column ordering is CSV_HEADER. Note that
    the csv is 'sparse': not every row uses all columns, and unused ones are empty. However, the first four columns are
    always non-empty, i.e., every prediction has them.

    :param json_io_dict: a "JSON IO dict" to load from. see docs for details. NB: this dict MUST have a valid "meta"
        section b/c we need ['meta']['targets'] for each target's 'unit' so we can figure out bin_end_notincl values.
    :return: a list of CSV rows including header - see CSV_HEADER
    """
    # todo merge w/cdc_csv_rows_from_json_io_dict()

    # do some initial validation
    if 'meta' not in json_io_dict:
        raise RuntimeError("no meta section found in json_io_dict")
    elif 'targets' not in json_io_dict['meta']:
        raise RuntimeError("no targets section found in json_io_dict meta section")
    elif 'predictions' not in json_io_dict:
        raise RuntimeError("no predictions section found in json_io_dict")

    rows = [CSV_HEADER]  # returned value. filled next
    target_name_to_dict = {target_dict['name']: target_dict for target_dict in json_io_dict['meta']['targets']}
    for prediction_dict in json_io_dict['predictions']:
        prediction_class = prediction_dict['class']
        if prediction_class not in ['BinCat', 'BinLwr', 'Binary', 'Named', 'Point', 'Sample', 'SampleCat']:
            raise RuntimeError(f"invalid prediction_dict class: {prediction_class}")

        target_name = prediction_dict['target']
        if target_name not in target_name_to_dict:
            raise RuntimeError(f"prediction_dict target not found in meta targets: {target_name}")

        location = prediction_dict['unit']
        target = prediction_dict['target']
        unit = target_name_to_dict[target_name]['unit']
        prediction = prediction_dict['prediction']
        # class-specific columns all default to empty:
        cat, family, lwr, param1, param2, param3, prob, sample, value = '', '', '', '', '', '', '', '', ''
        if prediction_class == 'BinCat':
            for cat, prob in zip(prediction['cat'], prediction['prob']):
                rows.append([location, target, unit, prediction_class, cat, family, lwr, param1, param2, param3, prob,
                             sample, value])
        elif prediction_class == 'BinLwr':
            for lwr, prob in zip(prediction['lwr'], prediction['prob']):
                rows.append([location, target, unit, prediction_class, cat, family, lwr, param1, param2, param3, prob,
                             sample, value])
        elif prediction_class == 'Binary':
            rows.append([location, target, unit, prediction_class, cat, family, lwr, param1, param2, param3,
                         prediction['prob'], sample, value])
        elif prediction_class == 'Named':
            rows.append([location, target, unit, prediction_class, cat, prediction['family'], lwr, prediction['param1'],
                         prediction['param2'], prediction['param3'], prob, sample, value])
        elif prediction_class == 'Point':
            rows.append([location, target, unit, prediction_class, cat, family, lwr, param1, param2, param3, prob,
                         sample, prediction['value']])
        elif prediction_class == 'Sample':
            for sample in prediction['sample']:
                rows.append([location, target, unit, prediction_class, cat, family, lwr, param1, param2, param3, prob,
                             sample, value])
        else:  # prediction_class == 'SampleCat'
            for cat, sample in zip(prediction['cat'], prediction['sample']):
                rows.append([location, target, unit, prediction_class, cat, family, lwr, param1, param2, param3, prob,
                             sample, value])
    return rows
