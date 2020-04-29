import json
from unittest import TestCase
from unittest.mock import patch

from zoltpy.covid19 import COVID19_TARGET_NAMES, covid19_row_validator
from zoltpy.quantile_io import json_io_dict_from_quantile_csv_file, _validate_header, REQUIRED_COLUMNS, \
    quantile_csv_rows_from_json_io_dict
from zoltpy.util import dataframe_from_json_io_dict


class QuantileIOTestCase(TestCase):
    """
    """


    def test_optional_additional_required_column_names(self):
        addl_req_cols = ['forecast_date', 'target_end_date']

        # target, location, location_name, type, quantile,value:
        with open('tests/quantile-predictions.csv') as quantile_fp:
            _, error_messages = \
                json_io_dict_from_quantile_csv_file(quantile_fp, ['1 wk ahead cum death', '1 day ahead cum death'],
                                                    addl_req_cols=addl_req_cols)
            self.assertEqual(1, len(error_messages))
            self.assertIn('invalid header. did not contain the required columns', error_messages[0])

        # forecast_date, target, target_end_date, location, location_name, type, quantile, value:
        with open('tests/covid19-data-processed-examples/2020-04-15-Geneva-DeterministicGrowth.csv') as quantile_fp:
            try:
                json_io_dict_from_quantile_csv_file(quantile_fp, ['1 wk ahead cum death', '1 day ahead cum death'],
                                                    addl_req_cols=addl_req_cols)
            except Exception as ex:
                self.fail(f"unexpected exception: {ex}")


    def test_json_io_dict_from_quantile_csv_file_calls_validate_header(self):
        column_index_dict = {'target': 0, 'location': 1, 'location_name': 2, 'type': 3, 'quantile': 4, 'value': 5}
        with patch('zoltpy.quantile_io._validate_header', return_value=column_index_dict) as mock:
            with open('tests/quantile-predictions.csv') as quantile_fp:
                json_io_dict_from_quantile_csv_file(quantile_fp, ['1 wk ahead cum death', '1 day ahead cum death'])
                self.assertEqual(1, mock.call_count)


    def test_json_io_dict_from_quantile_csv_file_small_tolerance(self):
        with open('tests/covid19-data-processed-examples/2020-04-20-YYG-ParamSearch-small.csv') as quantile_fp:
            _, error_messages = json_io_dict_from_quantile_csv_file(quantile_fp, COVID19_TARGET_NAMES)
            self.assertEqual(0, len(error_messages))


    def test_validate_header(self):
        # test various valid headers
        for columns_exp_idxs in [
            (REQUIRED_COLUMNS,  # canonical order: 'location', 'target', 'type', 'quantile', 'value'
             {'location': 0, 'target': 1, 'type': 2, 'quantile': 3, 'value': 4}),
            (['location', 'type', 'target', 'quantile', 'value'],  # different order
             {'location': 0, 'type': 1, 'target': 2, 'quantile': 3, 'value': 4}),
            (['location', 'type', 'target', 'quantile', 'foo', 'value'],  # extra column
             {'location': 0, 'type': 1, 'target': 2, 'quantile': 3, 'foo': 4, 'value': 5}),
            (['bar', 'location', 'type', 'target', 'quantile', 'foo', 'value'],  # extra columns
             {'bar': 0, 'location': 1, 'type': 2, 'target': 3, 'quantile': 4, 'foo': 5, 'value': 6})]:
            try:
                act_column_index_dict = _validate_header(columns_exp_idxs[0], [])
                self.assertEqual(columns_exp_idxs[1], act_column_index_dict)
            except Exception as ex:
                self.fail(f"unexpected exception: {ex}")

        # test removing each required_column one at a time
        for required_column in REQUIRED_COLUMNS:
            columns_exp_idxs = list(REQUIRED_COLUMNS)  # copy
            req_col_idx = columns_exp_idxs.index(required_column)
            del columns_exp_idxs[req_col_idx]
            with self.assertRaises(RuntimeError) as context:
                _validate_header(columns_exp_idxs, [])
            self.assertIn("invalid header. did not contain the required columns", str(context.exception))

        # test duplicate required column
        with self.assertRaises(RuntimeError) as context:
            _validate_header(list(REQUIRED_COLUMNS) + ['type'], [])
        self.assertIn("invalid header. did not contain the required columns", str(context.exception))


    def test_json_io_dict_from_quantile_csv_file_ok(self):
        for quantile_file in ['tests/quantile-predictions-5-col.csv', 'tests/quantile-predictions.csv']:
            with open(quantile_file) as quantile_fp, \
                    open('tests/quantile-predictions.json') as exp_json_fp:
                exp_json_io_dict = json.load(exp_json_fp)
                act_json_io_dict, _ = \
                    json_io_dict_from_quantile_csv_file(quantile_fp, ['1 wk ahead cum death', '1 day ahead cum death'])
                exp_json_io_dict['predictions'].sort(key=lambda _: (_['unit'], _['target'], _['class']))
                act_json_io_dict['predictions'].sort(key=lambda _: (_['unit'], _['target'], _['class']))
                self.assertEqual(exp_json_io_dict, act_json_io_dict)


    def test_other_ok_quantile_files(self):
        with open('tests/quantiles-CU-60contact.csv') as quantile_fp:
            _, error_messages = json_io_dict_from_quantile_csv_file(quantile_fp, COVID19_TARGET_NAMES)
            self.assertEqual(0, len(error_messages))


    def test_error_messages_actual_files_no_errors(self):
        # test large-ish actual files
        ok_quantile_files = [
            # '2020-04-12-IHME-CurveFit.csv',  # errors. tested below
            # '2020-04-15-Geneva-DeterministicGrowth.csv',  # ""
            '2020-04-13-COVIDhub-ensemble.csv',
            '2020-04-13-Imperial-ensemble1.csv',
            '2020-04-13-MOBS_NEU-GLEAM_COVID.csv']
        for quantile_file in ok_quantile_files:
            with open('tests/covid19-data-processed-examples/' + quantile_file) as quantile_fp:
                _, error_messages = json_io_dict_from_quantile_csv_file(quantile_fp, COVID19_TARGET_NAMES)
                self.assertEqual(0, len(error_messages))


    def test_error_messages_actual_file_with_errors(self):
        # test, and try printing a min-report:
        csv_file_exp_error_count_message = [
            ('2020-04-12-IHME-CurveFit.csv', 10, "Entries in `value` must be non-decreasing as quantiles increase"),
            ('2020-04-15-Geneva-DeterministicGrowth.csv', 1, "invalid target name(s)")]
        for quantile_file, exp_num_errors, exp_message in csv_file_exp_error_count_message:
            with open('tests/covid19-data-processed-examples/' + quantile_file) as quantile_fp:
                _, act_error_messages = json_io_dict_from_quantile_csv_file(quantile_fp, COVID19_TARGET_NAMES)
                self.assertEqual(exp_num_errors, len(act_error_messages))
                self.assertIn(exp_message, act_error_messages[0])  # arbitrarily pick first message. all are similar


    def test_json_io_dict_from_quantile_csv_file_bad_row_count(self):
        with open('tests/quantiles-bad-row-count.csv') as quantile_fp:
            _, act_error_messages = \
                json_io_dict_from_quantile_csv_file(quantile_fp, COVID19_TARGET_NAMES, covid19_row_validator)
            exp_errors = ["invalid number of items in row. len(header)=6 but len(row)=5. "
                          "row=['1 wk ahead cum death', 'Alaska', 'point', 'NA', '7.74526423651839']"]
            self.assertEqual(exp_errors, act_error_messages)


    def test_json_io_dict_from_quantile_csv_file_dup_points(self):
        with open('tests/quantiles-duplicate-points.csv') as quantile_fp:
            _, act_error_messages = json_io_dict_from_quantile_csv_file(quantile_fp, ['1 day ahead cum death'])
            exp_error_messages = ["Within a Prediction, there cannot be more than 1 Prediction Element of the same "
                                  "class. Found these duplicate unit/target tuples: "
                                  "[('04', '1 day ahead cum death', ['point', 'point'])]"]
            self.assertEqual(exp_error_messages, act_error_messages)


    def test_covid_validation_date_alignment(self):
        # test [add additional validations #56] - https://github.com/reichlab/covid19-forecast-hub/issues/56
        # (ensure that people are aligning forecast_date and target_end_date correctly)

        # 2020-04-13-MOBS_NEU-GLEAM_COVID.csv:
        column_index_dict = {'forecast_date': 0, 'target': 1, 'target_end_date': 2, 'location': 3, 'location_name': 4,
                             'type': 5, 'quantile': 6, 'value': 7}

        # 1/4) for x day ahead targets the target_end_date should be forecast_date + x
        row = ["2020-04-13", "1 day ahead cum death", "2020-04-14", "01", "Alabama", "point", "NA",
               "45.824147927692344"]  # ok: +1
        act_error_messages = covid19_row_validator(column_index_dict, row)
        self.assertEqual(0, len(act_error_messages))

        row = ["2020-04-13", "2 day ahead cum death", "2020-04-15", "01", "Alabama", "point", "NA",
               "48.22952942521442"]  # ok: +2
        act_error_messages = covid19_row_validator(column_index_dict, row)
        self.assertEqual(0, len(act_error_messages))

        row = ["2020-04-13", "1 day ahead cum death", "2020-04-15", "01", "Alabama", "point", "NA",
               "45.824147927692344"]  # bad: +2, not 1
        act_error_messages = covid19_row_validator(column_index_dict, row)
        self.assertEqual(1, len(act_error_messages))
        self.assertIn("invalid target_end_date: was not 1 day(s) after forecast_date", act_error_messages[0])

        # 2/4) for x week ahead targets, weekday(target_end_date) should be a Saturday (case: Sun or Mon)
        row = ["2020-04-13", "1 wk ahead cum death", "2020-04-18", "01", "Alabama", "point", "NA",
               "55.800809050176994"]  # ok: Mon -> Sat
        act_error_messages = covid19_row_validator(column_index_dict, row)
        self.assertEqual(0, len(act_error_messages))

        row = ["2020-04-13", "2 wk ahead cum death", "2020-04-25", "01", "Alabama", "point", "NA",
               "71.82206014865048"]  # ok: Mon -> Sat
        act_error_messages = covid19_row_validator(column_index_dict, row)
        self.assertEqual(0, len(act_error_messages))

        row = ["2020-04-13", "1 wk ahead cum death", "2020-04-19", "01", "Alabama", "point", "NA",
               "55.800809050176994"]  # bad: target_end_date is a Sun
        act_error_messages = covid19_row_validator(column_index_dict, row)
        self.assertEqual(1, len(act_error_messages))
        self.assertIn("target_end_date was not a Saturday", act_error_messages[0])

        # 3/4) (case: Sun or Mon) for x week ahead targets, ensure that the 1-week ahead forecast is for the next Sat
        row = ["2020-04-12", "1 wk ahead cum death", "2020-04-18", "01", "Alabama", "point", "NA",
               "55.800809050176994"]  # ok: 1 wk ahead Sun -> Sat
        act_error_messages = covid19_row_validator(column_index_dict, row)
        self.assertEqual(0, len(act_error_messages))

        row = ["2020-04-13", "1 wk ahead cum death", "2020-04-18", "01", "Alabama", "point", "NA",
               "55.800809050176994"]  # ok: 1 wk ahead Mon -> Sat
        act_error_messages = covid19_row_validator(column_index_dict, row)
        self.assertEqual(0, len(act_error_messages))

        row = ["2020-04-14", "1 wk ahead cum death", "2020-04-18", "01", "Alabama", "point", "NA",
               "55.800809050176994"]  # bad: 1 wk ahead Tue -> this Sat but s/b next Sat
        act_error_messages = covid19_row_validator(column_index_dict, row)
        self.assertEqual(1, len(act_error_messages))
        self.assertIn("target_end_date was not the expected Saturday", act_error_messages[0])

        row = ["2020-04-13", "2 wk ahead cum death", "2020-04-25", "01", "Alabama", "point", "NA",
               "71.82206014865048"]  # ok: 2 wk ahead Mon -> next Sat
        act_error_messages = covid19_row_validator(column_index_dict, row)
        self.assertEqual(0, len(act_error_messages))

        row = ["2020-04-13", "2 wk ahead cum death", "2020-04-18", "01", "Alabama", "point", "NA",
               "71.82206014865048"]  # bad: 2 wk ahead Mon -> next Sat
        act_error_messages = covid19_row_validator(column_index_dict, row)
        self.assertEqual(1, len(act_error_messages))
        self.assertIn("target_end_date was not the expected Saturday", act_error_messages[0])

        # 4/4) (case: Tue through Sat) for x week ahead targets, ensures that the 1-week ahead forecast is for the Sat after next
        row = ["2020-04-14", "1 wk ahead cum death", "2020-04-25", "01", "Alabama", "point", "NA",
               "55.800809050176994"]  # ok: 1 wk ahead Tue -> next Sat
        act_error_messages = covid19_row_validator(column_index_dict, row)
        self.assertEqual(0, len(act_error_messages))

        row = ["2020-04-14", "1 wk ahead cum death", "2020-04-18", "01", "Alabama", "point", "NA",
               "55.800809050176994"]  # bad: 1 wk ahead Tue -> this Sat, but should be next Sat (2020-04-25)
        act_error_messages = covid19_row_validator(column_index_dict, row)
        self.assertEqual(1, len(act_error_messages))
        self.assertIn("target_end_date was not the expected Saturday", act_error_messages[0])

        row = ["2020-04-13", "2 wk ahead cum death", "2020-04-25", "01", "Alabama", "point", "NA",
               "71.82206014865048"]  # ok: 2 wk ahead Mon -> next Sat
        act_error_messages = covid19_row_validator(column_index_dict, row)
        self.assertEqual(0, len(act_error_messages))

        row = ["2020-04-14", "2 wk ahead cum death", "2020-04-25", "01", "Alabama", "point", "NA",
               "71.82206014865048"]  # bad: 2 wk ahead Tue -> next Sat
        act_error_messages = covid19_row_validator(column_index_dict, row)
        self.assertEqual(1, len(act_error_messages))
        self.assertIn("target_end_date was not the expected Saturday", act_error_messages[0])


    def test_covid_validation_date_format(self):
        # test that `covid19_row_validator()` checks these columns are YYYY-MM-DD format: forecast_date, target_end_date

        # ok dates: '2020-04-15-Geneva-DeterministicGrowth.csv'
        test_dir = 'tests/covid19-data-processed-examples/'
        with open(test_dir + '2020-04-15-Geneva-DeterministicGrowth.csv') as quantile_fp:
            try:
                _, act_error_messages = \
                    json_io_dict_from_quantile_csv_file(quantile_fp, COVID19_TARGET_NAMES, covid19_row_validator)
            except Exception as ex:
                self.fail(f"unexpected exception: {ex}")

        # bad date: '2020-04-15-Geneva-DeterministicGrowth_bad_forecast_date.csv'
        with open(test_dir + '2020-04-15-Geneva-DeterministicGrowth_bad_forecast_date.csv') as quantile_fp:
            _, act_error_messages = \
                json_io_dict_from_quantile_csv_file(quantile_fp, COVID19_TARGET_NAMES, covid19_row_validator)
            self.assertEqual(1, len(act_error_messages))
            self.assertIn("invalid forecast_date or target_end_date format", act_error_messages[0])

        # bad date: '2020-04-15-Geneva-DeterministicGrowth_bad_target_end_date.csv'
        with open(test_dir + '2020-04-15-Geneva-DeterministicGrowth_bad_target_end_date.csv') as quantile_fp:
            _, act_error_messages = \
                json_io_dict_from_quantile_csv_file(quantile_fp, COVID19_TARGET_NAMES, covid19_row_validator)
            self.assertEqual(1, len(act_error_messages))
            self.assertIn("invalid forecast_date or target_end_date format", act_error_messages[0])


    def test_json_io_dict_from_quantile_csv_file_bad_covid_fips_code(self):
        for csv_file in ['quantiles-bad-row-fip-one-digit.csv', 'quantiles-bad-row-fip-three-digits.csv',
                         'quantiles-bad-row-fip-bad-two-digits.csv']:
            with open('tests/' + csv_file) as quantile_fp:
                _, error_messages = \
                    json_io_dict_from_quantile_csv_file(quantile_fp, COVID19_TARGET_NAMES, covid19_row_validator,
                                                        ['forecast_date', 'target_end_date'])
            self.assertEqual(1, len(error_messages))
            self.assertIn("invalid FIPS location", error_messages[0])


    def test_quantile_csv_rows_from_json_io_dict(self):
        with open('tests/docs-predictions.json') as fp:
            json_io_dict = json.load(fp)

        # blue sky. note that we hard-code the rows here instead of loading from an expected csv file b/c the latter
        # reads all values as strings, which means we'd have to cast types based on target. it became too painful :-)
        exp_rows = [['location', 'target', 'type', 'quantile', 'value'],
                    ['location1', 'pct next week', 'point', '', 2.1],
                    ['location2', 'pct next week', 'point', '', 2.0],
                    ['location2', 'pct next week', 'quantile', 0.025, 1.0],
                    ['location2', 'pct next week', 'quantile', 0.25, 2.2],
                    ['location2', 'pct next week', 'quantile', 0.5, 2.2],
                    ['location2', 'pct next week', 'quantile', 0.75, 5.0],
                    ['location2', 'pct next week', 'quantile', 0.975, 50.0],
                    ['location3', 'pct next week', 'point', '', 3.567],
                    ['location2', 'cases next week', 'point', '', 5],
                    ['location3', 'cases next week', 'point', '', 10],
                    ['location3', 'cases next week', 'quantile', 0.25, 0],
                    ['location3', 'cases next week', 'quantile', 0.75, 50],
                    ['location1', 'season severity', 'point', '', 'mild'],
                    ['location2', 'season severity', 'point', '', 'moderate'],
                    ['location1', 'above baseline', 'point', '', True],
                    ['location1', 'Season peak week', 'point', '', '2019-12-22'],
                    ['location2', 'Season peak week', 'point', '', '2020-01-05'],
                    ['location2', 'Season peak week', 'quantile', 0.5, '2019-12-22'],
                    ['location2', 'Season peak week', 'quantile', 0.75, '2019-12-29'],
                    ['location2', 'Season peak week', 'quantile', 0.975, '2020-01-05'],
                    ['location3', 'Season peak week', 'point', '', '2019-12-29']]
        act_rows = quantile_csv_rows_from_json_io_dict(json_io_dict)
        self.assertEqual(exp_rows, act_rows)


    # todo move to test_util.py
    def test_dataframe_from_json_io_dict(self):
        with open('tests/docs-predictions.json') as fp:
            json_io_dict = json.load(fp)

        df = dataframe_from_json_io_dict(json_io_dict)
        self.assertEqual((64, 12), df.shape)
