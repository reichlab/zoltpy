import json
from unittest import TestCase
from unittest.mock import patch

from zoltpy.quantile import json_io_dict_from_quantile_csv_file, _validate_header, REQUIRED_COLUMNS, \
    COVID19_TARGET_NAMES, covid19_row_validator


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
        with patch('zoltpy.quantile._validate_header', return_value=column_index_dict) as mock:
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
            exp_error_messages = ["found more than one point value for the same target_name, location. "
                                  "target_name='1 day ahead cum death', location='04', this point value=17, "
                                  "previous point_value=78"]
            self.assertEqual(exp_error_messages, act_error_messages)


    def test_covid_date_validation(self):
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
            exp_error_messages = ["invalid forecast_date or target_end_date format. forecast_date='20200415'. "
                                  "target_end_date=2020-04-16. row=['20200415', '1 day ahead inc death', "
                                  "'2020-04-16', 'US', 'US', 'point', 'NA', '2232']"]
            self.assertEqual(exp_error_messages, act_error_messages)

        # bad date: '2020-04-15-Geneva-DeterministicGrowth_bad_target_end_date.csv'
        with open(test_dir + '2020-04-15-Geneva-DeterministicGrowth_bad_target_end_date.csv') as quantile_fp:
            _, act_error_messages = \
                json_io_dict_from_quantile_csv_file(quantile_fp, COVID19_TARGET_NAMES, covid19_row_validator)
            exp_error_messages = ["invalid forecast_date or target_end_date format. forecast_date='2020-04-15'. "
                                  "target_end_date=20200416. row=['2020-04-15', '1 day ahead inc death', "
                                  "'20200416', 'US', 'US', 'point', 'NA', '2232']"]
            self.assertEqual(exp_error_messages, act_error_messages)


    def test_json_io_dict_from_quantile_csv_file_bad_covid(self):
        csv_file_exp_errors = [
            ('quantiles-bad-row-fip-one-digit.csv', [
                "invalid FIPS: not two characters: '2'. "
                "row=['2020-04-15', '1 day ahead inc death', '2020-04-16', '2', 'US', 'point', 'NA', '2232']"]),
            ('quantiles-bad-row-fip-three-digits.csv', [
                "invalid FIPS: not two characters: '222'. "
                "row=['2020-04-15', '1 day ahead inc death', '2020-04-16', '222', 'US', 'point', 'NA', '2232']",
                "invalid FIPS: two character int but out of range 1-95: '222'"]),
            ('quantiles-bad-row-fip-bad-two-digits.csv', [
                "invalid FIPS: two character int but out of range 1-95: '99'"]),
        ]
        for csv_file, exp_errors in csv_file_exp_errors:
            with open('tests/' + csv_file) as quantile_fp:
                _, act_error_messages = \
                    json_io_dict_from_quantile_csv_file(quantile_fp, COVID19_TARGET_NAMES, covid19_row_validator,
                                                        ['forecast_date', 'target_end_date'])
                self.assertEqual(exp_errors, act_error_messages)
