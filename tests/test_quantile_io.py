import json
from unittest import TestCase
from unittest.mock import patch

from zoltpy.quantile import json_io_dict_from_quantile_csv_file, _validate_header, REQUIRED_COLUMNS


class QuantileIOTestCase(TestCase):
    """
    """


    def test_json_io_dict_from_quantile_csv_file_calls_validate_header(self):
        # location_idx, target_idx, row_type_idx, quantile_idx, value_idx:
        with patch('zoltpy.quantile._validate_header', return_value=[1, 0, 3, 4, 5]) as mock:
            with open('tests/quantile-predictions.csv') as quantile_fp:
                json_io_dict_from_quantile_csv_file(quantile_fp)
                self.assertEqual(1, mock.call_count)


    def test_json_io_dict_from_quantile_csv_file_small_tolerance(self):
        with open('tests/covid19-forecast-hub_data-processed_examples/2020-04-20-YYG-ParamSearch-small.csv') \
                as quantile_fp:
            json_io_dict, act_error_messages = json_io_dict_from_quantile_csv_file(quantile_fp)
            self.assertEqual(0, len(act_error_messages))


    def test_validate_header(self):
        # test various valid headers
        for columns in [REQUIRED_COLUMNS,  # canonical order
                        ['location', 'type', 'target', 'quantile', 'value'],  # different order
                        ['location', 'type', 'target', 'quantile', 'foo', 'value'],  # extra column
                        ['bar', 'location', 'type', 'target', 'quantile', 'foo', 'value'],  # extra columns
                        ]:
            try:
                _validate_header(columns)
            except Exception as ex:
                self.fail(f"unexpected exception: {ex}")

        # test removing each required_column one at a time
        for required_column in REQUIRED_COLUMNS:
            columns = list(REQUIRED_COLUMNS)  # copy
            req_col_idx = columns.index(required_column)
            del columns[req_col_idx]
            with self.assertRaises(RuntimeError) as context:
                _validate_header(columns)
            self.assertIn("invalid header. did not contain the required columns", str(context.exception))

        # test duplicate required column
        with self.assertRaises(RuntimeError) as context:
            _validate_header(REQUIRED_COLUMNS + ['type'])
        self.assertIn("invalid header. did not contain the required columns", str(context.exception))


    def test_json_io_dict_from_quantile_csv_file_ok(self):
        for quantile_file in ['tests/quantile-predictions-5-col.csv', 'tests/quantile-predictions.csv']:
            with open(quantile_file) as quantile_fp, \
                    open('tests/quantile-predictions.json') as exp_json_fp:
                exp_json_io_dict = json.load(exp_json_fp)
                act_json_io_dict, error_messages = json_io_dict_from_quantile_csv_file(quantile_fp)
                exp_json_io_dict['predictions'].sort(key=lambda _: (_['unit'], _['target'], _['class']))
                act_json_io_dict['predictions'].sort(key=lambda _: (_['unit'], _['target'], _['class']))
                self.assertEqual(exp_json_io_dict, act_json_io_dict)


    def test_other_ok_quantile_files(self):
        with open('tests/quantiles-CU-60contact.csv') as quantile_fp:
            json_io_dict, act_error_messages = json_io_dict_from_quantile_csv_file(quantile_fp)
            self.assertEqual(0, len(act_error_messages))


    def test_error_messages_actual_files_no_errors(self):
        # test large-ish actual files
        ok_quantile_files = [
            # '2020-04-12-IHME-CurveFit.csv',  # errors. tested below
            # '2020-04-15-Geneva-DeterministicGrowth.csv',  # ""
            '2020-04-13-COVIDhub-ensemble.csv',
            '2020-04-13-Imperial-ensemble1.csv',
            '2020-04-13-MOBS_NEU-GLEAM_COVID.csv']
        for quantile_file in ok_quantile_files:
            with open('tests/covid19-forecast-hub_data-processed_examples/' + quantile_file) as quantile_fp:
                json_io_dict, act_error_messages = json_io_dict_from_quantile_csv_file(quantile_fp)
                self.assertEqual(0, len(act_error_messages))


    def test_error_messages_actual_file_with_errors(self):
        # test, and try printing a min-report:
        csv_file_exp_error_count_message = [
            ('2020-04-12-IHME-CurveFit.csv', 10, "Entries in `value` must be non-decreasing as quantiles increase"),
            ('2020-04-15-Geneva-DeterministicGrowth.csv', 1, "invalid target name(s)")]
        for quantile_file, exp_num_errors, exp_message in csv_file_exp_error_count_message:
            with open('tests/covid19-forecast-hub_data-processed_examples/' + quantile_file) as quantile_fp:
                json_io_dict, act_error_messages = json_io_dict_from_quantile_csv_file(quantile_fp)
                self.assertEqual(exp_num_errors, len(act_error_messages))
                self.assertIn(exp_message, act_error_messages[0])  # arbitrarily pick first message. all are similar


    def test_json_io_dict_from_quantile_csv_file_bad_header(self):
        csv_file_exp_errors = [
            ('quantiles-bad-row-count.csv', [
                "invalid number of items in row. len(header)=6 but len(row)=5. "
                "row=['1 wk ahead cum death', 'Alaska', 'point', 'NA', '7.74526423651839']"]),
            ('quantiles-bad-row-fip-one-digit.csv', [
                "invalid FIPS: not two characters: '2'. "
                "row=['1 wk ahead cum death', '2', 'Alaska', 'point', 'NA', '7.74526423651839']"]),
            ('quantiles-bad-row-fip-three-digits.csv', [
                "invalid FIPS: not two characters: '222'. "
                "row=['1 wk ahead cum death', '222', 'Alaska', 'point', 'NA', '7.74526423651839']",
                "invalid FIPS: two character int but out of range 1-95: '222'"]),
            ('quantiles-bad-row-fip-bad-two-digits.csv', [
                "invalid FIPS: two character int but out of range 1-95: '99'"]),
        ]
        for csv_file, exp_errors in csv_file_exp_errors:
            with open('tests/' + csv_file) as quantile_fp:
                json_io_dict, act_error_messages = json_io_dict_from_quantile_csv_file(quantile_fp)
                self.assertEqual(exp_errors, act_error_messages)


    def test_json_io_dict_from_quantile_csv_file_dup_points(self):
        with open('tests/quantiles-duplicate-points.csv') as quantile_fp:
            json_io_dict, act_error_messages = json_io_dict_from_quantile_csv_file(quantile_fp)
            exp_error_messages = ["found more than one point value for the same target_name, location_fips. "
                                  "target_name='1 day ahead cum death', location_fips='04', this point value=17, "
                                  "previous point_value=78"]
            self.assertEqual(exp_error_messages, act_error_messages)
