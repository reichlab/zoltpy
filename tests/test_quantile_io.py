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
            with open('tests/quantile-predictions.csv') as quantile_csv_fp:
                json_io_dict_from_quantile_csv_file(quantile_csv_fp)
                self.assertEqual(1, mock.call_count)


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
        for quantile_csv_file in ['tests/quantile-predictions-5-col.csv', 'tests/quantile-predictions.csv']:
            with open(quantile_csv_file) as quantile_csv_fp, \
                    open('tests/quantile-predictions.json') as exp_json_fp:
                exp_json_io_dict = json.load(exp_json_fp)
                act_json_io_dict = json_io_dict_from_quantile_csv_file(quantile_csv_fp)
                exp_json_io_dict['predictions'].sort(key=lambda _: (_['unit'], _['target'], _['class']))
                act_json_io_dict['predictions'].sort(key=lambda _: (_['unit'], _['target'], _['class']))
                self.assertEqual(exp_json_io_dict, act_json_io_dict)


    def test_other_ok_quantile_files(self):
        ok_quantile_files = ['tests/quantiles-CU-60contact.csv']
        for ok_quantile_file in ok_quantile_files:
            with open(ok_quantile_file) as quantile_csv_fp:
                try:
                    json_io_dict_from_quantile_csv_file(quantile_csv_fp)
                except Exception as ex:
                    self.fail(f"unexpected exception: {ex}")

    def test_json_io_dict_from_quantile_csv_file_bad_header(self):
        csv_file_exp_errors = [
            ('quantiles-bad-row-count.csv', 'invalid number of items in row. expected: 6 but got 5'),
            ('quantiles-bad-row-fip-one-digit.csv', 'invalid FIPS: not two characters'),
            ('quantiles-bad-row-fip-three-digits.csv', 'invalid FIPS: not two characters'),
            ('quantiles-bad-row-fip-bad-two-digits.csv', 'invalid FIPS: two character int but out of range'),
        ]
        for csv_file, exp_error in csv_file_exp_errors:
            with open('tests/' + csv_file) as quantile_csv_fp:
                with self.assertRaises(RuntimeError) as context:
                    json_io_dict_from_quantile_csv_file(quantile_csv_fp)
                self.assertIn(exp_error, str(context.exception))


    def test_json_io_dict_from_quantile_csv_file_dup_points(self):
        with open('tests/quantiles-duplicate-points.csv') as quantile_csv_fp:
            with self.assertRaises(RuntimeError) as context:
                json_io_dict_from_quantile_csv_file(quantile_csv_fp)
            self.assertIn('found more than one point value for the same target_name', str(context.exception))
