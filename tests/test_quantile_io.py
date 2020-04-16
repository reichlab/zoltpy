import json
from unittest import TestCase

from zoltpy.quantile import json_io_dict_from_quantile_csv_file


class QuantileIOTestCase(TestCase):
    """
    """


    def test_json_io_dict_from_quantile_csv_file_ok(self):
        for quantile_csv_file in ['tests/quantile-predictions-5-col.csv', 'tests/quantile-predictions.csv']:
            with open(quantile_csv_file) as quantile_csv_fp, \
                    open('tests/quantile-predictions.json') as exp_json_fp:
                exp_json_io_dict = json.load(exp_json_fp)
                act_json_io_dict = json_io_dict_from_quantile_csv_file(quantile_csv_fp)
                exp_json_io_dict['predictions'].sort(key=lambda _: (_['unit'], _['target'], _['class']))
                act_json_io_dict['predictions'].sort(key=lambda _: (_['unit'], _['target'], _['class']))
                self.assertEqual(exp_json_io_dict, act_json_io_dict)


    def test_json_io_dict_from_quantile_csv_file_bad_header(self):
        csv_file_exp_errors = [
            ('quantiles-bad-header-only-four.csv', 'invalid header. number of columns was not 5 or 6'),
            ('quantiles-bad-header-seven-cols.csv', 'invalid header. number of columns was not 5 or 6'),
            ('quantiles-bad-header-five-col-bad-name.csv', 'invalid header. had five columns, but not the expected'),
            ('quantiles-bad-header-six-col-bad-name.csv', 'invalid header. had six columns, but not the expected'),
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
