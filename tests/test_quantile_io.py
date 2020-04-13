import json
from unittest import TestCase

from zoltpy.quantile import json_io_dict_from_quantile_csv_file


class QuantileIOTestCase(TestCase):
    """
    """


    def test_json_io_dict_from_quantile_csv_file(self):
        with open('tests/quantile-predictions.csv') as quantile_csv_fp, \
                open('tests/quantile-predictions.json') as exp_json_fp:
            exp_json_io_dict = json.load(exp_json_fp)
            act_json_io_dict = json_io_dict_from_quantile_csv_file(quantile_csv_fp)
            exp_json_io_dict['predictions'].sort(key=lambda _: (_['unit'], _['target'], _['class']))
            act_json_io_dict['predictions'].sort(key=lambda _: (_['unit'], _['target'], _['class']))
            self.assertEqual(exp_json_io_dict, act_json_io_dict)
