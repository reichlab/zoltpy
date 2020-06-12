import json
from unittest import TestCase

from zoltpy.csv_io import csv_rows_from_json_io_dict


class CsvIOTestCase(TestCase):
    """
    """


    def test_csv_rows_from_json_io_dict(self):
        # invalid prediction class. ok: forecast-repository.utils.forecast.PREDICTION_CLASS_TO_JSON_IO_DICT_CLASS
        with self.assertRaises(RuntimeError) as context:
            json_io_dict = {'meta': {'targets': []},
                            'predictions': [{'class': 'InvalidClass'}]}
            csv_rows_from_json_io_dict(json_io_dict)
        self.assertIn('invalid prediction_dict class', str(context.exception))

        # blue sky. note that we hard-code the rows here instead of loading from an expected csv file b/c the latter
        # reads all values as strings, which means we'd have to cast types based on target. it became too painful :-)
        exp_rows = [
            ['unit', 'target', 'class', 'value', 'cat', 'prob', 'sample', 'quantile', 'family', 'param1', 'param2',
             'param3'],
            ['location1', 'pct next week', 'point', 2.1, '', '', '', '', '', '', '', ''],
            ['location1', 'pct next week', 'named', '', '', '', '', '', 'norm', 1.1, 2.2, ''],
            ['location2', 'pct next week', 'point', 2.0, '', '', '', '', '', '', '', ''],
            ['location2', 'pct next week', 'bin', '', 1.1, 0.3, '', '', '', '', '', ''],
            ['location2', 'pct next week', 'bin', '', 2.2, 0.2, '', '', '', '', '', ''],
            ['location2', 'pct next week', 'bin', '', 3.3, 0.5, '', '', '', '', '', ''],
            ['location2', 'pct next week', 'quantile', 1.0, '', '', '', 0.025, '', '', '', ''],
            ['location2', 'pct next week', 'quantile', 2.2, '', '', '', 0.25, '', '', '', ''],
            ['location2', 'pct next week', 'quantile', 2.2, '', '', '', 0.5, '', '', '', ''],
            ['location2', 'pct next week', 'quantile', 5.0, '', '', '', 0.75, '', '', '', ''],
            ['location2', 'pct next week', 'quantile', 50.0, '', '', '', 0.975, '', '', '', ''],
            ['location3', 'pct next week', 'point', 3.567, '', '', '', '', '', '', '', ''],
            ['location3', 'pct next week', 'sample', '', '', '', 2.3, '', '', '', '', ''],
            ['location3', 'pct next week', 'sample', '', '', '', 6.5, '', '', '', '', ''],
            ['location3', 'pct next week', 'sample', '', '', '', 0.0, '', '', '', '', ''],
            ['location3', 'pct next week', 'sample', '', '', '', 10.0234, '', '', '', '', ''],
            ['location3', 'pct next week', 'sample', '', '', '', 0.0001, '', '', '', '', ''],
            ['location1', 'cases next week', 'named', '', '', '', '', '', 'pois', 1.1, '', ''],
            ['location2', 'cases next week', 'point', 5, '', '', '', '', '', '', '', ''],
            ['location2', 'cases next week', 'sample', '', '', '', 0, '', '', '', '', ''],
            ['location2', 'cases next week', 'sample', '', '', '', 2, '', '', '', '', ''],
            ['location2', 'cases next week', 'sample', '', '', '', 5, '', '', '', '', ''],
            ['location3', 'cases next week', 'point', 10, '', '', '', '', '', '', '', ''],
            ['location3', 'cases next week', 'bin', '', 0, 0.0, '', '', '', '', '', ''],
            ['location3', 'cases next week', 'bin', '', 2, 0.1, '', '', '', '', '', ''],
            ['location3', 'cases next week', 'bin', '', 50, 0.9, '', '', '', '', '', ''],
            ['location3', 'cases next week', 'quantile', 0, '', '', '', 0.25, '', '', '', ''],
            ['location3', 'cases next week', 'quantile', 50, '', '', '', 0.75, '', '', '', ''],
            ['location1', 'season severity', 'point', 'mild', '', '', '', '', '', '', '', ''],
            ['location1', 'season severity', 'bin', '', 'mild', 0.0, '', '', '', '', '', ''],
            ['location1', 'season severity', 'bin', '', 'moderate', 0.1, '', '', '', '', '', ''],
            ['location1', 'season severity', 'bin', '', 'severe', 0.9, '', '', '', '', '', ''],
            ['location2', 'season severity', 'point', 'moderate', '', '', '', '', '', '', '', ''],
            ['location2', 'season severity', 'sample', '', '', '', 'moderate', '', '', '', '', ''],
            ['location2', 'season severity', 'sample', '', '', '', 'severe', '', '', '', '', ''],
            ['location2', 'season severity', 'sample', '', '', '', 'high', '', '', '', '', ''],
            ['location2', 'season severity', 'sample', '', '', '', 'moderate', '', '', '', '', ''],
            ['location2', 'season severity', 'sample', '', '', '', 'mild', '', '', '', '', ''],
            ['location1', 'above baseline', 'point', True, '', '', '', '', '', '', '', ''],
            ['location2', 'above baseline', 'bin', '', True, 0.9, '', '', '', '', '', ''],
            ['location2', 'above baseline', 'bin', '', False, 0.1, '', '', '', '', '', ''],
            ['location2', 'above baseline', 'sample', '', '', '', True, '', '', '', '', ''],
            ['location2', 'above baseline', 'sample', '', '', '', False, '', '', '', '', ''],
            ['location2', 'above baseline', 'sample', '', '', '', True, '', '', '', '', ''],
            ['location3', 'above baseline', 'sample', '', '', '', False, '', '', '', '', ''],
            ['location3', 'above baseline', 'sample', '', '', '', True, '', '', '', '', ''],
            ['location3', 'above baseline', 'sample', '', '', '', True, '', '', '', '', ''],
            ['location1', 'Season peak week', 'point', '2019-12-22', '', '', '', '', '', '', '', ''],
            ['location1', 'Season peak week', 'bin', '', '2019-12-15', 0.01, '', '', '', '', '', ''],
            ['location1', 'Season peak week', 'bin', '', '2019-12-22', 0.1, '', '', '', '', '', ''],
            ['location1', 'Season peak week', 'bin', '', '2019-12-29', 0.89, '', '', '', '', '', ''],
            ['location1', 'Season peak week', 'sample', '', '', '', '2020-01-05', '', '', '', '', ''],
            ['location1', 'Season peak week', 'sample', '', '', '', '2019-12-15', '', '', '', '', ''],
            ['location2', 'Season peak week', 'point', '2020-01-05', '', '', '', '', '', '', '', ''],
            ['location2', 'Season peak week', 'bin', '', '2019-12-15', 0.01, '', '', '', '', '', ''],
            ['location2', 'Season peak week', 'bin', '', '2019-12-22', 0.05, '', '', '', '', '', ''],
            ['location2', 'Season peak week', 'bin', '', '2019-12-29', 0.05, '', '', '', '', '', ''],
            ['location2', 'Season peak week', 'bin', '', '2020-01-05', 0.89, '', '', '', '', '', ''],
            ['location2', 'Season peak week', 'quantile', '2019-12-22', '', '', '', 0.5, '', '', '', ''],
            ['location2', 'Season peak week', 'quantile', '2019-12-29', '', '', '', 0.75, '', '', '', ''],
            ['location2', 'Season peak week', 'quantile', '2020-01-05', '', '', '', 0.975, '', '', '', ''],
            ['location3', 'Season peak week', 'point', '2019-12-29', '', '', '', '', '', '', '', ''],
            ['location3', 'Season peak week', 'sample', '', '', '', '2020-01-06', '', '', '', '', ''],
            ['location3', 'Season peak week', 'sample', '', '', '', '2019-12-16', '', '', '', '', '']]
        with open('tests/docs-predictions.json') as fp:
            json_io_dict = json.load(fp)
            act_rows = csv_rows_from_json_io_dict(json_io_dict)
        self.assertEqual(exp_rows, act_rows)
