import csv
import json
from pathlib import Path
from unittest import TestCase

from zoltpy.cdc import cdc_csv_rows_from_json_io_dict, json_io_dict_from_cdc_csv_file
from zoltpy.csv_util import csv_rows_from_json_io_dict


class UtilsTestCase(TestCase):
    """
    """


    def test_json_io_dict_from_cdc_csv_file(self):
        with open('tests/EW1-KoTsarima-2017-01-17-small.csv') as cdc_csv_fp, \
                open('tests/exp-predictions.json') as exp_json_fp:
            exp_json_io_dict = json.load(exp_json_fp)  # converted from EW1-KoTsarima-2017-01-17-small.csv
            act_json_io_dict = json_io_dict_from_cdc_csv_file(cdc_csv_fp)
            self.assertEqual(exp_json_io_dict, act_json_io_dict)

        # test a test larger csv file that has >1 bin rows
        with open('tests/20161023-KoTstable-20161109-small.cdc.csv') as cdc_csv_fp, \
                open('tests/20161023-KoTstable-20161109-small-exp-predictions.json') \
                        as exp_json_fp:
            exp_json_io_dict = json.load(exp_json_fp)
            act_json_io_dict = json_io_dict_from_cdc_csv_file(cdc_csv_fp)
            self.assertEqual(exp_json_io_dict, act_json_io_dict)

        # test a csv file with blank cells
        with open('tests/EW43-2019-FluOutlook_Mech.csv') as cdc_csv_fp, \
                open('tests/EW43-2019-FluOutlook_Mech-exp-predictions.json') \
                        as exp_json_fp:
            exp_json_io_dict = json.load(exp_json_fp)
            act_json_io_dict = json_io_dict_from_cdc_csv_file(cdc_csv_fp)
            self.assertEqual(exp_json_io_dict, act_json_io_dict)


    def test_csv_rows_from_json_io_dict(self):
        # no meta
        with self.assertRaises(RuntimeError) as context:
            csv_rows_from_json_io_dict({})
        self.assertIn('no meta section found in json_io_dict', str(context.exception))

        # no meta > targets
        with self.assertRaises(RuntimeError) as context:
            csv_rows_from_json_io_dict({'meta': {}})
        self.assertIn('no targets section found in json_io_dict meta section', str(context.exception))

        # invalid prediction class. ok: forecast-repository.utils.forecast.PREDICTION_CLASS_TO_JSON_IO_DICT_CLASS
        with self.assertRaises(RuntimeError) as context:
            json_io_dict = {'meta': {'targets': []},
                            'predictions': [{'class': 'InvalidClass'}]}
            csv_rows_from_json_io_dict(json_io_dict)
        self.assertIn('invalid prediction_dict class', str(context.exception))

        with open('tests/predictions-example.json') as fp:
            json_io_dict = json.load(fp)
        with self.assertRaises(RuntimeError) as context:
            # remove arbitrary meta target. doesn't matter b/c all are referenced
            del (json_io_dict['meta']['targets'][0])
            csv_rows_from_json_io_dict(json_io_dict)
        self.assertIn('prediction_dict target not found in meta targets', str(context.exception))

        with open('tests/predictions-example.json') as fp:
            json_io_dict = json.load(fp)
        # location,target,unit,class,cat,family,lwr,param1,param2,param3,prob,sample,value
        exp_rows = [
            ['unit', 'target', 'unit', 'class', 'cat', 'family', 'lwr', 'param1', 'param2', 'param3', 'prob',
             'sample', 'value'],
            ['US National', '1 wk ahead', 'percent', 'BinCat', 'cat1', '', '', '', '', '', 0.0, '', ''],
            ['US National', '1 wk ahead', 'percent', 'BinCat', 'cat2', '', '', '', '', '', 0.1, '', ''],
            ['US National', '1 wk ahead', 'percent', 'BinCat', 'cat3', '', '', '', '', '', 0.9, '', ''],
            ['HHS Region 1', '2 wk ahead', 'percent', 'BinLwr', '', '', 0.0, '', '', '', 0.0, '', ''],
            ['HHS Region 1', '2 wk ahead', 'percent', 'BinLwr', '', '', 0.1, '', '', '', 0.1, '', ''],
            ['HHS Region 1', '2 wk ahead', 'percent', 'BinLwr', '', '', 0.2, '', '', '', 0.9, '', ''],
            ['HHS Region 2', '3 wk ahead', 'percent', 'Binary', '', '', '', '', '', '', 0.5, '', ''],
            ['HHS Region 3', '4 wk ahead', 'percent', 'Named', '', 'gamma', '', 1.1, 2.2, 3.3, '', '', ''],
            ['HHS Region 4', 'Season onset', 'week', 'Point', '', '', '', '', '', '', '', '', '1'],
            ['HHS Region 5', 'Season peak percentage', 'percent', 'Sample', '', '', '', '', '', '', '', 1.1, ''],
            ['HHS Region 5', 'Season peak percentage', 'percent', 'Sample', '', '', '', '', '', '', '', 2.2, ''],
            ['HHS Region 6', 'Season peak week', 'week', 'SampleCat', 'cat1', '', '', '', '', '', '', 'cat1 sample',
             ''],
            ['HHS Region 6', 'Season peak week', 'week', 'SampleCat', 'cat2', '', '', '', '', '', '', 'cat2 sample',
             '']]
        act_rows = csv_rows_from_json_io_dict(json_io_dict)
        self.assertEqual(exp_rows, act_rows)


    def test_cdc_csv_rows_from_json_io_dict(self):
        # no predictions
        with self.assertRaises(RuntimeError) as context:
            cdc_csv_rows_from_json_io_dict({})
        self.assertIn('no predictions section found in json_io_dict', str(context.exception))

        # invalid prediction class
        for invalid_prediction_class in ['Binary', 'Named', 'Sample', 'SampleCat']:  # ok: 'BinCat', 'BinLwr', 'Point'
            with self.assertRaises(RuntimeError) as context:
                cdc_csv_rows_from_json_io_dict({'predictions': [{'class': invalid_prediction_class}]})
            self.assertIn('invalid prediction_dict class', str(context.exception))

        # prediction_dict target not recognized
        with self.assertRaises(RuntimeError) as context:
            cdc_csv_rows_from_json_io_dict({'predictions': [{'class': 'Point', 'target': 'non-CDC target'}]})
        self.assertIn('prediction_dict target not recognized', str(context.exception))

        # blue sky
        with open(Path('tests/EW1-KoTsarima-2017-01-17-small.csv')) as csv_fp:
            csv_reader = csv.reader(csv_fp, delimiter=',')
            exp_rows = list(csv_reader)
            exp_rows[0] = list(map(str.lower, exp_rows[0]))  # fix header case difference
            exp_rows = list(map(_xform_cdc_csv_row, sorted(exp_rows)))
        with open('tests/EW1-KoTsarima-2017-01-17-small.json') as fp:
            json_io_dict = json.load(fp)
        act_rows = sorted(cdc_csv_rows_from_json_io_dict(json_io_dict))
        self.assertEqual(exp_rows, act_rows)


# test_cdc_csv_rows_from_json_io_dict() helper that transforms expected row values to float() as needed to match actual
def _xform_cdc_csv_row(row):
    location, target, row_type, unit, bin_start_incl, bin_end_notincl, value = row
    if row_type == 'Bin' and unit == 'percent':
        try:
            bin_start_incl = float(bin_start_incl)
            bin_end_notincl = float(bin_end_notincl)
            value = float(value)
        except ValueError:
            pass

    if row_type == 'Bin' and unit == 'week':
        try:
            value = float(value)
        except ValueError:
            pass

    if row_type == 'Point' and unit == 'percent':
        try:
            value = float(value)
        except ValueError:
            pass

    return [location, target, row_type, unit, bin_start_incl, bin_end_notincl, value]
