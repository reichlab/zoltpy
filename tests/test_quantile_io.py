import json
from unittest import TestCase
from unittest.mock import patch

from zoltpy.covid19 import covid19_row_validator, COVID_ADDL_REQ_COLS, FIPS_CODES_STATE, \
    FIPS_CODES_COUNTY, COVID_TARGETS
from zoltpy.csv_io import CSV_HEADER
from zoltpy.quantile_io import json_io_dict_from_quantile_csv_file, _validate_header, REQUIRED_COLUMNS, \
    quantile_csv_rows_from_json_io_dict, summarized_error_messages, MESSAGE_DATE_ALIGNMENT, MESSAGE_FORECAST_CHECKS, \
    MESSAGE_QUANTILES_AND_VALUES, MESSAGE_QUANTILES_AS_A_GROUP
from zoltpy.util import dataframe_from_json_io_dict


class QuantileIOTestCase(TestCase):
    """
    """


    def test_locations(self):
        self.assertEqual(58, len(FIPS_CODES_STATE))
        self.assertEqual(3142, len(FIPS_CODES_COUNTY))


    def test_optional_additional_required_column_names(self):
        # target, location, location_name, type, quantile,value:
        with open('tests/quantile-predictions.csv') as quantile_fp:
            _, error_messages = \
                json_io_dict_from_quantile_csv_file(quantile_fp, ['1 wk ahead cum death', '1 day ahead inc hosp'],
                                                    addl_req_cols=COVID_ADDL_REQ_COLS)
            self.assertEqual(1, len(error_messages))
            self.assertEqual(MESSAGE_FORECAST_CHECKS, error_messages[0][0])
            self.assertIn('invalid header. did not contain the required column(s)', error_messages[0][1])

        # forecast_date, target, target_end_date, location, location_name, type, quantile, value:
        with open('tests/covid19-data-processed-examples/2020-04-15-Geneva-DeterministicGrowth.csv') as quantile_fp:
            try:
                json_io_dict_from_quantile_csv_file(quantile_fp, ['1 wk ahead cum death', '1 day ahead inc hosp'],
                                                    addl_req_cols=COVID_ADDL_REQ_COLS)
            except Exception as ex:
                self.fail(f"unexpected exception: {ex}")


    def test_json_io_dict_from_quantile_csv_file_calls_validate_header(self):
        column_index_dict = {'target': 0, 'location': 1, 'type': 2, 'quantile': 3, 'value': 4}
        with patch('zoltpy.quantile_io._validate_header', return_value=(column_index_dict, None)) as mock, \
                open('tests/quantile-predictions.csv') as quantile_fp:
            json_io_dict_from_quantile_csv_file(quantile_fp, ['1 wk ahead cum death', '1 day ahead inc hosp'])
            self.assertEqual(1, mock.call_count)


    def test_json_io_dict_from_quantile_csv_file_small_tolerance(self):
        with open('tests/covid19-data-processed-examples/2020-04-20-YYG-ParamSearch-small.csv') as quantile_fp:
            _, error_messages = json_io_dict_from_quantile_csv_file(quantile_fp, COVID_TARGETS,
                                                                    covid19_row_validator,
                                                                    addl_req_cols=COVID_ADDL_REQ_COLS)
            self.assertEqual(0, len(error_messages))

    def test_json_io_dict_from_invalid_type_header(self):
        with open('covid19-data-processed-examples/2020-04-20-YYG-invalid-type.csv') as quantile_fp:
            _, error_messages = json_io_dict_from_quantile_csv_file(quantile_fp, COVID_TARGETS,
                                                                    covid19_row_validator,
                                                                    addl_req_cols=COVID_ADDL_REQ_COLS)
            self.assertEqual(1, len(error_messages))


    def test_validate_header(self):
        for columns, exp_column_index_dict, exp_error in [
            (REQUIRED_COLUMNS,  # canonical order
             {'location': 0, 'target': 1, 'type': 2, 'quantile': 3, 'value': 4},
             None),
            (['location', 'type', 'target', 'quantile', 'value'],  # different order
             {'location': 0, 'type': 1, 'target': 2, 'quantile': 3, 'value': 4},
             None),
            (['location', 'type', 'target', 'quantile', 'foo', 'value'],  # extra column
             {'location': 0, 'type': 1, 'target': 2, 'quantile': 3, 'value': 5},
             "invalid header. contained extra columns(s)"),
            (['bar', 'location', 'type', 'target', 'quantile', 'foo', 'value'],  # extra columns
             {'location': 1, 'type': 2, 'target': 3, 'quantile': 4, 'value': 6},
             "invalid header. contained extra columns(s)"),
            (list(REQUIRED_COLUMNS) + ['type'],  # duplicate required column
             None,
             "invalid header. found duplicate column(s)"),
            (list(REQUIRED_COLUMNS) + ['foo', 'foo'],  # duplicate extra column
             None,
             "invalid header. found duplicate column(s)"),
        ]:
            act_column_index_dict, act_error = _validate_header(columns, [])
            self.assertEqual(exp_column_index_dict, act_column_index_dict)
            if exp_error is None:
                self.assertIsNone(act_error)
            else:
                self.assertIn(exp_error, act_error)

        # test removing each required_column one at a time
        for required_column in REQUIRED_COLUMNS:
            columns_exp_idxs = list(REQUIRED_COLUMNS)  # copy
            req_col_idx = columns_exp_idxs.index(required_column)
            del columns_exp_idxs[req_col_idx]
            act_column_index_dict, act_error = _validate_header(columns_exp_idxs, [])
            self.assertIsNone(act_column_index_dict)
            self.assertIn("invalid header. did not contain the required column(s)", act_error)


    def test_json_io_dict_from_quantile_csv_file_ok(self):
        for quantile_file in ['tests/quantile-predictions-5-col.csv',
                              'tests/quantile-predictions.csv']:
            with open(quantile_file) as quantile_fp, \
                    open('tests/quantile-predictions.json') as exp_json_fp:
                exp_json_io_dict = json.load(exp_json_fp)
                act_json_io_dict, _ = json_io_dict_from_quantile_csv_file(quantile_fp, ['1 wk ahead cum death',
                                                                                        '1 day ahead inc hosp'])
                exp_json_io_dict['predictions'].sort(key=lambda _: (_['unit'], _['target'], _['class']))
                act_json_io_dict['predictions'].sort(key=lambda _: (_['unit'], _['target'], _['class']))
                self.assertEqual(exp_json_io_dict, act_json_io_dict)


    def test_other_ok_quantile_files(self):
        with open('tests/quantiles-CU-60contact.csv') as quantile_fp:
            _, error_messages = json_io_dict_from_quantile_csv_file(quantile_fp, COVID_TARGETS,
                                                                    covid19_row_validator,
                                                                    addl_req_cols=COVID_ADDL_REQ_COLS)
            self.assertEqual(0, len(error_messages))


    def test_error_messages_actual_files_no_errors(self):
        # test large-ish actual files
        ok_quantile_files = [
            # '2020-04-12-IHME-CurveFit.csv',  # errors. tested below
            # '2020-04-15-Geneva-DeterministicGrowth.csv',  # ""
            '2020-04-13-COVIDhub-ensemble.csv',
            '2020-04-12-Imperial-ensemble1.csv',
            '2020-04-13-MOBS_NEU-GLEAM_COVID.csv']
        for quantile_file in ok_quantile_files:
            with open('tests/covid19-data-processed-examples/' + quantile_file) as quantile_fp:
                _, error_messages = json_io_dict_from_quantile_csv_file(quantile_fp, COVID_TARGETS,
                                                                        covid19_row_validator,
                                                                        addl_req_cols=COVID_ADDL_REQ_COLS)
                self.assertEqual(0, len(error_messages))


    def test_error_messages_actual_file_with_errors(self):
        file_exp_num_errors_message_priority_messages = [
            ('2020-04-12-IHME-CurveFit.csv', 5, MESSAGE_QUANTILES_AND_VALUES,
             ["Entries in `value` must be non-decreasing as quantiles increase"]),
            ('2020-04-15-Geneva-DeterministicGrowth.csv', 1, MESSAGE_FORECAST_CHECKS,
             ["invalid target name(s)"]),
            ('2020-05-17-CovidActNow-SEIR_CAN.csv', 10, MESSAGE_FORECAST_CHECKS,
             ["entries in the `value` column must be non-negative"]),
            ('2020-06-21-USC-SI_kJalpha.csv', 1, MESSAGE_FORECAST_CHECKS,
             ["entries in the `value` column must be non-negative"]),
            ('2020-07-04-YYG-ParamSearch.csv', 2, MESSAGE_FORECAST_CHECKS,
             ["invalid header. contained extra columns(s)", "invalid target name(s)"]),
            ('2020-07-12-UMass-MechBayes.csv', 2, MESSAGE_FORECAST_CHECKS,
             ["invalid quantile for target", "entries in the `type` column must be either 'point' or 'quantile'"]),
        ]
        for quantile_file, exp_num_errors, exp_priority, exp_error_messages in \
                file_exp_num_errors_message_priority_messages:
            with open('tests/covid19-data-processed-examples/' + quantile_file) as quantile_fp:
                _, act_error_messages = json_io_dict_from_quantile_csv_file(quantile_fp, COVID_TARGETS,
                                                                            covid19_row_validator,
                                                                            addl_req_cols=COVID_ADDL_REQ_COLS)
                self.assertEqual(exp_num_errors, len(act_error_messages), exp_error_messages)
                for act_priority, act_error_message in act_error_messages:
                    self.assertEqual(exp_priority, act_priority)
                    self.assertTrue(any([exp_error_message in act_error_message for exp_error_message in exp_error_messages]))


    def test_summarize_error_messages(self):
        input_error_messages = [(MESSAGE_DATE_ALIGNMENT,
                                 "The number of elements in the `quantile` and `value` vectors should be identical")] * 3
        input_error_messages.extend([(MESSAGE_FORECAST_CHECKS,
                                      "Entries in `value` must be non-decreasing as quantiles increase")] * 3)
        act_error_messages = summarized_error_messages(input_error_messages, max_num_dups=2)
        exp_error_messages = ['The number of elements in the `quantile` and `value` vectors should be identical',
                              'The number of elements in the `quantile` and `value` vectors should be identical',
                              'The number of elemen...',
                              'Entries in `value` must be non-decreasing as quantiles increase',
                              'Entries in `value` must be non-decreasing as quantiles increase',
                              'Entries in `value` m...']
        self.assertEqual(sorted(exp_error_messages), sorted(act_error_messages))


    def test_json_io_dict_from_quantile_csv_file_bad_row_count(self):
        with open('tests/quantiles-bad-row-count.csv') as quantile_fp:  # header: 6, row: 5
            _, error_messages = json_io_dict_from_quantile_csv_file(quantile_fp, COVID_TARGETS)
            exp_errors = [(MESSAGE_FORECAST_CHECKS,
                           "invalid number of items in row. len(header)=5 but len(row)=4. "
                           "row=['1 wk ahead cum death', 'point', 'NA', '7.74526423651839']")]
            self.assertEqual(exp_errors, error_messages)


    def test_json_io_dict_from_quantile_csv_file_dup_points(self):
        with open('tests/quantiles-duplicate-points.csv') as quantile_fp:
            _, act_error_messages = json_io_dict_from_quantile_csv_file(quantile_fp, ['1 day ahead inc hosp'])
            exp_error_messages = [(MESSAGE_QUANTILES_AND_VALUES,
                                   "Within a Prediction, there cannot be more than 1 Prediction Element of the same "
                                   "class. Found these duplicate unit/target/classes tuples: [('04', '1 day ahead "
                                   "inc hosp', ['point', 'point'])]"),
                                  (MESSAGE_QUANTILES_AS_A_GROUP,
                                   "There must be exactly one point prediction for each location/target pair. Found "
                                   "these unit, target, point counts tuples did not have exactly one point: [('04', "
                                   "'1 day ahead inc hosp', 2)]")]
            self.assertEqual(exp_error_messages, act_error_messages)


    def test_json_io_dict_from_quantile_csv_file_no_points(self):
        with open('tests/quantile-predictions-no-point.csv') as quantile_fp:
            _, error_messages = json_io_dict_from_quantile_csv_file(quantile_fp, ['1 day ahead inc hosp',
                                                                                  '1 wk ahead cum death'])
            self.assertEqual(1, len(error_messages))
            self.assertEqual(MESSAGE_QUANTILES_AS_A_GROUP, error_messages[0][0])
            self.assertIn("There must be exactly one point prediction for each location/target pair",
                          error_messages[0][1])


    def test_json_io_dict_from_quantile_csv_file_nan(self):
        with open('tests/quantile-predictions-nan-point.csv') as quantile_fp:
            _, error_messages = \
                json_io_dict_from_quantile_csv_file(quantile_fp, ['1 wk ahead cum death', '1 day ahead inc hosp'])
            self.assertEqual(1, len(error_messages))
            self.assertEqual(MESSAGE_FORECAST_CHECKS, error_messages[0][0])
        self.assertIn('entries in the `value` column must be an int or float', error_messages[0][1])

        with open('tests/quantile-predictions-nan-quantile.csv') as quantile_fp:
            _, error_messages = \
                json_io_dict_from_quantile_csv_file(quantile_fp, ['1 wk ahead cum death', '1 day ahead inc hosp'])
            self.assertEqual(1, len(error_messages))
            self.assertEqual(MESSAGE_FORECAST_CHECKS, error_messages[0][0])
            self.assertIn('entries in the `quantile` column must be an int or float in [0, 1]', error_messages[0][1])


    def test_covid_validation_date_alignment(self):
        # test [add additional validations #56] - https://github.com/reichlab/covid19-forecast-hub/issues/56
        # (ensure that people are aligning forecast_date and target_end_date correctly)
        column_index_dict = {'forecast_date': 0, 'target': 1, 'target_end_date': 2, 'location': 3, 'location_name': 4,
                             'type': 5, 'quantile': 6, 'value': 7}  # 2020-04-13-MOBS_NEU-GLEAM_COVID.csv

        # 1/4) for x day ahead targets the target_end_date should be forecast_date + x
        row = ["2020-04-13", "1 day ahead inc hosp", "2020-04-14", "01", "Alabama", "point", "NA",
               "45.824147927692344"]  # ok: +1
        error_messages = covid19_row_validator(column_index_dict, row, True)
        # (0, "invalid location for target. location='01', target='1 day ahead inc hosp'. row=['2020-04-13', '1 day ahead inc hosp', '2020-04-14', '01', 'Alabama', 'point', 'NA', '45.824147927692344']")
        self.assertEqual(0, len(error_messages))

        row = ["2020-04-13", "2 day ahead inc hosp", "2020-04-15", "01", "Alabama", "point", "NA",
               "48.22952942521442"]  # ok: +2
        error_messages = covid19_row_validator(column_index_dict, row, True)
        self.assertEqual(0, len(error_messages))

        row = ["2020-04-13", "1 day ahead inc hosp", "2020-04-15", "01", "Alabama", "point", "NA",
               "45.824147927692344"]  # bad: +2, not 1
        error_messages = covid19_row_validator(column_index_dict, row, True)
        self.assertEqual(1, len(error_messages))
        self.assertEqual(MESSAGE_FORECAST_CHECKS, error_messages[0][0])
        self.assertIn("invalid target_end_date: was not 1 day(s) after forecast_date", error_messages[0][1])

        # 2/4) for x week ahead targets, weekday(target_end_date) should be a Saturday (case: Sun or Mon)
        row = ["2020-04-13", "1 wk ahead cum death", "2020-04-18", "01", "Alabama", "point", "NA",
               "55.800809050176994"]  # ok: Mon -> Sat
        error_messages = covid19_row_validator(column_index_dict, row, True)
        self.assertEqual(0, len(error_messages))

        row = ["2020-04-13", "2 wk ahead cum death", "2020-04-25", "01", "Alabama", "point", "NA",
               "71.82206014865048"]  # ok: Mon -> Sat
        error_messages = covid19_row_validator(column_index_dict, row, True)
        self.assertEqual(0, len(error_messages))

        row = ["2020-04-13", "1 wk ahead cum death", "2020-04-19", "01", "Alabama", "point", "NA",
               "55.800809050176994"]  # bad: target_end_date is a Sun
        error_messages = covid19_row_validator(column_index_dict, row, True)
        self.assertEqual(1, len(error_messages))
        self.assertEqual(MESSAGE_DATE_ALIGNMENT, error_messages[0][0])
        self.assertIn("target_end_date was not a Saturday", error_messages[0][1])

        # 3/4) (case: Sun or Mon) for x week ahead targets, ensure that the 1-week ahead forecast is for the next Sat
        row = ["2020-04-12", "1 wk ahead cum death", "2020-04-18", "01", "Alabama", "point", "NA",
               "55.800809050176994"]  # ok: 1 wk ahead Sun -> Sat
        error_messages = covid19_row_validator(column_index_dict, row, True)
        self.assertEqual(0, len(error_messages))

        row = ["2020-04-13", "1 wk ahead cum death", "2020-04-18", "01", "Alabama", "point", "NA",
               "55.800809050176994"]  # ok: 1 wk ahead Mon -> Sat
        error_messages = covid19_row_validator(column_index_dict, row, True)
        self.assertEqual(0, len(error_messages))

        row = ["2020-04-14", "1 wk ahead cum death", "2020-04-18", "01", "Alabama", "point", "NA",
               "55.800809050176994"]  # bad: 1 wk ahead Tue -> this Sat but s/b next Sat
        error_messages = covid19_row_validator(column_index_dict, row, True)
        self.assertEqual(1, len(error_messages))
        self.assertEqual(MESSAGE_DATE_ALIGNMENT, error_messages[0][0])
        self.assertIn("target_end_date was not the expected Saturday", error_messages[0][1])

        row = ["2020-04-13", "2 wk ahead cum death", "2020-04-25", "01", "Alabama", "point", "NA",
               "71.82206014865048"]  # ok: 2 wk ahead Mon -> next Sat
        error_messages = covid19_row_validator(column_index_dict, row, True)
        self.assertEqual(0, len(error_messages))

        row = ["2020-04-13", "2 wk ahead cum death", "2020-04-18", "01", "Alabama", "point", "NA",
               "71.82206014865048"]  # bad: 2 wk ahead Mon -> next Sat
        error_messages = covid19_row_validator(column_index_dict, row, True)
        self.assertEqual(1, len(error_messages))
        self.assertEqual(MESSAGE_DATE_ALIGNMENT, error_messages[0][0])
        self.assertIn("target_end_date was not the expected Saturday", error_messages[0][1])

        # 4/4) (case: Tue through Sat) for x week ahead targets, ensures that the 1-week ahead forecast is for the Sat after next
        row = ["2020-04-14", "1 wk ahead cum death", "2020-04-25", "01", "Alabama", "point", "NA",
               "55.800809050176994"]  # ok: 1 wk ahead Tue -> next Sat
        error_messages = covid19_row_validator(column_index_dict, row, True)
        self.assertEqual(0, len(error_messages))

        row = ["2020-04-14", "1 wk ahead cum death", "2020-04-18", "01", "Alabama", "point", "NA",
               "55.800809050176994"]  # bad: 1 wk ahead Tue -> this Sat, but should be next Sat (2020-04-25)
        error_messages = covid19_row_validator(column_index_dict, row, True)
        self.assertEqual(1, len(error_messages))
        self.assertEqual(MESSAGE_DATE_ALIGNMENT, error_messages[0][0])
        self.assertIn("target_end_date was not the expected Saturday", error_messages[0][1])

        row = ["2020-04-13", "2 wk ahead cum death", "2020-04-25", "01", "Alabama", "point", "NA",
               "71.82206014865048"]  # ok: 2 wk ahead Mon -> next Sat
        error_messages = covid19_row_validator(column_index_dict, row, True)
        self.assertEqual(0, len(error_messages))

        row = ["2020-04-14", "2 wk ahead cum death", "2020-04-25", "01", "Alabama", "point", "NA",
               "71.82206014865048"]  # bad: 2 wk ahead Tue -> next Sat
        error_messages = covid19_row_validator(column_index_dict, row, True)
        self.assertEqual(1, len(error_messages))
        self.assertEqual(MESSAGE_DATE_ALIGNMENT, error_messages[0][0])
        self.assertIn("target_end_date was not the expected Saturday", error_messages[0][1])


    def test_covid_validation_date_format(self):
        # test that `covid19_row_validator()` checks these columns are YYYY-MM-DD format: forecast_date, target_end_date

        # ok dates: '2020-04-15-Geneva-DeterministicGrowth.csv'
        test_dir = 'tests/covid19-data-processed-examples/'
        with open(test_dir + '2020-04-15-Geneva-DeterministicGrowth.csv') as quantile_fp:
            try:
                _, error_messages = \
                    json_io_dict_from_quantile_csv_file(quantile_fp, COVID_TARGETS, covid19_row_validator,
                                                        addl_req_cols=COVID_ADDL_REQ_COLS)
            except Exception as ex:
                self.fail(f"unexpected exception: {ex}")

        # bad date: '2020-04-15-Geneva-DeterministicGrowth_bad_forecast_date.csv'
        with open(test_dir + '2020-04-15-Geneva-DeterministicGrowth_bad_forecast_date.csv') as quantile_fp:
            _, error_messages = \
                json_io_dict_from_quantile_csv_file(quantile_fp, COVID_TARGETS, covid19_row_validator,
                                                    addl_req_cols=COVID_ADDL_REQ_COLS)
            self.assertEqual(1, len(error_messages))
            self.assertEqual(MESSAGE_FORECAST_CHECKS, error_messages[0][0])
            self.assertIn("invalid forecast_date or target_end_date format", error_messages[0][1])

        # bad date: '2020-04-15-Geneva-DeterministicGrowth_bad_target_end_date.csv'
        with open(test_dir + '2020-04-15-Geneva-DeterministicGrowth_bad_target_end_date.csv') as quantile_fp:
            _, error_messages = \
                json_io_dict_from_quantile_csv_file(quantile_fp, COVID_TARGETS, covid19_row_validator,
                                                    addl_req_cols=COVID_ADDL_REQ_COLS)
            self.assertEqual(1, len(error_messages))
            self.assertEqual(MESSAGE_FORECAST_CHECKS, error_messages[0][0])
            self.assertIn("invalid forecast_date or target_end_date format", error_messages[0][1])


    def test_covid_validation_quantiles(self):
        # tests a quantile not in COVID_QUANTILES_NON_CASE
        column_index_dict = {'forecast_date': 0, 'target': 1, 'target_end_date': 2, 'location': 3, 'location_name': 4,
                             'type': 5, 'quantile': 6, 'value': 7}  # 2020-04-13-MOBS_NEU-GLEAM_COVID.csv

        row = ["2020-04-13", "1 day ahead inc hosp", "2020-04-14", "01", "Alabama", "quantile", "0.1",
               "18.045499696631747"]  # 0.1 is OK (matches 0.100)
        error_messages = covid19_row_validator(column_index_dict, row, True)
        self.assertEqual(0, len(error_messages))

        row = ["2020-04-13", "1 day ahead inc hosp", "2020-04-14", "01", "Alabama", "quantile", "0.11",
               "18.045499696631747"]  # 0.11 bad
        error_messages = covid19_row_validator(column_index_dict, row, True)
        self.assertEqual(1, len(error_messages))
        self.assertEqual(MESSAGE_FORECAST_CHECKS, error_messages[0][0])
        self.assertIn("invalid quantile for target. quantile='0.11'", error_messages[0][1])

        # from 2020-05-17-CovidActNow-SEIR_CAN.csv
        column_index_dict = {'forecast_date': 0, 'location': 1, 'location_name': 2, 'target': 3, 'type': 4,
                             'target_end_date': 5, 'quantile': 6, 'value': 7}
        row = ['2020-05-17', '01', 'Alabama', '1 day ahead inc hosp', 'quantile', '2020-05-18', '0.010',
               '-29.859790255308283']  # quantile not >= 0
        error_messages = covid19_row_validator(column_index_dict, row, True)
        # [(0, "invalid location for target. location='01', target='1 day ahead inc hosp'.      row=['2020-05-17', '01', 'Alabama', '1 day ahead inc hosp', 'quantile', '2020-05-18', '0.010', '-29.859790255308283']"),
        #  (0, "entries in the `value` column must be non-negative. value='-29.859790255308283'. row=['2020-05-17', '01', 'Alabama', '1 day ahead inc hosp', 'quantile', '2020-05-18', '0.010', '-29.859790255308283']"),
        #  (0, "invalid quantile for target. quantile='0.010', target='1 day ahead inc hosp'.   row=['2020-05-17', '01', 'Alabama', '1 day ahead inc hosp', 'quantile', '2020-05-18', '0.010', '-29.859790255308283']")]
        self.assertEqual(1, len(error_messages))
        self.assertEqual(MESSAGE_FORECAST_CHECKS, error_messages[0][0])
        self.assertIn("entries in the `value` column must be non-negative", error_messages[0][1])

        column_index_dict = {'forecast_date': 0, 'target': 1, 'target_end_date': 2, 'location': 3, 'location_name': 4,
                             'type': 5, 'quantile': 6, 'value': 7}  # from 2020-06-21-USC-SI_kJalpha.csv
        row = ['2020-06-21', '2 day ahead inc hosp', '2020-06-23', '31', 'Nebraska', 'Point', 'NA',
               '-0.02443515411698627']  # value not >= 0
        error_messages = covid19_row_validator(column_index_dict, row, True)
        self.assertEqual(1, len(error_messages))
        self.assertEqual(MESSAGE_FORECAST_CHECKS, error_messages[0][0])
        self.assertIn("entries in the `value` column must be non-negative", error_messages[0][1])


    def test_json_io_dict_from_quantile_csv_file_bad_covid_fips_code(self):
        for csv_file in ['quantiles-bad-row-fip-one-digit.csv',
                         'quantiles-bad-row-fip-three-digits.csv',
                         'quantiles-bad-row-fip-bad-two-digits.csv']:
            with open('tests/' + csv_file) as quantile_fp:
                _, error_messages = \
                    json_io_dict_from_quantile_csv_file(quantile_fp, COVID_TARGETS, covid19_row_validator,
                                                        COVID_ADDL_REQ_COLS)
            self.assertEqual(1, len(error_messages))
            self.assertEqual(MESSAGE_FORECAST_CHECKS, error_messages[0][0])
            self.assertIn("invalid location for target", error_messages[0][1])


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

        # expose a bug where the last row from `csv_rows_from_json_io_dict()` was lost due to pop()
        with patch('zoltpy.csv_io.csv_rows_from_json_io_dict') as mock:
            mock.return_value = [CSV_HEADER,
                                 ['location1', 'pct next week', 'point', 2.1, '', '', '', '', '', '', '', ''],
                                 ['location2', 'pct next week', 'quantile', 1.0, '', '', '', 0.025, '', '', '', '']]
            act_rows = quantile_csv_rows_from_json_io_dict(json_io_dict)
            mock.assert_called_once()

            exp_rows = [['location', 'target', 'type', 'quantile', 'value'],
                        ['location1', 'pct next week', 'point', '', 2.1],
                        ['location2', 'pct next week', 'quantile', 0.025, 1.0]]
            self.assertEqual(exp_rows, act_rows)


    def test_county_cases(self):
        # test blue sky
        with open('tests/county-examples/correct.csv') as quantile_fp:
            _, error_messages = json_io_dict_from_quantile_csv_file(quantile_fp,
                                                                    COVID_TARGETS, covid19_row_validator,
                                                                    addl_req_cols=COVID_ADDL_REQ_COLS)
        self.assertEqual(0, len(error_messages))

        # test invalid combinations
        file_exp_num_errors_messages = [
            ('invalid-inc-hosp-target-for-county.csv', 8, 'invalid location for target'),
            ('invalid-quantiles-for-case-target.csv', 16, 'invalid quantile for target'),
            ('invalid-wk-cum-death-target-for-county.csv', 8, 'invalid location for target'),
            ('invalid-wk-inc-death-target-for-county.csv', 8, 'invalid location for target'),
        ]
        for quantile_file, exp_num_errors, exp_message in file_exp_num_errors_messages:
            with open('tests/county-examples/' + quantile_file) as quantile_fp:
                _, error_messages = json_io_dict_from_quantile_csv_file(quantile_fp, COVID_TARGETS,
                                                                        covid19_row_validator,
                                                                        addl_req_cols=COVID_ADDL_REQ_COLS)
                self.assertEqual(exp_num_errors, len(error_messages))
                self.assertEqual(MESSAGE_FORECAST_CHECKS, error_messages[0][0])
                self.assertIn(exp_message, error_messages[0][1])  # arbitrarily pick first message. all are similar


# todo move to test_util.py
def test_dataframe_from_json_io_dict(self):
    with open('tests/docs-predictions.json') as fp:
        json_io_dict = json.load(fp)

    df = dataframe_from_json_io_dict(json_io_dict)
    self.assertEqual((64, 12), df.shape)
