import json
from unittest import TestCase
from unittest.mock import patch

from zoltpy.covid19 import validate_config_dict, validate_quantile_csv_file


class CdcIOTestCase(TestCase):
    """
    """


    def test_validate_quantile_csv_file_calls_validate_config_dict(self):
        validation_config = {'target_groups':
                                 [{"outcome_variable": "inc flu hosp", "targets": [], "locations": [], "quantiles": []}]}
        with patch('zoltpy.covid19.validate_config_dict') as validate_config_mock:
            validate_quantile_csv_file('tests/quantile-predictions.csv', validation_config)
            validate_config_mock.assert_called_once_with(validation_config)

        validation_config = {'target_groups': [
            {"outcome_variable": "inc flu hosp", "targets": [], "locations": [], "quantiles": ['not a number']}]}
        error_messages = validate_quantile_csv_file('tests/quantile-predictions.csv', validation_config)
        self.assertEqual(1, len(error_messages))
        self.assertIn("invalid validation_config", error_messages[0])


    def test_validate_config_dict(self):
        # case: not a dict
        with self.assertRaisesRegex(RuntimeError, "validation_config was not a dict"):
            validate_config_dict(None)

        # case: dict but no 'target_groups' key
        with self.assertRaisesRegex(RuntimeError, "validation_config did not contain 'target_groups' key"):
            validate_config_dict({})

        # case: has 'target_groups', but not a list
        with self.assertRaisesRegex(RuntimeError, "'target_groups' was not a list"):
            validate_config_dict({'target_groups': None})

        # case: dict with one 'target_groups', but not all keys present in it
        with self.assertRaisesRegex(RuntimeError, "one or more target group keys was missing"):
            validate_config_dict({'target_groups': [{}]})

        # case: dict with one 'target_groups' with all keys present, but targets, locations, and quantiles not lists
        bad_target_groups = [{"outcome_variable": "inc flu hosp", "targets": 'not a list', "locations": [], "quantiles": []},
                             {"outcome_variable": "inc flu hosp", "targets": [], "locations": 'not a list', "quantiles": []},
                             {"outcome_variable": "inc flu hosp", "targets": [], "locations": [], "quantiles": 'not a list'}]
        for bad_target_group in bad_target_groups:
            with self.assertRaisesRegex(RuntimeError, "one of these fields was not a list"):
                validate_config_dict({'target_groups': [bad_target_group]})

        # case: dict with one 'target_groups', but its name is not a string
        with self.assertRaisesRegex(RuntimeError, "'outcome_variable' field was not a string"):
            validate_config_dict({'target_groups': [{"outcome_variable": None, "targets": [], "locations": [], "quantiles": []}]})

        # case: dict with one 'target_groups' with all keys present, but targets or locations contain non-strings
        bad_target_groups = [{"outcome_variable": "inc flu hosp", "targets": [-1], "locations": [], "quantiles": []},
                             {"outcome_variable": "inc flu hosp", "targets": [], "locations": [-1], "quantiles": []}]
        for bad_target_group in bad_target_groups:
            with self.assertRaisesRegex(RuntimeError, "one of these fields contained non-strings"):
                validate_config_dict({'target_groups': [bad_target_group]})

        # case: dict with one 'target_groups' with all keys present, but quantiles contains non-numbers
        with self.assertRaisesRegex(RuntimeError, "'quantiles' field contained non-numbers"):
            validate_config_dict({'target_groups': [
                {"outcome_variable": "inc flu hosp", "targets": [], "locations": [], "quantiles": ['not a number']}]})

        # case: blue sky
        try:
            validate_config_dict({'target_groups':
                                      [{"outcome_variable": "inc flu hosp", "targets": [], "locations": [], "quantiles": []}]})
        except Exception as ex:
            self.fail(f"unexpected exception: {ex}")

        # case: load from file
        with open('tests/covid-project-config.json', 'r') as fp:
            validation_config = json.load(fp)
            try:
                validate_config_dict(validation_config)
            except Exception as ex:
                self.fail(f"unexpected exception: {ex}")
