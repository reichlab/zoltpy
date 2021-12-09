from unittest import TestCase

from zoltpy.covid19 import validate_config_dict


class CdcIOTestCase(TestCase):
    """
    """


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
        bad_target_groups = [{"name": "inc flu hosp", "targets": 'not a list', "locations": [], "quantiles": []},
                             {"name": "inc flu hosp", "targets": [], "locations": 'not a list', "quantiles": []},
                             {"name": "inc flu hosp", "targets": [], "locations": [], "quantiles": 'not a list'}]
        for bad_target_group in bad_target_groups:
            with self.assertRaisesRegex(RuntimeError, "one of these fields was not a list"):
                validate_config_dict({'target_groups': [bad_target_group]})

        # case: dict with one 'target_groups', but its name is not a string
        with self.assertRaisesRegex(RuntimeError, "'name' field was not a string"):
            validate_config_dict({'target_groups': [{"name": None, "targets": [], "locations": [], "quantiles": []}]})

        # case: blue sky
        try:
            validate_config_dict({'target_groups':
                                      [{"name": "inc flu hosp", "targets": [], "locations": [], "quantiles": []}]})
        except Exception as ex:
            self.fail(f"unexpected exception: {ex}")
