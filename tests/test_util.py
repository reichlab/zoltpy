import json
from unittest import TestCase
from unittest.mock import patch

from tests.test_connection import PROJECTS_LIST_DICTS, mock_authenticate
from zoltpy.connection import ZoltarConnection
from zoltpy.util import delete_forecast, download_forecast, dataframe_from_json_io_dict


class UtilTestCase(TestCase):
    """
    todo util tests to implement:

    - for any that search by project or model name: case where doesn't exist
    - create_project():
      - no existing project
      - calls existing_project.delete()
      - POSTs '{conn.host}/api/projects/'
      - returns a Project()
    - delete_model():
      - no existing project
      - no existing model
      - calls model.delete()
    - ! upload_forecast():
      - no existing project
      - no existing model
      - calls model.upload_forecast()
      - calls busy_poll_job()
    - ! upload_forecast_batch():
      - no existing project
      - no existing model
      - 'batch args had different lengths'
      - 'no forecasts to upload'
      - can take filename string or json_io_dict
      - calls model.upload_forecast() on each
      - returns the last job
    - dataframe_from_json_io_dict():
      - input examples/docs-predictions.json
      - test output df
    """


    def test_download_forecast(self):
        def json_for_uri_mock_side_effect(*args, **kwargs):  # returns a sequence of return args
            return {'http://example.com/api/projects/': PROJECTS_LIST_DICTS,
                    'http://example.com/api/project/3/models/': [MODEL_DICT],
                    'https://example.com/api/model/150/forecasts/': [FORECAST_DICT]}[args[0]]


        conn = mock_authenticate(ZoltarConnection('http://example.com'))
        with patch('zoltpy.connection.ZoltarConnection.json_for_uri') as json_for_uri_mock, \
                patch('zoltpy.connection.ZoltarConnection.re_authenticate_if_necessary'), \
                patch('zoltpy.connection.Forecast.data') as forecast_data_mock:
            json_for_uri_mock.side_effect = json_for_uri_mock_side_effect

            project_name = "Docs Example Project"
            model_abbr = "60-contact"
            timezero_date = "2020-04-12"

            # case: blue sky
            download_forecast(conn, project_name, model_abbr, timezero_date)
            forecast_data_mock.assert_called_once()

            # case: bad project name
            forecast_data_mock.reset_mock()
            with self.assertRaises(RuntimeError) as context:
                download_forecast(conn, project_name + 'bad', model_abbr, timezero_date)
            self.assertIn('found no project named', str(context.exception))

            # case: bad model name
            forecast_data_mock.reset_mock()
            with self.assertRaises(RuntimeError) as context:
                download_forecast(conn, project_name, model_abbr + 'bad', timezero_date)
            self.assertIn('found no model named', str(context.exception))

            # case: bad timezero_date
            forecast_data_mock.reset_mock()
            with self.assertRaises(RuntimeError) as context:
                download_forecast(conn, project_name, model_abbr, "2020-02-22")
            self.assertIn('found no forecast with timezero date', str(context.exception))


    def test_delete_forecast(self):
        def json_for_uri_mock_side_effect(*args, **kwargs):  # returns a sequence of return args
            return {'http://example.com/api/projects/': PROJECTS_LIST_DICTS,
                    'http://example.com/api/project/3/models/': [MODEL_DICT],
                    'https://example.com/api/model/150/forecasts/': [FORECAST_DICT]}[args[0]]


        conn = mock_authenticate(ZoltarConnection('http://example.com'))
        with patch('zoltpy.connection.ZoltarConnection.json_for_uri') as json_for_uri_mock, \
                patch('zoltpy.connection.ZoltarConnection.re_authenticate_if_necessary'), \
                patch('zoltpy.connection.Forecast.delete') as delete_forecast_mock:
            json_for_uri_mock.side_effect = json_for_uri_mock_side_effect

            # case: finds existing_forecast
            delete_forecast(conn, PROJECTS_LIST_DICTS[0]['name'], MODEL_DICT['name'], '2020-04-12')
            self.assertEqual(1, delete_forecast_mock.call_count)

            # case: does not find existing_forecast
            delete_forecast_mock.reset_mock()
            delete_forecast(conn, PROJECTS_LIST_DICTS[0]['name'], MODEL_DICT['name'], '2020-04-22')
            self.assertEqual(0, delete_forecast_mock.call_count)


    def test_dataframe_from_json_io_dict(self):
        with open('tests/docs-predictions.json') as fp:
            json_io_dict = json.load(fp)

        df = dataframe_from_json_io_dict(json_io_dict)
        self.assertEqual((64, 12), df.shape)


FORECAST_DICT = {
    "id": 9921,
    "url": "https://example.com/api/forecast/9921/",
    "forecast_model": "https://example.com/api/model/150/",
    "source": "2020-04-12-CU-60contact.csv",
    "time_zero": {
        "id": 609,
        "url": "https://example.com/api/timezero/609/",
        "timezero_date": "2020-04-12",
        "data_version_date": None,
        "is_season_start": False
    },
    "created_at": "2020-05-05T14:37:59.446110-04:00",
    "issued_at": "2011-10-02",
    "notes": "",
    "forecast_data": "https://example.com/api/forecast/9921/data/"
}

MODEL_DICT = {
    "id": 150,
    "url": "https://example.com/api/model/150/",
    "project": "https://example.com/api/project/44/",
    "owner": "https://example.com/api/user/7/",
    "name": "60-contact",
    "abbreviation": "60-contact",
    "description": "This model makes predictions about the future that are dependent on a particular set \r\n    of assumptions about how interventions are implemented and how effective they are. \r\n    Estimates of spatio-temporal COVID-19 demand and medical system critical care supply were \r\n    calculated for all continental US counties. These estimates were statistically summarized \r\n    and mapped for US counties, regions and urban versus non-urban areas. Estimates of COVID-19 \r\n    infections and patients needing critical care were calculated for 21-day and 42-day time periods \r\n    starting from April 2, 2020 to May 13, 2020 for a reactive pattern of 40% contact reduction \r\n    (\"60contact\") through actions such as social distancing. Multiple national public and private \r\n    datasets were linked and harmonized in order to calculate county-level hospital critical care \r\n    bed counts that include currently available beds and those that could be made available under \r\n    four surge response scenarios – very low, low, medium, and high – as well as deaths in counties \r\n    that had exceeded their hospital critical care capacity limits.\r\ncitation: \"Sen Pei, Jeffrey Shaman, Initial Simulation of SARS-CoV2 Spread and Intervention Effects in the Continental US. medRxiv.doi: https://doi.org/10.1101/2020.03.21.20040303;  Flattening the curve before it flattens us: hospital critical care capacity limits and mortality from novel coronavirus (SARS-CoV2) cases in US counties. https://behcolumbia.files.wordpress.com/2020/04/flattening-the-curve-before-it-flattens-us-20200405b.pdf\"",
    "home_url": "https://github.com/reichlab/covid19-forecast-hub/tree/master/data-processed/CU-60contact",
    "aux_data_url": None,
    "forecasts": [
        "https://example.com/api/forecast/9921/",
        "https://example.com/api/forecast/9922/",
        "https://example.com/api/forecast/10068/",
        "https://example.com/api/forecast/10069/",
        "https://example.com/api/forecast/10070/"
    ]
}
