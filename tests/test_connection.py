import base64
import datetime
import json
import unittest
from unittest import mock
from unittest.mock import patch, MagicMock

import dateutil

from zoltpy.cdc_io import YYYY_MM_DD_DATE_FORMAT
from zoltpy.connection import ZoltarConnection, ZoltarSession, ZoltarResource, Project, Model, Unit, Target, TimeZero, \
    Forecast, Job, QueryType


class ConnectionTestCase(unittest.TestCase):
    """
    """


    def test_authenticate(self):
        conn = ZoltarConnection('')
        username = 'Z_USERNAME'
        password = 'Z_PASSWORD'
        mock_authenticate(conn, username, password)
        with patch('requests.post') as post_mock:
            post_mock.return_value.status_code = 200
            post_mock.return_value.json = MagicMock(return_value={'token': MOCK_TOKEN})
            conn.authenticate(username, password)
            self.assertEqual(username, conn.username)
            self.assertEqual(password, conn.password)
            self.assertIsInstance(conn.session, ZoltarSession)
            self.assertEqual(MOCK_TOKEN, conn.session.token)
            post_mock.assert_called_once_with('/api-token-auth/', {'username': 'Z_USERNAME', 'password': 'Z_PASSWORD'})


    def test_id_for_uri(self):
        self.assertEqual(71, ZoltarResource.id_for_uri('http://example.com/api/forecast/71'))  # no trailing '/'
        self.assertEqual(71, ZoltarResource.id_for_uri('http://example.com/api/forecast/71/'))


    def test_json_for_uri_calls_re_authenticate_if_necessary(self):
        with patch('zoltpy.connection.ZoltarConnection.re_authenticate_if_necessary') as re_auth_mock, \
                patch('requests.get') as get_mock:
            get_mock.return_value.status_code = 200
            conn = mock_authenticate(ZoltarConnection('http://example.com'))
            conn.json_for_uri('/')
            re_auth_mock.assert_called_once()


    def test_delete_calls_re_authenticate_if_necessary(self):
        with patch('zoltpy.connection.ZoltarConnection.re_authenticate_if_necessary') as re_auth_mock, \
                patch('zoltpy.connection.ZoltarConnection.json_for_uri') as json_for_uri_mock, \
                patch('requests.delete') as delete_mock:
            json_for_uri_mock.return_value = PROJECTS_LIST_DICTS
            delete_mock.return_value.status_code = 200
            conn = mock_authenticate(ZoltarConnection('http://example.com'))
            projects = conn.projects
            projects[0].delete()
            re_auth_mock.assert_called_once()


    @mock.patch('zoltpy.connection.ZoltarConnection.json_for_uri')
    def test_verify_instance_api_hits(self, json_for_uri_mock):
        json_for_uri_mock.return_value = PROJECTS_LIST_DICTS
        conn = ZoltarConnection('https://example.com')
        projects = conn.projects
        json_for_uri_mock.assert_called_once_with('https://example.com/api/projects/')

        json_for_uri_mock.reset_mock()
        models = projects[0].models
        json_for_uri_mock.assert_called_once_with('http://example.com/api/project/3/models/')

        json_for_uri_mock.reset_mock()
        _ = projects[0].units
        json_for_uri_mock.assert_called_once_with('http://example.com/api/project/3/units/')

        json_for_uri_mock.reset_mock()
        _ = projects[0].targets
        json_for_uri_mock.assert_called_once_with('http://example.com/api/project/3/targets/')

        json_for_uri_mock.reset_mock()
        _ = projects[0].timezeros
        json_for_uri_mock.assert_called_once_with('http://example.com/api/project/3/timezeros/')

        json_for_uri_mock.return_value = MODELS_LIST_DICTS[0]
        json_for_uri_mock.reset_mock()
        _ = models[0].name
        json_for_uri_mock.assert_not_called()


    @mock.patch('zoltpy.connection.ZoltarConnection.json_for_uri')
    def test_instances(self, json_for_uri_mock):
        json_for_uri_mock.return_value = PROJECTS_LIST_DICTS
        conn = ZoltarConnection()

        # test ZoltarConnection.projects
        projects = conn.projects  # hits /api/projects/
        self.assertEqual(2, len(projects))

        project_0 = projects[0]
        self.assertIsInstance(project_0, Project)
        self.assertEqual("Docs Example Project", project_0.name)

        # test Project.models
        json_for_uri_mock.return_value = MODELS_LIST_DICTS
        models = project_0.models  # hits api/project/3/models/
        self.assertEqual(1, len(models))

        model_0 = models[0]
        self.assertIsInstance(model_0, Model)
        self.assertEqual("docs forecast model", model_0.name)

        # test Project.units
        json_for_uri_mock.return_value = UNITS_LIST_DICTS
        units = project_0.units  # hits api/project/3/units/
        self.assertEqual(3, len(units))

        unit_0 = units[0]
        self.assertIsInstance(unit_0, Unit)
        self.assertEqual("location1", unit_0.name)

        # test Project.targets
        json_for_uri_mock.return_value = TARGETS_LIST_DICTS
        targets = project_0.targets  # hits api/project/3/targets/
        self.assertEqual(2, len(targets))

        target_0 = targets[0]
        self.assertIsInstance(target_0, Target)
        self.assertEqual("pct next week", target_0.name)

        # test Project.timezeros
        json_for_uri_mock.return_value = TIMEZEROS_LIST_DICTS
        timezeros = project_0.timezeros  # hits api/project/3/timezeros/
        self.assertEqual(3, len(timezeros))

        timezero_0 = timezeros[0]
        self.assertIsInstance(timezero_0, TimeZero)  # "2011-10-02"
        self.assertIsInstance(timezero_0.timezero_date, datetime.date)
        self.assertIsInstance(timezero_0.data_version_date, datetime.date)
        self.assertEqual(datetime.date(2011, 10, 2), timezero_0.timezero_date)
        self.assertEqual(datetime.date(2011, 10, 22), timezero_0.data_version_date)

        timezero_1 = timezeros[1]  # None
        self.assertIsInstance(timezero_1.timezero_date, datetime.date)
        self.assertEqual(datetime.date(2011, 10, 9), timezero_1.timezero_date)
        self.assertEqual(None, timezero_1.data_version_date)

        # test Project.created_at
        json_for_uri_mock.return_value = {
            "id": 44,
            "url": "https://www.zoltardata.com/api/project/44/truth/",
            "project": "https://www.zoltardata.com/api/project/44/",
            "source": "zoltar-truth.csv",
            "created_at": "2021-06-16T21:06:37.893283+00:00",
            "issued_at": "2021-06-16T21:06:37.851554+00:00"}
        created_at = project_0.truth_created_at
        self.assertIsInstance(created_at, datetime.datetime)
        self.assertEqual(dateutil.parser.parse("2021-06-16T21:06:37.893283+00:00"), created_at)

        issued_at = project_0.truth_issued_at
        self.assertIsInstance(issued_at, datetime.datetime)
        self.assertEqual(dateutil.parser.parse("2021-06-16T21:06:37.851554+00:00"), issued_at)

        # test Model.forecasts
        json_for_uri_mock.return_value = FORECASTS_LIST_DICTS
        forecasts = model_0.forecasts  # hits api/model/5/forecasts/
        self.assertEqual(2, len(forecasts))

        # test Model.latest_forecast
        json_for_uri_mock.return_value = FORECASTS_LIST_DICTS
        latest_forecast = model_0.latest_forecast  # hits api/model/5/forecasts/
        self.assertIsInstance(latest_forecast, Forecast)
        self.assertEqual('2020-08-17', latest_forecast.timezero.timezero_date.strftime(YYYY_MM_DD_DATE_FORMAT))

        forecast_0 = forecasts[0]
        self.assertIsInstance(forecast_0, Forecast)
        self.assertEqual('2020-08-17-COVIDhub-ensemble.csv', forecast_0.source)


    @mock.patch('zoltpy.connection.ZoltarConnection.json_for_uri')
    def test_upload_truth_data(self, json_for_uri_mock):
        conn = mock_authenticate(ZoltarConnection('http://example.com'))
        project = Project(conn, 'http://example.com/api/project/3/')
        with open('tests/job-2.json') as ufj_fp, \
                open('tests/docs-ground-truth.csv') as csv_fp, \
                patch('requests.post') as post_mock, \
                patch('zoltpy.connection.ZoltarConnection.re_authenticate_if_necessary') as re_auth_mock:
            job_json = json.load(ufj_fp)
            post_mock.return_value.status_code = 200
            post_mock.return_value.json = MagicMock(return_value=job_json)
            act_job_json = project.upload_truth_data(csv_fp)
            re_auth_mock.assert_called_once()
            self.assertEqual(1, post_mock.call_count)
            self.assertEqual('http://example.com/api/project/3/truth/', post_mock.call_args[0][0])
            self.assertIsInstance(act_job_json, Job)
            self.assertEqual(job_json['url'], act_job_json.uri)


    @mock.patch('zoltpy.connection.ZoltarConnection.json_for_uri')
    def test_upload_forecast(self, json_for_uri_mock):
        conn = mock_authenticate(ZoltarConnection('http://example.com'))
        with open('tests/job-2.json') as ufj_fp, \
                open("examples/example-model-config.json") as fp, \
                patch('requests.post') as post_mock, \
                patch('zoltpy.connection.ZoltarConnection.re_authenticate_if_necessary') as re_auth_mock:
            job_json = json.load(ufj_fp)
            model_config = json.load(fp)
            model_config['url'] = 'http://example.com/api/model/5/'
            post_mock.return_value.status_code = 200
            post_mock.return_value.json = MagicMock(return_value=job_json)
            forecast_model = Model(conn, model_config['url'], model_config)
            act_job_json = forecast_model.upload_forecast({}, None, None)
            re_auth_mock.assert_called_once()
            self.assertEqual(1, post_mock.call_count)
            self.assertEqual('http://example.com/api/model/5/forecasts/', post_mock.call_args[0][0])
            self.assertIsInstance(act_job_json, Job)
            self.assertEqual(job_json['url'], act_job_json.uri)


    def test_create_timezero(self):
        conn = mock_authenticate(ZoltarConnection('http://example.com'))
        with patch('zoltpy.connection.ZoltarConnection.json_for_uri', return_value=PROJECTS_LIST_DICTS), \
             patch('requests.post') as post_mock, \
                patch('zoltpy.connection.ZoltarConnection.re_authenticate_if_necessary') as re_auth_mock:
            project = conn.projects[0]
            post_mock.return_value.status_code = 200
            post_return_value = {"id": 705,
                                 "url": "http://example.com/api/timezero/705/",
                                 "timezero_date": "2011-10-02",
                                 "data_version_date": "2011-10-03",
                                 "is_season_start": True,
                                 "season_name": "2011-2012"}
            post_mock.return_value.json = MagicMock(return_value=post_return_value)
            project.create_timezero("2011-10-02", "2011-10-03", True, "2011-2012")
            post_mock.assert_called_once()
            exp_timezero_config = dict(post_return_value)  # copy
            del exp_timezero_config['id']
            del exp_timezero_config['url']
            act_timezero_config = post_mock.call_args[1]['json']['timezero_config']
            self.assertEqual(exp_timezero_config, act_timezero_config)
            re_auth_mock.assert_called_once()


    @mock.patch('zoltpy.connection.ZoltarConnection.json_for_uri')
    def test_submit_and_download_forecast_query(self, json_for_uri_mock):
        json_for_uri_mock.return_value = PROJECTS_LIST_DICTS
        conn = mock_authenticate(ZoltarConnection('http://example.com'))
        project = conn.projects[0]

        with open('tests/job-submit-query.json') as job_submit_json_fp, \
                patch('requests.post') as post_mock, \
                patch('zoltpy.connection.ZoltarConnection.re_authenticate_if_necessary'):
            # test submit
            query = {}  # all forecasts
            job_submit_json = json.load(job_submit_json_fp)
            post_mock.return_value.status_code = 200
            post_mock.return_value.json = MagicMock(return_value=job_submit_json)
            job = project.submit_query(QueryType.FORECASTS, query)
            self.assertEqual('http://example.com/api/project/3/forecast_queries/', post_mock.call_args[0][0])
            self.assertEqual(post_mock.call_args[1]['json'], {'query': query})
            self.assertIsInstance(job, Job)

            # test download
            json_for_uri_mock.reset_mock(return_value=True)
            json_for_uri_mock.return_value.content = b'unit,target,class,value,cat,prob,sample,quantile,family,param1,param2,param3\r\nlocation1,season severity,bin,,moderate,0.1,,,,,,\r\n'
            rows = job.download_data()
            json_for_uri_mock.assert_called_once_with('http://127.0.0.1:8000/api/job/44/data/', False, 'text/csv')
            self.assertEqual(2, len(rows))
            exp_rows = [['unit', 'target', 'class', 'value', 'cat', 'prob', 'sample', 'quantile',
                         'family', 'param1', 'param2', 'param3'],
                        ['location1', 'season severity', 'bin', '', 'moderate', '0.1', '', '',
                         '', '', '', '']]
            self.assertEqual(exp_rows, rows)


    @mock.patch('zoltpy.connection.ZoltarConnection.json_for_uri')
    def test_submit_truth_query(self, json_for_uri_mock):
        json_for_uri_mock.return_value = PROJECTS_LIST_DICTS
        conn = mock_authenticate(ZoltarConnection('http://example.com'))
        project = conn.projects[0]

        with open('tests/job-submit-query.json') as job_submit_json_fp, \
                patch('requests.post') as post_mock, \
                patch('zoltpy.connection.ZoltarConnection.re_authenticate_if_necessary'):
            # test submit
            query = {}  # all forecasts
            job_submit_json = json.load(job_submit_json_fp)
            post_mock.return_value.status_code = 200
            post_mock.return_value.json = MagicMock(return_value=job_submit_json)
            job = project.submit_query(QueryType.TRUTH, query)
            self.assertEqual('http://example.com/api/project/3/truth_queries/', post_mock.call_args[0][0])
            self.assertEqual(post_mock.call_args[1]['json'], {'query': query})
            self.assertIsInstance(job, Job)


    @mock.patch('zoltpy.connection.ZoltarConnection.json_for_uri')
    def test_edit_model(self, json_for_uri_mock):
        conn = ZoltarConnection()
        mock_authenticate(conn, '', '')

        json_for_uri_mock.return_value = PROJECTS_LIST_DICTS
        projects = conn.projects  # hits /api/projects/
        project_0 = projects[0]

        json_for_uri_mock.return_value = MODELS_LIST_DICTS
        models = project_0.models  # hits api/project/3/models/
        model_0 = models[0]

        with open('examples/example-model-config.json') as fp:
            model_config = json.load(fp)

        # case: blue sky
        with patch('requests.put') as put_mock:
            put_mock.return_value.status_code = 200
            model_0.edit(model_config)
            put_mock.assert_called_once_with('http://example.com/api/model/5/', json={'model_config': model_config},
                                             headers={'Authorization': f'JWT {MOCK_TOKEN}'})


    @mock.patch('zoltpy.connection.ZoltarConnection.json_for_uri')
    def test_create_model(self, json_for_uri_mock):
        with open('examples/example-model-config.json') as fp:
            model_config = json.load(fp)
            model_config['url'] = 'http://example.com/api/model/5/'
        conn = mock_authenticate(ZoltarConnection('http://example.com'))
        project = Project(conn, 'http://example.com/api/project/3/')
        with patch('requests.post') as post_mock, \
                patch('zoltpy.connection.ZoltarConnection.re_authenticate_if_necessary') as re_auth_mock:
            post_mock.return_value.status_code = 200
            post_mock.return_value.json = MagicMock(return_value=model_config)
            new_model = project.create_model(model_config)
            self.assertEqual(1, post_mock.call_count)
            self.assertEqual('http://example.com/api/project/3/models/', post_mock.call_args[0][0])
            self.assertIsInstance(new_model, Model)
            re_auth_mock.assert_called_once()


    def test_forecasts_set_source(self):
        from tests.test_util import FORECAST_DICT  # avoid circular imports


        conn = mock_authenticate(ZoltarConnection('http://example.com'))  # default token (mock_token) is expired
        with patch('requests.patch') as patch_mock, \
                patch('zoltpy.connection.ZoltarConnection.re_authenticate_if_necessary') as re_auth_mock, \
                patch('zoltpy.connection.ZoltarConnection.json_for_uri') as json_for_uri_mock:
            patch_mock.return_value.status_code = 200
            forecast = Forecast(conn, "http://example.com/api/forecast/3/", FORECAST_DICT)
            self.assertEqual(FORECAST_DICT['source'], forecast.source)

            new_source = 'new source'
            new_forecast_dict = dict(FORECAST_DICT)  # non-deep copy OK
            new_forecast_dict['source'] = new_source
            forecast.source = new_source  # call setter. does not refresh
            json_for_uri_mock.return_value = new_forecast_dict
            forecast.refresh()
            self.assertEqual(new_source, forecast.source)


    def test_forecasts_set_issued_at(self):
        from tests.test_util import FORECAST_DICT  # avoid circular imports


        conn = mock_authenticate(ZoltarConnection('http://example.com'))  # default token (mock_token) is expired
        with patch('requests.patch') as patch_mock, \
                patch('zoltpy.connection.ZoltarConnection.re_authenticate_if_necessary') as re_auth_mock, \
                patch('zoltpy.connection.ZoltarConnection.json_for_uri') as json_for_uri_mock:
            patch_mock.return_value.status_code = 200
            forecast = Forecast(conn, "http://example.com/api/forecast/3/", FORECAST_DICT)
            self.assertEqual(FORECAST_DICT['issued_at'], forecast.issued_at)

            new_issued_at = 'new issued_at'  # type isn't checked locally, just remotely
            new_forecast_dict = dict(FORECAST_DICT)  # non-deep copy OK
            new_forecast_dict['issued_at'] = new_issued_at
            forecast.issued_at = new_issued_at  # call setter. does not refresh
            json_for_uri_mock.return_value = new_forecast_dict
            forecast.refresh()
            self.assertEqual(new_issued_at, forecast.issued_at)


    def test_is_token_expired(self):
        # test an expired token
        conn = mock_authenticate(ZoltarConnection('http://example.com'))  # default token (mock_token) is expired
        self.assertTrue(conn.session.is_token_expired())

        # construct and test an unexpired token
        token_split = MOCK_TOKEN.split('.')  # 3 parts: header, payload, signature
        old_header = token_split[0]
        old_signature = token_split[1]

        # round to exclude decimal portion - throws off some JWT tools:
        ten_min_from_now = round((datetime.datetime.utcnow() + datetime.timedelta(minutes=10)).timestamp())
        new_payload = {'user_id': 3, 'username': 'model_owner1', 'exp': ten_min_from_now, 'email': ''}
        new_payload_json = json.dumps(new_payload)
        payload_b64 = base64.b64encode(new_payload_json.encode('utf_8'))
        unexpired_token = f"{old_header}.{payload_b64.decode('utf-8')}.{old_signature}"
        conn.session.token = unexpired_token
        self.assertFalse(conn.session.is_token_expired())


#
# mock_authenticate()
#

# MOCK_TOKEN is an expired token as returned by zoltar. decoded contents:
# - header:  {"typ": "JWT", "alg": "HS256"}
# - payload: {"user_id": 3, "username": "model_owner1", "exp": 1558442805, "email": ""}
# - expiration:
#   05/21/2019 @ 12:46pm               (UTC)
#   2019-05-21T12:46:45+00:00          (ISO 8601)
#   Tuesday, May 21, 2019 12:46:45 PM  (GMT)
#   datetime(2019, 5, 21, 12, 46, 45)  (python): datetime.utcfromtimestamp(1558442805)
MOCK_TOKEN = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJ1c2VyX2lkIjozLCJ1c2VybmFtZSI6Im1vZGVsX293bmVyMSIsImV4cCI6MTU1ODQ0MjgwNSwiZW1haWwiOiIifQ.o03V2RxkFpA5ThhRAidwDWCdcQNeJzr1wwFkOFKUI74"


def mock_authenticate(conn, username='', password=''):
    with patch('requests.post') as post_mock:
        post_mock.return_value.status_code = 200
        post_mock.return_value.json = MagicMock(return_value={'token': MOCK_TOKEN})
        conn.authenticate(username, password)
        return conn


#
# test data
#


PROJECTS_LIST_DICTS = [
    {
        "id": 3,
        "url": "http://example.com/api/project/3/",
        "owner": None,
        "is_public": True,
        "name": "Docs Example Project",
        "description": "d2",
        "home_url": "https://reichlab.io",
        "time_interval_type": "Day",
        "visualization_y_label": "v1",
        "core_data": "",
        "truth": "http://example.com/api/project/3/truth/",
        "model_owners": [],
        "models": [
            "http://example.com/api/model/5/"
        ],
        "units": [
            "http://example.com/api/unit/23/",
            "http://example.com/api/unit/24/",
        ],
        "targets": [
            "http://example.com/api/target/15/",
            "http://example.com/api/target/16/",
        ],
        "timezeros": [
            "http://example.com/api/timezero/5/",
            "http://example.com/api/timezero/6/",
        ]
    },
    {
        "id": 4,
        "url": "http://example.com/api/project/4/",
        "owner": "http://example.com/api/user/1/",
        "is_public": True,
        "name": "My project",
        "description": "d1",
        "home_url": "https://reichlab.io",
        "time_interval_type": "Week",
        "visualization_y_label": "v2",
        "core_data": "",
        "truth": "http://example.com/api/project/4/truth/",
        "model_owners": [],
        "models": [
            "http://example.com/api/model/6/"
        ],
        "units": [
            "http://example.com/api/unit/26/",
        ],
        "targets": [
            "http://example.com/api/target/20/",
        ],
        "timezeros": [
            "http://example.com/api/timezero/8/",
        ]
    }
]

MODELS_LIST_DICTS = [
    {
        "id": 5,
        "url": "http://example.com/api/model/5/",
        "project": "http://example.com/api/project/3/",
        "owner": None,
        "name": "docs forecast model",
        "abbreviation": "doc_model_abbrev",
        "description": "",
        "home_url": "",
        "aux_data_url": None,
        "forecasts": [
            "http://example.com/api/forecast/3/"
        ]
    }
]

UNITS_LIST_DICTS = [
    {
        "id": 23,
        "url": "http://example.com/api/unit/23/",
        "name": "location1"
    },
    {
        "id": 24,
        "url": "http://example.com/api/unit/24/",
        "name": "location2"
    },
    {
        "id": 25,
        "url": "http://example.com/api/unit/25/",
        "name": "location3"
    }
]

TARGETS_LIST_DICTS = [
    {
        "id": 15,
        "url": "http://example.com/api/target/15/",
        "name": "pct next week",
        "description": "The forecasted percentage of positive tests for the next week",
        "type": "continuous",
        "is_step_ahead": True,
        "step_ahead_increment": 1,
        "unit": "percent"
    },
    {
        "id": 16,
        "url": "http://example.com/api/target/16/",
        "name": "cases next week",
        "description": "A forecasted integer number of cases for a future week.",
        "type": "discrete",
        "is_step_ahead": True,
        "step_ahead_increment": 1,
        "unit": "cases"
    }
]

TIMEZEROS_LIST_DICTS = [
    {
        "id": 5,
        "url": "http://example.com/api/timezero/5/",
        "timezero_date": "2011-10-02",
        "data_version_date": "2011-10-22",
        "is_season_start": True,
        "season_name": "2011-2012"
    },
    {
        "id": 6,
        "url": "http://example.com/api/timezero/6/",
        "timezero_date": "2011-10-09",
        "data_version_date": None,
        "is_season_start": False,
        "season_name": None
    },
    {
        "id": 7,
        "url": "http://example.com/api/timezero/7/",
        "timezero_date": "2011-10-16",
        "data_version_date": None,
        "is_season_start": False,
        "season_name": None
    }
]

FORECASTS_LIST_DICTS = [
    {
        "id": 12888,
        "url": "https://www.zoltardata.com/api/forecast/12888/",
        "forecast_model": "https://www.zoltardata.com/api/model/159/",
        "source": "2020-08-17-COVIDhub-ensemble.csv",
        "time_zero": {
            "id": 565,
            "url": "https://www.zoltardata.com/api/timezero/565/",
            "timezero_date": "2020-08-17",
            "data_version_date": None,
            "is_season_start": False
        },
        "created_at": "2020-08-18T15:16:23.217655-04:00",
        "notes": "",
        "forecast_data": "https://www.zoltardata.com/api/forecast/12888/data/"
    },
    {
        "id": 12385,
        "url": "https://www.zoltardata.com/api/forecast/12385/",
        "forecast_model": "https://www.zoltardata.com/api/model/159/",
        "source": "2020-07-06-COVIDhub-ensemble.csv",
        "time_zero": {
            "id": 559,
            "url": "https://www.zoltardata.com/api/timezero/559/",
            "timezero_date": "2020-07-06",
            "data_version_date": None,
            "is_season_start": False
        },
        "created_at": "2020-07-08T15:19:25.557095-04:00",
        "notes": "",
        "forecast_data": "https://www.zoltardata.com/api/forecast/12385/data/"
    }
]

if __name__ == '__main__':
    unittest.main()
