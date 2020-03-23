import unittest
from unittest import mock
from unittest.mock import patch, MagicMock

from zoltpy.connection import ZoltarConnection, ZoltarSession, ZoltarResource, Project, Model, Unit, Target, TimeZero, \
    Forecast


# MOCK_TOKEN is an expired token as returned by zoltar. decoded contents:
# - header:  {"typ": "JWT", "alg": "HS256"}
# - payload: {"user_id": 3, "username": "model_owner1", "exp": 1558442805, "email": ""}
# - expiration:
#   05/21/2019 @ 12:46pm               (UTC)
#   2019-05-21T12:46:45+00:00          (ISO 8601)
#   Tuesday, May 21, 2019 12:46:45 PM  (GMT)
#   datetime(2019, 5, 21, 12, 46, 45)  (python): datetime.utcfromtimestamp(1558442805)
MOCK_TOKEN = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJ1c2VyX2lkIjozLCJ1c2VybmFtZSI6Im1vZGVsX293bmVyMSIsImV4cCI6MTU1ODQ0MjgwNSwiZW1haWwiOiIifQ.o03V2RxkFpA5ThhRAidwDWCdcQNeJzr1wwFkOFKUI74"


class TestConnection(unittest.TestCase):
    """"""


    def test_authenticate(self):
        conn = ZoltarConnection('')
        u = 'Z_USERNAME'
        p = 'Z_PASSWORD'
        with patch('requests.post') as post_mock:
            post_mock.return_value.status_code = 200
            post_mock.return_value.json = MagicMock(return_value={'token': MOCK_TOKEN})
            conn.authenticate(u, p)
            self.assertEqual(u, conn.username)
            self.assertEqual(p, conn.password)
            self.assertIsInstance(conn.session, ZoltarSession)
            self.assertEqual(MOCK_TOKEN, conn.session.token)
            post_mock.assert_called_once_with('/api-token-auth/', {'username': 'Z_USERNAME', 'password': 'Z_PASSWORD'})


    def test_id_for_uri(self):
        self.assertEqual(71, ZoltarResource.id_for_uri('http://example.com/api/forecast/71'))  # no trailing '/'
        self.assertEqual(71, ZoltarResource.id_for_uri('http://example.com/api/forecast/71/'))


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
        projects[0].units
        json_for_uri_mock.assert_called_once_with('http://example.com/api/project/3/units/')

        json_for_uri_mock.reset_mock()
        projects[0].targets
        json_for_uri_mock.assert_called_once_with('http://example.com/api/project/3/targets/')

        json_for_uri_mock.reset_mock()
        projects[0].timezeros
        json_for_uri_mock.assert_called_once_with('http://example.com/api/project/3/timezeros/')

        json_for_uri_mock.return_value = MODELS_LIST_DICTS[0]
        json_for_uri_mock.reset_mock()
        models[0].name
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
        self.assertIsInstance(timezero_0, TimeZero)
        self.assertEqual("2011-10-02", timezero_0.timezero_date)

        # test Model.forecasts
        json_for_uri_mock.return_value = FORECASTS_LIST_DICTS
        forecasts = model_0.forecasts  # hits api/model/5/forecasts/
        self.assertEqual(1, len(forecasts))

        forecast_0 = forecasts[0]
        self.assertIsInstance(forecast_0, Forecast)
        self.assertEqual("docs-predictions.json", forecast_0.source)


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
        "score_data": "http://example.com/api/project/3/score_data/",
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
        "score_data": "http://example.com/api/project/4/score_data/",
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
        "abbreviation": "",
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
        "data_version_date": None,
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
        "id": 3,
        "url": "http://example.com/api/forecast/3/",
        "forecast_model": "http://example.com/api/model/5/",
        "source": "docs-predictions.json",
        "time_zero": "http://example.com/api/timezero/5/",
        "created_at": "2020-03-05T15:47:47.369231-05:00",
        "forecast_data": "http://example.com/api/forecast/3/data/"
    }
]

if __name__ == '__main__':
    unittest.main()
