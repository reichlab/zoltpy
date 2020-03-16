import unittest
from unittest import mock
from unittest.mock import patch, MagicMock

from zoltpy.connection import ZoltarConnection, ZoltarSession, ZoltarResource


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
        json_for_uri_mock.return_value = project_list_dict
        conn = ZoltarConnection()
        projects = conn.projects  # hits the API to get the project list
        json_for_uri_mock.assert_called_once_with('https://zoltardata.com/api/projects/')

        json_for_uri_mock.reset_mock()
        models = projects[0].models

        json_for_uri_mock.assert_not_called()

        json_for_uri_mock.reset_mock()
        projects[0].units
        json_for_uri_mock.assert_not_called()

        json_for_uri_mock.reset_mock()
        projects[0].targets
        json_for_uri_mock.assert_not_called()

        json_for_uri_mock.reset_mock()
        projects[0].timezeros
        json_for_uri_mock.assert_not_called()

        json_for_uri_mock.return_value = model_dict
        json_for_uri_mock.reset_mock()
        models[0].name  # hits the API
        json_for_uri_mock.assert_called_once_with('http://127.0.0.1:8000/api/model/5/')


project_list_dict = [
    {
        "id": 3,
        "url": "http://127.0.0.1:8000/api/project/3/",
        "owner": None,
        "is_public": True,
        "name": "Docs Example Project",
        "description": "d2",
        "home_url": "https://reichlab.io",
        "time_interval_type": "Day",
        "visualization_y_label": "v1",
        "core_data": "",
        "truth": "http://127.0.0.1:8000/api/project/3/truth/",
        "model_owners": [],
        "score_data": "http://127.0.0.1:8000/api/project/3/score_data/",
        "models": [
            "http://127.0.0.1:8000/api/model/5/"
        ],
        "units": [
            "http://127.0.0.1:8000/api/unit/23/",
            "http://127.0.0.1:8000/api/unit/24/",
        ],
        "targets": [
            "http://127.0.0.1:8000/api/target/15/",
            "http://127.0.0.1:8000/api/target/16/",
        ],
        "timezeros": [
            "http://127.0.0.1:8000/api/timezero/5/",
            "http://127.0.0.1:8000/api/timezero/6/",
        ]
    },
    {
        "id": 4,
        "url": "http://127.0.0.1:8000/api/project/4/",
        "owner": "http://127.0.0.1:8000/api/user/1/",
        "is_public": True,
        "name": "My project",
        "description": "d1",
        "home_url": "https://reichlab.io",
        "time_interval_type": "Week",
        "visualization_y_label": "v2",
        "core_data": "",
        "truth": "http://127.0.0.1:8000/api/project/4/truth/",
        "model_owners": [],
        "score_data": "http://127.0.0.1:8000/api/project/4/score_data/",
        "models": [
            "http://127.0.0.1:8000/api/model/6/"
        ],
        "units": [
            "http://127.0.0.1:8000/api/unit/26/",
        ],
        "targets": [
            "http://127.0.0.1:8000/api/target/20/",
        ],
        "timezeros": [
            "http://127.0.0.1:8000/api/timezero/8/",
        ]
    }
]

model_dict = {
    "id": 5,
    "url": "http://127.0.0.1:8000/api/model/5/",
    "project": "http://127.0.0.1:8000/api/project/3/",
    "owner": None,
    "name": "docs forecast model",
    "abbreviation": "",
    "description": "",
    "home_url": "",
    "aux_data_url": None,
    "forecasts": [
        "http://127.0.0.1:8000/api/forecast/3/"
    ]
}

if __name__ == '__main__':
    unittest.main()
