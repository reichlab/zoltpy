import unittest
from unittest.mock import patch, MagicMock

from zoltpy.connection import ZoltarConnection, ZoltarSession


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
    """
    """


    def test_authenticate(self):
        conn = ZoltarConnection('')
        u = 'username'
        p = 'password'
        with patch('requests.post', return_value=MagicMock()) as post_mock:
            post_mock.return_value.status_code = 200
            post_mock.return_value.json = MagicMock(return_value={'token': MOCK_TOKEN})
            conn.authenticate(u, p)
            self.assertEqual(u, conn.username)
            self.assertEqual(p, conn.password)
            self.assertIsInstance(conn.session, ZoltarSession)
            self.assertEqual(MOCK_TOKEN, conn.session.token)
            post_mock.assert_called_once_with('/api-token-auth/', {'username': 'username', 'password': 'password'})


if __name__ == '__main__':
    unittest.main()
