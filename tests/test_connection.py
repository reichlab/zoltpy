'''
WIP
'''

import unittest
from unittest.mock import patch

from zoltpy.connection import ZoltarConnection, ZoltarSession


class TestConnection(unittest.TestCase):

    def test_authenticate(self):
        host = 'http://idonotexist.com'
        conn = ZoltarConnection(host)
        self.assertEqual(host, conn.host)

        u = 'username'
        p = 'password'
        token = '''eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM
0NTY3ODkwIiwibmFtZSI6IkpvaG4gRG9lIiwiaWF0IjoxNTE2Mj
M5MDIyfQ.SflKxwRJSMeKKF2QT4fwpMeJf36POk6yJV_adQssw5c'''
        with patch.object(ZoltarSession, '_get_token', return_value=token) as mock_method:
            # with patch('zoltpy.connection.ZoltarSession._get_token', return_value=token) as mock_method:
            conn.authenticate(u, p)
            print('XX', conn.session.token)
            self.assertIsInstance(conn.session, ZoltarSession)


    def test_is_token_expired(self):
        self.fail()  # todo xx


if __name__ == '__main__':
    unittest.main()
