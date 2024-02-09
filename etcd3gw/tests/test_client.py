# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

import requests.exceptions
from unittest import mock

from etcd3gw.client import Etcd3Client
from etcd3gw import exceptions as exc
from etcd3gw.tests import base


class TestEtcd3Gateway(base.TestCase):

    def test_client_version_discovery(self):
        client = Etcd3Client()
        with mock.patch.object(client, "session") as mock_session:
            mock_response = mock.Mock()
            mock_response.status_code = 200
            mock_response.reason = "OK"
            mock_response.json.return_value = {
                "etcdserver": "3.4.0",
                "etcdcluster": "3.0.0"
            }
            mock_session.get.return_value = mock_response
            self.assertEqual("http://localhost:2379",
                             client.base_url)
            self.assertEqual("http://localhost:2379/v3/lease/grant",
                             client.get_url("/lease/grant"))

    def test_client_version_discovery_v3beta(self):
        client = Etcd3Client()
        with mock.patch.object(client, "session") as mock_session:
            mock_response = mock.Mock()
            mock_response.status_code = 200
            mock_response.reason = "OK"
            mock_response.json.return_value = {
                "etcdserver": "3.3.0",
                "etcdcluster": "3.0.0"
            }
            mock_session.get.return_value = mock_response
            self.assertEqual("http://localhost:2379",
                             client.base_url)
            self.assertEqual("http://localhost:2379/v3beta/lease/grant",
                             client.get_url("/lease/grant"))

    def test_client_version_discovery_v3alpha(self):
        client = Etcd3Client()
        with mock.patch.object(client, "session") as mock_session:
            mock_response = mock.Mock()
            mock_response.status_code = 200
            mock_response.reason = "OK"
            mock_response.json.return_value = {
                "etcdserver": "3.2.0",
                "etcdcluster": "3.0.0"
            }
            mock_session.get.return_value = mock_response
            self.assertEqual("http://localhost:2379",
                             client.base_url)
            self.assertEqual("http://localhost:2379/v3alpha/lease/grant",
                             client.get_url("/lease/grant"))

    def test_client_version_discovery_fail(self):
        client = Etcd3Client()
        with mock.patch.object(client, "session") as mock_session:
            mock_response = mock.Mock()
            mock_response.status_code = 500
            mock_response.reason = "Internal Server Error"
            mock_session.get.return_value = mock_response
            self.assertRaises(
                exc.Etcd3Exception,
                client.get_url, "/lease/grant")

    def test_client_version_discovery_version_absent(self):
        client = Etcd3Client()
        with mock.patch.object(client, "session") as mock_session:
            mock_response = mock.Mock()
            mock_response.status_code = 200
            mock_response.reason = "OK"
            mock_response.json.return_value = {}
            mock_session.get.return_value = mock_response
            self.assertRaises(
                exc.ApiVersionDiscoveryFailedError,
                client.get_url, "/lease/grant")

    def test_client_version_discovery_version_malformed(self):
        client = Etcd3Client()
        with mock.patch.object(client, "session") as mock_session:
            mock_response = mock.Mock()
            mock_response.status_code = 200
            mock_response.reason = "OK"
            mock_response.json.return_value = {
                "etcdserver": "3.2.a",
                "etcdcluster": "3.0.0"
            }
            mock_session.get.return_value = mock_response
            self.assertRaises(
                exc.ApiVersionDiscoveryFailedError,
                client.get_url, "/lease/grant")

    def test_client_api_path(self):
        client = Etcd3Client(api_path='/v3/')
        self.assertEqual("http://localhost:2379",
                         client.base_url)
        self.assertEqual("http://localhost:2379/v3/lease/grant",
                         client.get_url("/lease/grant"))

    def test_client_ipv4(self):
        client = Etcd3Client(host="127.0.0.1", api_path='/v3/')
        self.assertEqual("http://127.0.0.1:2379",
                         client.base_url)
        self.assertEqual("http://127.0.0.1:2379/v3/lease/grant",
                         client.get_url("/lease/grant"))

    def test_client_ipv6(self):
        client = Etcd3Client(host="::1", api_path='/v3/')
        self.assertEqual("http://[::1]:2379",
                         client.base_url)
        self.assertEqual("http://[::1]:2379/v3/lease/grant",
                         client.get_url("/lease/grant"))

    def test_client_status(self):
        client = Etcd3Client(host="127.0.0.1", api_path='/v3/')
        with mock.patch.object(client, "session") as mock_session:
            mock_response = mock.Mock()
            mock_response.status_code = 200
            mock_response.reason = "OK"
            mock_response.text = "{}"
            mock_session.post.return_value = mock_response
            client.status()
            mock_session.post.assert_has_calls([
                mock.call('http://127.0.0.1:2379/v3/maintenance/status',
                          timeout=None, json={})
            ])

    def test_client_with_timeout(self):
        client = Etcd3Client(host="127.0.0.1", api_path='/v3/', timeout=60)
        with mock.patch.object(client, "session") as mock_session:
            mock_response = mock.Mock()
            mock_response.status_code = 200
            mock_response.reason = "OK"
            mock_response.text = "{}"
            mock_session.post.return_value = mock_response
            client.status()
            mock_session.post.assert_has_calls([
                mock.call('http://127.0.0.1:2379/v3/maintenance/status',
                          timeout=60, json={})
            ])

    def test_client_timed_out(self):
        client = Etcd3Client(host="127.0.0.1", api_path='/v3/', timeout=60)
        with mock.patch.object(client, "session") as mock_session:
            mock_session.post.side_effect = requests.exceptions.Timeout()
            self.assertRaises(exc.ConnectionTimeoutError, client.status)
            mock_session.post.assert_has_calls([
                mock.call('http://127.0.0.1:2379/v3/maintenance/status',
                          timeout=60, json={})
            ])

    def test_client_connection_error(self):
        client = Etcd3Client(host="127.0.0.1", api_path='/v3/')
        with mock.patch.object(client, "session") as mock_session:
            mock_session.post.side_effect = \
                requests.exceptions.ConnectionError()
            self.assertRaises(exc.ConnectionFailedError, client.status)
            mock_session.post.assert_has_calls([
                mock.call('http://127.0.0.1:2379/v3/maintenance/status',
                          timeout=None, json={})
            ])

    def test_client_bad_request(self):
        client = Etcd3Client(host="127.0.0.1", api_path='/v3/')
        with mock.patch.object(client, "session") as mock_session:
            mock_response = mock.Mock()
            mock_response.status_code = 400
            mock_response.reason = "Bad Request"
            mock_response.text = '''{
"error": "etcdserver: mvcc: required revision has been compacted",
"code": 11
}'''
            mock_session.post.return_value = mock_response
            try:
                client.status()
                self.assertFalse(True)
            except exc.Etcd3Exception as e:
                self.assertEqual(str(e), "Bad Request")
                self.assertEqual(e.detail_text, '''{
"error": "etcdserver: mvcc: required revision has been compacted",
"code": 11
}''')

    def test_client_exceptions_by_code(self):
        client = Etcd3Client(host="127.0.0.1", api_path='/v3/')
        with mock.patch.object(client, "session") as mock_session:
            mock_response = mock.Mock()
            mock_response.status_code = 500
            mock_response.reason = "Internal Server Error"
            mock_response.text = '''{
"error": "etcdserver: unable to reach quorum"
}'''
            mock_session.post.return_value = mock_response
            try:
                client.status()
                self.assertFalse(True)
            except exc.InternalServerError as e:
                self.assertEqual(str(e), "Internal Server Error")
                self.assertEqual(e.detail_text, '''{
"error": "etcdserver: unable to reach quorum"
}''')
