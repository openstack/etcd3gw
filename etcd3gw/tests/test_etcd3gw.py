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

"""
test_etcd3-gateway
----------------------------------

Tests for `etcd3gw` module.
"""

import base64
import json
import requests
import threading
import time
import uuid

from testtools.testcase import unittest
from unittest import mock
import urllib3

from etcd3gw.client import Etcd3Client
from etcd3gw import exceptions
from etcd3gw.tests import base
from etcd3gw import utils


def _is_etcd3_running():
    try:
        urllib3.PoolManager().request('GET', '127.0.0.1:2379')
        return True
    except urllib3.exceptions.HTTPError:
        return False


class TestEtcd3Gateway(base.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.client = Etcd3Client(api_path='/v3/')

    @unittest.skipUnless(
        _is_etcd3_running(), "etcd3 is not available")
    def test_client_status(self):
        response = self.client.status()
        self.assertIsNotNone(response)
        self.assertIn('version', response)
        self.assertIn('header', response)
        self.assertIn('cluster_id', response['header'])

    @unittest.skipUnless(
        _is_etcd3_running(), "etcd3 is not available")
    def test_client_members(self):
        response = self.client.members()
        self.assertTrue(len(response) > 0)
        self.assertIn('clientURLs', response[0])
        self.assertIn('peerURLs', response[0])

    @unittest.skipUnless(
        _is_etcd3_running(), "etcd3 is not available")
    def test_client_with_keys_and_values(self):
        self.assertTrue(self.client.put('foo0', 'bar0'))
        self.assertTrue(self.client.put('foo1', 2001))
        self.assertTrue(self.client.put('foo2', b'bar2'))

        self.assertEqual([b'bar0'], self.client.get('foo0'))
        self.assertEqual([b'2001'], self.client.get('foo1'))
        self.assertEqual([b'bar2'], self.client.get('foo2'))

        self.assertEqual(True, self.client.delete('foo0'))
        self.assertEqual([], self.client.get('foo0'))

        self.assertEqual(False, self.client.delete('foo0'))
        self.assertTrue(len(self.client.get_all()) > 0)

    @unittest.skipUnless(
        _is_etcd3_running(), "etcd3 is not available")
    def test_get_and_delete_prefix(self):
        for i in range(20):
            self.client.put('/doot1/range{}'.format(i), 'i am a range')

        values = list(self.client.get_prefix('/doot1/range'))
        assert len(values) == 20
        for value, metadata in values:
            self.assertEqual(b'i am a range', value)
            self.assertTrue(metadata['key'].startswith(b'/doot1/range'))

        self.assertEqual(True, self.client.delete_prefix('/doot1/range'))
        values = list(self.client.get_prefix('/doot1/range'))
        assert len(values) == 0

    @unittest.skipUnless(
        _is_etcd3_running(), "etcd3 is not available")
    def test_get_prefix_sort_order(self):
        def remove_prefix(string, prefix):
            return string[len(prefix):]

        initial_keys = 'abcde'
        initial_values = 'qwert'

        for k, v in zip(initial_keys, initial_values):
            self.client.put('/doot2/{}'.format(k), v)

        keys = b''
        for value, meta in self.client.get_prefix(
                '/doot2', sort_order='ascend'):
            keys += remove_prefix(meta['key'], '/doot2/')

        assert keys == initial_keys.encode("latin-1")

        reverse_keys = b''
        for value, meta in self.client.get_prefix(
                '/doot2', sort_order='descend'):
            reverse_keys += remove_prefix(meta['key'], '/doot2/')

        assert reverse_keys == ''.join(
            reversed(initial_keys)
        ).encode("latin-1")

    @unittest.skipUnless(
        _is_etcd3_running(), "etcd3 is not available")
    def test_get_prefix_sort_order_explicit_sort_target_key(self):
        def remove_prefix(string, prefix):
            return string[len(prefix):]

        initial_keys_ordered = 'abcde'
        initial_keys = 'aebdc'
        initial_values = 'qwert'

        for k, v in zip(initial_keys, initial_values):
            self.client.put('/doot2/{}'.format(k), v)

        keys = b''
        for value, meta in self.client.get_prefix(
                '/doot2', sort_order='ascend', sort_target='key'):
            keys += remove_prefix(meta['key'], '/doot2/')

        assert keys == initial_keys_ordered.encode("latin-1")

        reverse_keys = b''
        for value, meta in self.client.get_prefix(
                '/doot2', sort_order='descend', sort_target='key'):
            reverse_keys += remove_prefix(meta['key'], '/doot2/')

        assert reverse_keys == ''.join(
            reversed(initial_keys_ordered)
        ).encode("latin-1")

    @unittest.skipUnless(
        _is_etcd3_running(), "etcd3 is not available")
    def test_get_prefix_sort_order_explicit_sort_target_rev(self):
        def remove_prefix(string, prefix):
            return string[len(prefix):]

        initial_keys = 'aebdc'
        initial_values = 'qwert'

        for k, v in zip(initial_keys, initial_values):
            self.client.put('/expsortmod/{}'.format(k), v)

        keys = b''
        for value, meta in self.client.get_prefix(
                '/expsortmod', sort_order='ascend', sort_target='mod'):
            keys += remove_prefix(meta['key'], '/expsortmod/')

        assert keys == initial_keys.encode("latin-1")

        reverse_keys = b''
        for value, meta in self.client.get_prefix(
                '/expsortmod', sort_order='descend', sort_target='mod'):
            reverse_keys += remove_prefix(meta['key'], '/expsortmod/')

        assert reverse_keys == ''.join(
            reversed(initial_keys)
        ).encode("latin-1")

    @unittest.skipUnless(
        _is_etcd3_running(), "etcd3 is not available")
    def test_replace_success(self):
        key = '/doot/thing' + str(uuid.uuid4())
        self.client.put(key, 'toot')
        status = self.client.replace(key, 'toot', 'doot')
        v = self.client.get(key)
        self.assertEqual([b'doot'], v)
        self.assertTrue(status)

    @unittest.skipUnless(
        _is_etcd3_running(), "etcd3 is not available")
    def test_replace_fail(self):
        key = '/doot/thing' + str(uuid.uuid4())
        self.client.put(key, 'boot')
        status = self.client.replace(key, 'toot', 'doot')
        v = self.client.get(key)
        self.assertEqual([b'boot'], v)
        self.assertFalse(status)

    @unittest.skipUnless(
        _is_etcd3_running(), "etcd3 is not available")
    def test_client_lease(self):
        lease = self.client.lease(ttl=60)
        self.assertIsNotNone(lease)

        ttl = lease.ttl()
        self.assertTrue(0 <= ttl <= 60)

        keys = lease.keys()
        self.assertEqual([], keys)

        ttl = lease.refresh()
        self.assertTrue(0 <= ttl <= 60)

        self.assertTrue(lease.revoke())

    @unittest.skipUnless(
        _is_etcd3_running(), "etcd3 is not available")
    def test_client_lease_with_keys(self):
        lease = self.client.lease(ttl=60)
        self.assertIsNotNone(lease)

        self.assertTrue(self.client.put('foo12', 'bar12', lease))
        self.assertTrue(self.client.put('foo13', 'bar13', lease))

        keys = lease.keys()
        self.assertEqual(2, len(keys))
        self.assertIn(b'foo12', keys)
        self.assertIn(b'foo13', keys)

        self.assertEqual([b'bar12'], self.client.get('foo12'))
        self.assertEqual([b'bar13'], self.client.get('foo13'))

        self.assertTrue(lease.revoke())

    @unittest.skipUnless(
        _is_etcd3_running(), "etcd3 is not available")
    def test_watch_key(self):
        key = '/%s-watch_key/watch' % str(uuid.uuid4())

        def update_etcd(v):
            self.client.put(key, v)
            out = self.client.get(key)
            self.assertEqual([v.encode("latin-1")], out)

        def update_key():
            # sleep to make watch can get the event
            time.sleep(3)
            update_etcd('0')
            time.sleep(1)
            update_etcd('1')
            time.sleep(1)
            update_etcd('2')
            time.sleep(1)
            update_etcd('3')
            time.sleep(1)

        t = threading.Thread(name="update_key", target=update_key)
        t.start()

        change_count = 0
        events_iterator, cancel = self.client.watch(key)
        for event in events_iterator:
            self.assertEqual(event['kv']['key'], key.encode("latin-1"))
            self.assertEqual(
                event['kv']['value'],
                str(change_count).encode("latin-1"),
            )

            # if cancel worked, we should not receive event 3
            assert event['kv']['value'] != b'3'

            change_count += 1
            if change_count > 2:
                # if cancel not work, we will block in this for-loop forever
                cancel()

        t.join()

    @unittest.skipUnless(
        _is_etcd3_running(), "etcd3 is not available")
    def test_watch_huge_payload(self):
        key = '/%s-watch_key/watch/huge_payload' % str(uuid.uuid4())

        def update_etcd(v):
            print(f"put({key}, {v}")
            self.client.put(key, v)
            out = self.client.get(key)
            self.assertEqual([v.encode("latin-1")], out)

        def update_key():
            # sleep to make watch can get the event
            time.sleep(3)
            update_etcd('0' * 10000)
            time.sleep(1)
            update_etcd('1' * 10000)
            time.sleep(1)
            update_etcd('2' * 10000)
            time.sleep(1)
            update_etcd('3' * 10000)
            time.sleep(1)

        t = threading.Thread(name="update_key_huge", target=update_key)
        t.start()

        change_count = 0
        events_iterator, cancel = self.client.watch(key)
        for event in events_iterator:
            self.assertEqual(event['kv']['key'], key.encode("latin-1"))
            self.assertEqual(
                event['kv']['value'],
                (str(change_count) * 10000).encode("latin-1"),
            )

            # if cancel worked, we should not receive event 3
            assert event['kv']['value'][0] != b'3'

            change_count += 1
            if change_count > 2:
                # if cancel not work, we will block in this for-loop forever
                cancel()

        t.join()

    @unittest.skipUnless(
        _is_etcd3_running(), "etcd3 is not available")
    def test_watch_prefix(self):
        key = '/%s-watch_prefix/watch/prefix/' % str(uuid.uuid4())

        def update_etcd(v):
            self.client.put(key + v, v)
            out = self.client.get(key + v)
            self.assertEqual([v.encode("latin-1")], out)

        def update_key():
            # sleep to make watch can get the event
            time.sleep(3)
            update_etcd('0')
            time.sleep(1)
            update_etcd('1')
            time.sleep(1)
            update_etcd('2')
            time.sleep(1)
            update_etcd('3')
            time.sleep(1)

        t = threading.Thread(name="update_key_prefix", target=update_key)
        t.start()

        change_count = 0
        events_iterator, cancel = self.client.watch_prefix(key)
        for event in events_iterator:
            if not event['kv']['key'].startswith(key.encode("latin-1")):
                continue

            self.assertEqual(
                event['kv']['key'],
                ('%s%s' % (key, change_count)).encode("latin-1"),
            )
            self.assertEqual(
                event['kv']['value'],
                str(change_count).encode("latin-1"),
            )

            # if cancel worked, we should not receive event 3
            assert event['kv']['value'] != b'3'

            change_count += 1
            if change_count > 2:
                # if cancel not work, we will block in this for-loop forever
                cancel()

        t.join()

    @unittest.skipUnless(
        _is_etcd3_running(), "etcd3 is not available")
    def test_sequential_watch_prefix_once(self):
        try:
            self.client.watch_prefix_once('/doot/', 1)
        except exceptions.WatchTimedOut:
            pass
        try:
            self.client.watch_prefix_once('/doot/', 1)
        except exceptions.WatchTimedOut:
            pass
        try:
            self.client.watch_prefix_once('/doot/', 1)
        except exceptions.WatchTimedOut:
            pass

    @unittest.skipUnless(
        _is_etcd3_running(), "etcd3 is not available")
    def test_client_lock_acquire_release(self):
        with self.client.lock(ttl=60) as lock:
            ttl = lock.refresh()
            self.assertTrue(0 <= ttl <= 60)
        self.assertFalse(lock.is_acquired())

        with self.client.lock(ttl=60) as lock:
            self.assertFalse(lock.acquire())

    @unittest.skipUnless(
        _is_etcd3_running(), "etcd3 is not available")
    def test_client_locks(self):
        lock = self.client.lock(id='xyz-%s' % time.perf_counter(), ttl=60)
        self.assertIsNotNone(lock)

        self.assertTrue(lock.acquire())
        self.assertIsNotNone(lock.uuid)

        ttl = lock.refresh()
        self.assertTrue(0 <= ttl <= 60)

        self.assertTrue(lock.is_acquired())
        self.assertTrue(lock.release())
        self.assertFalse(lock.release())
        self.assertFalse(lock.is_acquired())

    @unittest.skipUnless(
        _is_etcd3_running(), "etcd3 is not available")
    def test_create_success(self):
        key = '/foo/unique' + str(uuid.uuid4())
        # Verify that key is empty
        self.assertEqual([], self.client.get(key))

        status = self.client.create(key, 'bar')
        # Verify that key is 'bar'
        self.assertEqual([b'bar'], self.client.get(key))
        self.assertTrue(status)

    @unittest.skipUnless(
        _is_etcd3_running(), "etcd3 is not available")
    def test_create_fail(self):
        key = '/foo/' + str(uuid.uuid4())
        # Assign value to the key
        self.client.put(key, 'bar')
        self.assertEqual([b'bar'], self.client.get(key))

        status = self.client.create(key, 'goo')
        # Verify that key is still 'bar'
        self.assertEqual([b'bar'], self.client.get(key))
        self.assertFalse(status)

    @unittest.skipUnless(
        _is_etcd3_running(), "etcd3 is not available")
    def test_create_with_lease_success(self):
        key = '/foo/unique' + str(uuid.uuid4())
        # Verify that key is empty
        self.assertEqual([], self.client.get(key))
        lease = self.client.lease()

        status = self.client.create(key, 'bar', lease=lease)
        # Verify that key is 'bar'
        self.assertEqual([b'bar'], self.client.get(key))
        self.assertTrue(status)
        keys = lease.keys()
        self.assertEqual(1, len(keys))
        self.assertIn(key.encode('latin-1'), keys)

    def my_iter_content(self, *args, **kwargs):
        payload = json.dumps({
            'result': {
                'events': [{
                    'kv': {'key': base64.b64encode(b'value').decode('utf-8')},
                }]
            }
        })

        if not kwargs.get('decode_unicode', False):
            payload = payload.encode()
        return [payload]

    @mock.patch.object(requests.Response, 'iter_content', new=my_iter_content)
    @mock.patch.object(requests.sessions.Session, 'post')
    def test_watch_unicode(self, mock_post):
        mocked_response = requests.Response()
        mocked_response.connection = mock.Mock()
        mock_post.return_value = mocked_response

        try:
            res = self.client.watch_once('/some/key', timeout=1)
        except exceptions.WatchTimedOut:
            self.fail("watch timed out when server responded with unicode")

        self.assertEqual(res, {'kv': {'key': b'value'}})

    def _post_key(self, key_name, provide_value=True):
        payload = {"key": utils._encode(key_name)}
        if provide_value:
            payload["value"] = utils._encode(key_name)
        self.client.post(self.client.get_url("/kv/put"), json=payload)

    @unittest.skipUnless(
        _is_etcd3_running(), "etcd3 is not available")
    def test_client_keys_with_metadata_and_value(self):
        test_key_value = b"some_key"
        self._post_key(test_key_value)
        result = self.client.get(test_key_value, metadata=True)
        self.assertTrue(
            len(result) > 0,
            str(test_key_value) + " key is not found in etcd"
        )
        value, metadata = result[0]
        self.assertEqual(
            value,
            test_key_value,
            "unable to get value for " + str(test_key_value)
        )

    @unittest.skipUnless(
        _is_etcd3_running(), "etcd3 is not available")
    def test_client_keys_with_metadata_and_no_value(self):
        value_is_not_set_default = b""
        test_key = b"some_key"
        self._post_key(test_key, provide_value=False)
        result = self.client.get(test_key, metadata=True)
        self.assertTrue(
            len(result) > 0,
            str(test_key) + " key is not found in etcd"
        )
        value, metadata = result[0]
        self.assertEqual(
            value,
            value_is_not_set_default,
            "unable to get value for " + str(test_key)
        )
