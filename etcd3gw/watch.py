#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

import json
import socket

from etcd3gw.utils import _decode
from etcd3gw.utils import _encode
from etcd3gw.utils import _get_threadpool_executor


def _watch(resp, callback):
    for line in resp.iter_content(chunk_size=None, decode_unicode=False):
        decoded_line = line.decode('utf-8')
        # Skip a possible empty line (only "\n")
        # https://bugs.launchpad.net/python-etcd3gw/+bug/2072492
        if not decoded_line.strip():
            continue
        payload = json.loads(decoded_line)
        if 'created' in payload['result']:
            if payload['result']['created']:
                continue
            else:
                raise Exception('Unable to create watch')
        if 'events' in payload['result']:
            for event in payload['result']['events']:
                event['kv']['key'] = _decode(event['kv']['key'])
                if 'value' in event['kv']:
                    event['kv']['value'] = _decode(event['kv']['value'])
                callback(event)


class Watcher(object):

    KW_ARGS = ['start_revision', 'progress_notify', 'filters', 'prev_kv']
    KW_ENCODED_ARGS = ['range_end']

    def __init__(self, client, key, callback, **kwargs):
        create_watch = {
            'key': _encode(key)
        }

        for arg in kwargs:
            if arg in self.KW_ARGS:
                create_watch[arg] = kwargs[arg]
            elif arg in self.KW_ENCODED_ARGS:
                create_watch[arg] = _encode(kwargs[arg])

        create_request = {
            "create_request": create_watch
        }
        self._response = client.session.post(client.get_url('/watch'),
                                             json=create_request,
                                             stream=True)

        clazz = _get_threadpool_executor()
        self._executor = clazz(max_workers=2)
        self._executor.submit(_watch, self._response, callback)

    def stop(self):
        try:
            s = socket.fromfd(self._response.raw._fp.fileno(),
                              socket.AF_INET,
                              socket.SOCK_STREAM)
            s.shutdown(socket.SHUT_RDWR)
            s.close()
        except Exception:
            pass
        self._response.connection.close()
        self._executor.shutdown(wait=False)
