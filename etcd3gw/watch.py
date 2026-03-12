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

from collections.abc import Callable
import json
import socket
from typing import Any, Literal, TYPE_CHECKING
import warnings

import requests

from etcd3gw.utils import _decode
from etcd3gw.utils import _encode
from etcd3gw.utils import _get_threadpool_executor

if TYPE_CHECKING:
    from etcd3gw import client as _client_module


def _watch(
    resp: requests.Response,
    callback: Callable[[dict[str, Any]], None],
) -> None:
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


class Watcher:
    def __init__(
        self,
        client: '_client_module.Etcd3Client',
        key: str,
        callback: Callable[[dict[str, Any]], None],
        *,
        start_revision: int | None = None,
        progress_notify: bool | None = None,
        filters: list[Literal['NOPUT', 'NODELETE']] | None = None,
        prev_kv: bool | None = None,
        range_end: str | None = None,
        watch_id: int | None = None,
        fragment: bool | None = None,
        **kwargs: Any,
    ) -> None:
        """Create a watcher for the given key or range.

        See ``WatchCreateRequest`` in
        https://github.com/etcd-io/etcd/blob/main/api/etcdserverpb/rpc.proto
        for the full field definitions.

        :param client: etcd client
        :param key: key to watch
        :param callback: callable invoked for each watch event
        :param start_revision: revision to watch from
        :param progress_notify: get periodic progress notifications if no
            events are received
        :param filters: filter types (``"NOPUT"`` or ``"NODELETE"``);
            requires etcd >= 3.1
        :param prev_kv: return the previous key-value on event;
            requires etcd >= 3.1
        :param range_end: end of the range to watch (key is the start)
        :param watch_id: ID to assign to this watcher; 0 means auto-assign
            (etcd >= 3.4)
        :param fragment: split large watch responses into multiple smaller
            responses (etcd >= 3.4)
        """
        create_watch: dict[str, Any] = {'key': _encode(key)}

        if start_revision is not None:
            create_watch['start_revision'] = start_revision
        if progress_notify is not None:
            create_watch['progress_notify'] = progress_notify
        if filters is not None:
            create_watch['filters'] = filters
        if prev_kv is not None:
            create_watch['prev_kv'] = prev_kv
        if range_end is not None:
            create_watch['range_end'] = _encode(range_end)
        if watch_id is not None:
            create_watch['watch_id'] = watch_id
        if fragment is not None:
            create_watch['fragment'] = fragment

        if kwargs:
            # TODO(stephenfin): Remove this warning and the kwargs argument
            # from here and all callers in a future version
            warnings.warn(
                "Found unknown argument. This is either a bug in the caller "
                "or a bug/missing feature in etcd3gw.",
                DeprecationWarning,
            )

        create_request = {"create_request": create_watch}
        self._response = client.session.post(
            client.get_url('/watch'), json=create_request, stream=True
        )

        clazz = _get_threadpool_executor()
        self._executor = clazz(max_workers=2)
        self._executor.submit(_watch, self._response, callback)

    def stop(self) -> None:
        try:
            s = socket.fromfd(
                self._response.raw._fp.fileno(),
                socket.AF_INET,
                socket.SOCK_STREAM,
            )
            s.shutdown(socket.SHUT_RDWR)
            s.close()
        except Exception:  # noqa: S110
            pass
        self._response.connection.close()
        self._executor.shutdown(wait=False)
