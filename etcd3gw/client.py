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

from __future__ import annotations

from collections.abc import Callable, Iterator
import json
import os
import queue
import threading
from typing import Any, Literal, cast, overload
import uuid
import warnings

import requests

from etcd3gw import exceptions
from etcd3gw.lease import Lease
from etcd3gw.lock import Lock
from etcd3gw.types import Event
from etcd3gw.types import KeyValue
from etcd3gw.types import Member
from etcd3gw.types import RangeResponse
from etcd3gw.types import StatusResponse
from etcd3gw.types import TxnResponse
from etcd3gw.utils import _decode
from etcd3gw.utils import _encode
from etcd3gw.utils import _increment_last_byte
from etcd3gw.utils import DEFAULT_TIMEOUT
from etcd3gw import watch

_SORT_ORDER = ['none', 'ascend', 'descend']
_SORT_TARGET = ['key', 'version', 'create', 'mod', 'value']

_EXCEPTIONS_BY_CODE: dict[int | None, type[exceptions.Etcd3Exception]] = {
    requests.codes['internal_server_error']: exceptions.InternalServerError,
    requests.codes['service_unavailable']: exceptions.ConnectionFailedError,
    requests.codes['request_timeout']: exceptions.ConnectionTimeoutError,
    requests.codes['gateway_timeout']: exceptions.ConnectionTimeoutError,
    requests.codes['precondition_failed']: exceptions.PreconditionFailedError,
}

DEFAULT_API_PATH: str | None = os.getenv('ETCD3GW_API_PATH')


class Etcd3Client:
    def __init__(
        self,
        host: str = 'localhost',
        port: int = 2379,
        protocol: str = "http",
        ca_cert: str | None = None,
        cert_key: str | None = None,
        cert_cert: str | None = None,
        timeout: float | int | None = None,
        api_path: str | None = DEFAULT_API_PATH,
        session: requests.Session | None = None,
    ) -> None:
        """Construct an client to talk to etcd3's grpc-gateway's /v3 HTTP API

        :param host: etcd host
        :param port: etcd port
        :param protocol: protocol (http or https)
        :param ca_cert: CA certificate for SSL/TLS verification
        :param cert_key: client certificate key
        :param cert_cert: client certificate
        :param timeout: request timeout
        :param api_path: API path (default to auto-discovery)
        :param session: optional preconfigured request.session object.
                        If not provided new session will be created
        """
        self.host = host
        self.port = port
        self.protocol = protocol

        if session is not None:
            self.session = session
        else:
            self.session = requests.Session()
            if ca_cert is not None:
                self.session.verify = ca_cert
            if cert_cert is not None and cert_key is not None:
                self.session.cert = (cert_cert, cert_key)

        self.timeout = timeout
        self._api_path: str | None = api_path

    @property
    def api_path(self) -> str:
        if self._api_path is not None:
            return self._api_path
        self._discover_api_path()
        assert self._api_path is not None
        return self._api_path

    @property
    def base_url(self) -> str:
        host = (
            '[' + self.host + ']' if (self.host.find(':') != -1) else self.host
        )
        return self.protocol + '://' + host + ':' + str(self.port)

    def _discover_api_path(self) -> None:
        """Discover api version and set api_path"""
        try:
            resp = self.session.get(
                self.base_url + '/version', timeout=self.timeout
            )
        except requests.exceptions.Timeout as ex:
            raise exceptions.ConnectionTimeoutError(str(ex))
        except requests.exceptions.ConnectionError as ex:
            raise exceptions.ConnectionFailedError(str(ex))

        if resp.status_code in _EXCEPTIONS_BY_CODE:
            raise _EXCEPTIONS_BY_CODE[resp.status_code](resp.text, resp.reason)

        if resp.status_code != requests.codes['ok']:
            raise exceptions.Etcd3Exception(resp.text, resp.reason)

        try:
            version_str = resp.json()['etcdserver']
        except KeyError:
            raise exceptions.ApiVersionDiscoveryFailedError(
                'Malformed response from version API'
            )

        try:
            version = tuple(int(part) for part in version_str.split('.', 2))
        except ValueError:
            raise exceptions.ApiVersionDiscoveryFailedError(
                f'Failed to parse etcd cluster version: {version_str}'
            )

        # NOTE(tkajinam): https://etcd.io/docs/v3.5/dev-guide/api_grpc_gateway/
        #                 explains mapping between etcd version and available
        #                 api versions
        if version >= (3, 4):
            self._api_path = '/v3/'
        elif version >= (3, 3):
            self._api_path = '/v3beta/'
        else:
            self._api_path = '/v3alpha/'

    def get_url(self, path: str) -> str:
        """Construct a full url to the v3 API given a specific path

        :param path:
        :return: url
        """
        return self.base_url + self.api_path + path.lstrip("/")

    def post(
        self, url: str, *args: Any, json: Any = None, **kwargs: Any
    ) -> Any:
        """helper method for HTTP POST

        :param args:
        :param kwargs:
        :return: json response
        """
        try:
            resp = self.session.post(  # type: ignore[misc]
                url, *args, json=json, timeout=self.timeout, **kwargs
            )
        except requests.exceptions.Timeout as ex:
            raise exceptions.ConnectionTimeoutError(str(ex))
        except requests.exceptions.ConnectionError as ex:
            raise exceptions.ConnectionFailedError(str(ex))

        if resp.status_code in _EXCEPTIONS_BY_CODE:
            raise _EXCEPTIONS_BY_CODE[resp.status_code](resp.text, resp.reason)

        if resp.status_code != requests.codes['ok']:
            raise exceptions.Etcd3Exception(resp.text, resp.reason)

        return resp.json()

    def status(self) -> StatusResponse:
        """Status gets the status of the etcd cluster member.

        :return: json response
        """
        return cast(
            StatusResponse,
            self.post(self.get_url("/maintenance/status"), json={}),
        )

    def members(self) -> list[Member]:
        """Lists all the members in the cluster.

        :return: json response
        """
        result = self.post(self.get_url("/cluster/member/list"), json={})
        return cast(list[Member], result['members'])

    def lease(self, ttl: int = DEFAULT_TIMEOUT) -> Lease:
        """Create a Lease object given a timeout

        :param ttl: timeout
        :return: Lease object
        """
        result = self.post(
            self.get_url("/lease/grant"), json={"TTL": ttl, "ID": 0}
        )
        return Lease(int(result['ID']), client=self)

    def lock(self, id: str | None = None, ttl: int = DEFAULT_TIMEOUT) -> Lock:
        """Create a Lock object given an ID and timeout

        :param id: ID for the lock, creates a new uuid if not provided
        :param ttl: timeout
        :return: Lock object
        """
        if id is None:
            id = str(uuid.uuid4())
        return Lock(id, ttl=ttl, client=self)

    def create(
        self,
        key: str | bytes,
        value: str | bytes,
        lease: Lease | None = None,
    ) -> bool:
        """Atomically create the given key only if the key doesn't exist.

        This verifies that the create_revision of a key equales to 0, then
        creates the key with the value.
        This operation takes place in a transaction.

        :param key: key in etcd to create
        :param value: value of the key
        :param lease: lease to connect with, optional
        :returns: status of transaction, ``True`` if the create was
                  successful, ``False`` otherwise
        :rtype: bool
        """
        base64_key = _encode(key)
        base64_value = _encode(value)
        payload: dict[str, Any] = {
            'compare': [
                {
                    'key': base64_key,
                    'result': 'EQUAL',
                    'target': 'CREATE',
                    'create_revision': 0,
                }
            ],
            'success': [
                {
                    'request_put': {
                        'key': base64_key,
                        'value': base64_value,
                    }
                }
            ],
            'failure': [],
        }
        if lease:
            payload['success'][0]['request_put']['lease'] = lease.id
        result = self.transaction(payload)
        if 'succeeded' in result:
            return result['succeeded']
        return False

    def put(
        self,
        key: str | bytes,
        value: str | bytes,
        lease: Lease | None = None,
    ) -> bool:
        """Put puts the given key into the key-value store.

        A put request increments the revision of the key-value store
        and generates one event in the event history.

        :param key:
        :param value:
        :param lease:
        :return: boolean
        """
        payload: dict[str, Any] = {
            "key": _encode(key),
            "value": _encode(value),
        }
        if lease:
            payload['lease'] = lease.id
        self.post(self.get_url("/kv/put"), json=payload)
        return True

    @overload
    def get(
        self,
        key: str | bytes,
        metadata: Literal[False] = False,
        sort_order: Literal['none', 'ascend', 'descend'] | None = None,
        sort_target: (
            Literal['key', 'version', 'create', 'mod', 'value'] | None
        ) = None,
        *,
        range_end: str | None = None,
        limit: int | None = None,
        revision: int | None = None,
        serializable: bool | None = None,
        keys_only: bool | None = None,
        count_only: bool | None = None,
        min_mod_revision: int | None = None,
        max_mod_revision: int | None = None,
        min_create_revision: int | None = None,
        max_create_revision: int | None = None,
        **kwargs: Any,
    ) -> list[bytes]: ...

    @overload
    def get(
        self,
        key: str | bytes,
        metadata: Literal[True] = ...,
        sort_order: Literal['none', 'ascend', 'descend'] | None = None,
        sort_target: (
            Literal['key', 'version', 'create', 'mod', 'value'] | None
        ) = None,
        *,
        range_end: str | None = None,
        limit: int | None = None,
        revision: int | None = None,
        serializable: bool | None = None,
        keys_only: bool | None = None,
        count_only: bool | None = None,
        min_mod_revision: int | None = None,
        max_mod_revision: int | None = None,
        min_create_revision: int | None = None,
        max_create_revision: int | None = None,
        **kwargs: Any,
    ) -> list[tuple[bytes, KeyValue]]: ...

    def get(
        self,
        key: str | bytes,
        metadata: bool = False,
        sort_order: Literal['none', 'ascend', 'descend'] | None = None,
        sort_target: (
            Literal['key', 'version', 'create', 'mod', 'value'] | None
        ) = None,
        *,
        range_end: str | None = None,
        limit: int | None = None,
        revision: int | None = None,
        serializable: bool | None = None,
        keys_only: bool | None = None,
        count_only: bool | None = None,
        min_mod_revision: int | None = None,
        max_mod_revision: int | None = None,
        min_create_revision: int | None = None,
        max_create_revision: int | None = None,
        **kwargs: Any,
    ) -> list[bytes] | list[tuple[bytes, KeyValue]]:
        """Range gets the keys in the range from the key-value store.

        :param key:
        :param metadata:
        :param sort_order: 'ascend' or 'descend' or None
        :param sort_target: 'key' or 'version' or 'create' or 'mod' or 'value'
        :return:
        """
        try:
            order = 0
            if sort_order:
                order = _SORT_ORDER.index(sort_order)
        except ValueError:
            raise ValueError('sort_order must be one of "ascend" or "descend"')

        try:
            target = 0
            if sort_target:
                target = _SORT_TARGET.index(sort_target)
        except ValueError:
            raise ValueError(
                'sort_target must be one of "key", '
                '"version", "create", "mod" or "value"'
            )

        payload: dict[str, Any] = {
            "key": _encode(key),
            "sort_order": order,
            "sort_target": target,
        }

        if range_end is not None:
            payload['range_end'] = _encode(range_end)
        if limit is not None:
            payload['limit'] = limit
        if revision is not None:
            payload['revision'] = revision
        if serializable is not None:
            payload['serializable'] = serializable
        if keys_only is not None:
            payload['keys_only'] = keys_only
        if count_only is not None:
            payload['count_only'] = count_only
        if min_mod_revision is not None:
            payload['min_mod_revision'] = min_mod_revision
        if max_mod_revision is not None:
            payload['max_mod_revision'] = max_mod_revision
        if min_create_revision is not None:
            payload['min_create_revision'] = min_create_revision
        if max_create_revision is not None:
            payload['max_create_revision'] = max_create_revision

        if kwargs:
            # TODO(stephenfin): Remove this warning and the kwargs argument in
            # a future version
            warnings.warn(
                "Found unknown argument. This is either a bug in the caller "
                "or a bug/missing feature in etcd3gw.",
                DeprecationWarning,
            )
            payload.update(kwargs)

        result = cast(
            RangeResponse, self.post(self.get_url("/kv/range"), json=payload)
        )
        if 'kvs' not in result:
            return []

        if metadata:

            def value_with_metadata(
                item: KeyValue,
            ) -> tuple[bytes, KeyValue]:
                item['key'] = _decode(item['key'])
                value = _decode(item.pop('value', ''))
                return value, item

            return [value_with_metadata(item) for item in result['kvs']]

        return [_decode(item.get('value', '')) for item in result['kvs']]

    def get_all(
        self,
        sort_order: Literal['none', 'ascend', 'descend'] | None = None,
        sort_target: (
            Literal['key', 'version', 'create', 'mod', 'value'] | None
        ) = 'key',
    ) -> list[tuple[bytes, KeyValue]]:
        """Get all keys currently stored in etcd.

        :returns: sequence of (value, metadata) tuples
        """
        return self.get(
            key='\0',
            metadata=True,
            sort_order=sort_order,
            sort_target=sort_target,
            range_end='\0',
        )

    def get_prefix(
        self,
        key_prefix: str | bytes,
        sort_order: Literal['none', 'ascend', 'descend'] | None = None,
        sort_target: (
            Literal['key', 'version', 'create', 'mod', 'value'] | None
        ) = None,
    ) -> list[tuple[bytes, KeyValue]]:
        """Get a range of keys with a prefix.

        :param sort_order: 'ascend' or 'descend' or None
        :param key_prefix: first key in range

        :returns: sequence of (value, metadata) tuples
        """
        return self.get(
            key_prefix,
            metadata=True,
            range_end=_increment_last_byte(key_prefix),
            sort_order=sort_order,
            sort_target=sort_target,
        )

    def replace(
        self,
        key: str | bytes,
        initial_value: str | bytes,
        new_value: str | bytes,
    ) -> bool:
        """Atomically replace the value of a key with a new value.

        This compares the current value of a key, then replaces it with a new
        value if it is equal to a specified value. This operation takes place
        in a transaction.

        :param key: key in etcd to replace
        :param initial_value: old value to replace
        :param new_value: new value of the key
        :returns: status of transaction, ``True`` if the replace was
                  successful, ``False`` otherwise
        :rtype: bool
        """
        base64_key = _encode(key)
        base64_initial_value = _encode(initial_value)
        base64_new_value = _encode(new_value)
        txn = {
            'compare': [
                {
                    'key': base64_key,
                    'result': 'EQUAL',
                    'target': 'VALUE',
                    'value': base64_initial_value,
                }
            ],
            'success': [
                {
                    'request_put': {
                        'key': base64_key,
                        'value': base64_new_value,
                    }
                }
            ],
            'failure': [],
        }
        result = self.transaction(txn)
        if 'succeeded' in result:
            return result['succeeded']
        return False

    def delete(
        self,
        key: str | bytes,
        *,
        range_end: str | None = None,
        prev_kv: bool | None = None,
        **kwargs: Any,
    ) -> bool:
        """DeleteRange deletes the given range from the key-value store.

        A delete request increments the revision of the key-value store and
        generates a delete event in the event history for every deleted key.

        :param key: key (or start of range) to delete
        :param range_end: end of range to delete; if unset, only ``key`` is
            deleted
        :param prev_kv: if set, return the deleted key-value pairs
        :return: ``True`` if any key was deleted, ``False`` otherwise
        """
        payload: dict[str, Any] = {
            "key": _encode(key),
        }
        if range_end is not None:
            payload['range_end'] = _encode(range_end)
        if prev_kv is not None:
            payload['prev_kv'] = prev_kv

        if kwargs:
            # TODO(stephenfin): Remove this warning and the kwargs argument in
            # a future version
            warnings.warn(
                "Found unknown argument. This is either a bug in the caller "
                "or a bug/missing feature in etcd3gw.",
                DeprecationWarning,
            )
            payload.update(kwargs)

        result = self.post(self.get_url("/kv/deleterange"), json=payload)
        if 'deleted' in result:
            return True
        return False

    def delete_prefix(self, key_prefix: str | bytes) -> bool:
        """Delete a range of keys with a prefix in etcd."""
        return self.delete(
            key_prefix, range_end=_increment_last_byte(key_prefix)
        )

    # NOTE(stephenfin): It would be nice to type txn better but it's pretty
    # complicated
    #
    # https://github.com/etcd-io/etcd/blob/release-3.6/api/etcdserverpb/rpc.proto#L659-L672
    # https://etcd.io/docs/v3.6/learning/api/#transaction
    def transaction(self, txn: dict[str, Any]) -> TxnResponse:
        """Txn processes multiple requests in a single transaction.

        A txn request increments the revision of the key-value store and
        generates events with the same revision for every completed request.
        It is not allowed to modify the same key several times within one txn.

        :param txn:
        :return:
        """
        return cast(
            TxnResponse,
            self.post(self.get_url("/kv/txn"), data=json.dumps(txn)),
        )

    def watch(
        self,
        key: str | bytes,
        *,
        start_revision: int | None = None,
        progress_notify: bool | None = None,
        filters: list[Literal['NOPUT', 'NODELETE']] | None = None,
        prev_kv: bool | None = None,
        range_end: str | None = None,
        watch_id: int | None = None,
        fragment: bool | None = None,
        **kwargs: Any,
    ) -> tuple[Iterator[Event], Callable[[], None]]:
        """Watch a key.

        :param key: key to watch
        :param start_revision: revision to watch from
        :param progress_notify: get periodic progress notifications
        :param filters: filter types (``"NOPUT"`` or ``"NODELETE"``)
        :param prev_kv: return the previous key-value on event
        :param range_end: end of the range to watch (key is the start)
        :param watch_id: ID to assign to this watcher; 0 means auto-assign
            (etcd >= 3.4)
        :param fragment: split large watch responses into multiple smaller
            responses (etcd >= 3.4)

        :returns: tuple of ``events_iterator`` and ``cancel``.
                  Use ``events_iterator`` to get the events of key changes
                  and ``cancel`` to cancel the watch request
        """
        event_queue: queue.Queue[Event | None] = queue.Queue()

        def callback(event: Event) -> None:
            event_queue.put(event)

        w = watch.Watcher(
            self,
            key,
            callback,
            start_revision=start_revision,
            progress_notify=progress_notify,
            filters=filters,
            prev_kv=prev_kv,
            range_end=range_end,
            **kwargs,
        )
        canceled = threading.Event()

        def cancel() -> None:
            canceled.set()
            event_queue.put(None)
            w.stop()

        def iterator() -> Iterator[Event]:
            while not canceled.is_set():
                event = event_queue.get()
                if event is None:
                    canceled.set()
                if not canceled.is_set():
                    assert event is not None
                    yield event

        return iterator(), cancel

    def watch_prefix(
        self,
        key_prefix: str | bytes,
        *,
        start_revision: int | None = None,
        progress_notify: bool | None = None,
        filters: list[Literal['NOPUT', 'NODELETE']] | None = None,
        prev_kv: bool | None = None,
        watch_id: int | None = None,
        fragment: bool | None = None,
        **kwargs: Any,
    ) -> tuple[Iterator[Event], Callable[[], None]]:
        """The same as ``watch``, but watches a range of keys with a prefix.

        :param key_prefix: key prefix to watch
        :param start_revision: revision to watch from
        :param progress_notify: get periodic progress notifications
        :param filters: filter types (``"NOPUT"`` or ``"NODELETE"``)
        :param prev_kv: return the previous key-value on event
        :param watch_id: ID to assign to this watcher; 0 means auto-assign
            (etcd >= 3.4)
        :param fragment: split large watch responses into multiple smaller
            responses (etcd >= 3.4)
        """
        return self.watch(
            key_prefix,
            range_end=_increment_last_byte(key_prefix),
            start_revision=start_revision,
            progress_notify=progress_notify,
            filters=filters,
            prev_kv=prev_kv,
            **kwargs,
        )

    def watch_once(
        self,
        key: str | bytes,
        timeout: float | int | None = None,
        *,
        start_revision: int | None = None,
        progress_notify: bool | None = None,
        filters: list[Literal['NOPUT', 'NODELETE']] | None = None,
        prev_kv: bool | None = None,
        range_end: str | None = None,
        watch_id: int | None = None,
        fragment: bool | None = None,
        **kwargs: Any,
    ) -> Event:
        """Watch a key and stops after the first event.

        :param key: key to watch
        :param timeout: (optional) timeout in seconds.
        :param start_revision: revision to watch from
        :param progress_notify: get periodic progress notifications
        :param filters: filter types (``"NOPUT"`` or ``"NODELETE"``)
        :param prev_kv: return the previous key-value on event
        :param range_end: end of the range to watch (key is the start)
        :param watch_id: ID to assign to this watcher; 0 means auto-assign
            (etcd >= 3.4)
        :param fragment: split large watch responses into multiple smaller
            responses (etcd >= 3.4)
        :returns: event
        """
        event_queue: queue.Queue[Event] = queue.Queue()

        def callback(event: Event) -> None:
            event_queue.put(event)

        w = watch.Watcher(
            self,
            key,
            callback,
            start_revision=start_revision,
            progress_notify=progress_notify,
            filters=filters,
            prev_kv=prev_kv,
            range_end=range_end,
            **kwargs,
        )
        try:
            return event_queue.get(timeout=timeout)
        except queue.Empty:
            raise exceptions.WatchTimedOut()
        finally:
            w.stop()

    def watch_prefix_once(
        self,
        key_prefix: str | bytes,
        timeout: float | int | None = None,
        *,
        start_revision: int | None = None,
        progress_notify: bool | None = None,
        filters: list[Literal['NOPUT', 'NODELETE']] | None = None,
        prev_kv: bool | None = None,
        watch_id: int | None = None,
        fragment: bool | None = None,
        **kwargs: Any,
    ) -> Event:
        """Watches a range of keys with a prefix, similar to watch_once.

        :param key_prefix: key prefix to watch
        :param timeout: (optional) timeout in seconds.
        :param start_revision: revision to watch from
        :param progress_notify: get periodic progress notifications
        :param filters: filter types (``"NOPUT"`` or ``"NODELETE"``)
        :param prev_kv: return the previous key-value on event
        :param watch_id: ID to assign to this watcher; 0 means auto-assign
            (etcd >= 3.4)
        :param fragment: split large watch responses into multiple smaller
            responses (etcd >= 3.4)
        """
        return self.watch_once(
            key_prefix,
            timeout=timeout,
            range_end=_encode(_increment_last_byte(key_prefix)),
            start_revision=start_revision,
            progress_notify=progress_notify,
            filters=filters,
            prev_kv=prev_kv,
            **kwargs,
        )


def client(
    host: str = 'localhost',
    port: int = 2379,
    ca_cert: str | None = None,
    cert_key: str | None = None,
    cert_cert: str | None = None,
    timeout: float | None = None,
    protocol: str = "http",
    api_path: str | None = DEFAULT_API_PATH,
    session: requests.Session | None = None,
) -> Etcd3Client:
    """Return an instance of an Etcd3Client."""
    return Etcd3Client(
        host=host,
        port=port,
        ca_cert=ca_cert,
        cert_key=cert_key,
        cert_cert=cert_cert,
        timeout=timeout,
        api_path=api_path,
        protocol=protocol,
        session=session,
    )
