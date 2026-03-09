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

import types
from typing import TYPE_CHECKING, cast
import uuid

from etcd3gw import exceptions
from etcd3gw.utils import _encode
from etcd3gw.utils import DEFAULT_TIMEOUT
from etcd3gw.utils import LOCK_PREFIX

if TYPE_CHECKING:
    from etcd3gw import client as _client_module
    from etcd3gw import lease as _lease_module


class Lock:
    def __init__(
        self,
        name: str,
        ttl: int = DEFAULT_TIMEOUT,
        *,
        client: '_client_module.Etcd3Client',
    ) -> None:
        """Create a lock using the given name with specified timeout

        :param name:
        :param ttl:
        :param client:
        """
        self.name = name
        self.ttl = ttl
        self.client: _client_module.Etcd3Client = client
        self.key = LOCK_PREFIX + self.name
        self.lease: _lease_module.Lease | None = None
        self._uuid = str(uuid.uuid1())

    @property
    def uuid(self) -> str:
        """The unique id of the lock"""
        return self._uuid

    def acquire(self) -> bool:
        """Acquire the lock."""
        self.lease = self.client.lease(self.ttl)

        base64_key = _encode(self.key)
        base64_value = _encode(self._uuid)
        txn = {
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
                        'lease': self.lease.id,
                    }
                }
            ],
            'failure': [{'request_range': {'key': base64_key}}],
        }
        result = self.client.transaction(txn)
        if 'succeeded' in result:
            return cast(bool, result['succeeded'])
        return False

    def release(self) -> bool:
        """Release the lock"""
        base64_key = _encode(self.key)
        base64_value = _encode(self._uuid)

        txn = {
            'compare': [
                {
                    'key': base64_key,
                    'result': 'EQUAL',
                    'target': 'VALUE',
                    'value': base64_value,
                }
            ],
            'success': [{'request_delete_range': {'key': base64_key}}],
        }

        result = self.client.transaction(txn)
        if 'succeeded' in result:
            return cast(bool, result['succeeded'])
        return False

    def refresh(self) -> int:
        """Refresh the lease on the lock

        :return:
        """
        if self.lease is None:
            raise exceptions.Etcd3Exception('lease must be acquired first')

        return self.lease.refresh()

    def is_acquired(self) -> bool:
        """Check if the lock is acquired"""
        values = self.client.get(self.key)
        return self._uuid.encode("latin-1") in values

    def __enter__(self) -> 'Lock':
        """Use the lock as a contextmanager"""
        self.acquire()
        return self

    def __exit__(
        self,
        exception_type: type[BaseException] | None,
        exception_value: BaseException | None,
        traceback: types.TracebackType | None,
    ) -> None:
        self.release()
