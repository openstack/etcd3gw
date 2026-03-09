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

import time

from etcd3gw.client import Etcd3Client
from etcd3gw.lock import Lock


def main() -> None:
    client = Etcd3Client()

    print('>>>> Status')
    result = client.status()
    print("cluster id : {!r}".format(result['header']['cluster_id']))

    result = client.members()
    print(f"first member info : {result[0]!r}")

    print('>>>> Lease')
    lease = client.lease()
    print(f"Lease id : {lease.id!r}")
    print(f"Lease ttl : {lease.ttl()!r}")
    print(f"Lease refresh : {lease.refresh()!r}")

    result = client.put('foo2', 'bar2', lease)
    print(f"Key put foo2 : {result!r}")
    result = client.put('foo3', 'bar3', lease)
    print(f"Key put foo3 : {result!r}")
    print(f"Lease Keys : {lease.keys()!r}")

    result = lease.revoke()
    print(f"Lease Revoke : {result!r}")

    result = client.get('foox')
    print(f"Key get foox : {result!r}")

    result = client.put('foo', 'bar')
    print(f"Key put foo : {result!r}")
    result = client.get('foo')
    print(f"Key get foo : {result!r}")
    result = client.delete('foo')
    print(f"Key delete foo : {result!r}")
    result = client.delete('foo-unknown')
    print(f"Key delete foo-unknown : {result!r}")

    print('>>>> Lock')
    lock = Lock(f'xyz-{time.perf_counter()}', ttl=10000, client=client)
    result = lock.acquire()
    print(f"acquire : {result!r}")
    result = lock.refresh()
    print(f"refresh : {result!r}")
    result = lock.is_acquired()
    print(f"is_acquired : {result!r}")
    result = lock.release()
    print(f"release : {result!r}")
    result = lock.is_acquired()
    print(f"is_acquired : {result!r}")


if __name__ == "__main__":
    main()
