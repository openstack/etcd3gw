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
    status = client.status()
    print("cluster id : {!r}".format(status['header']['cluster_id']))

    members = client.members()
    print(f"first member info : {members[0]!r}")

    print('>>>> Lease')
    lease = client.lease()
    print(f"Lease id : {lease.id!r}")
    print(f"Lease ttl : {lease.ttl()!r}")
    print(f"Lease refresh : {lease.refresh()!r}")

    print(f"Key put foo2 : {client.put('foo2', 'bar2', lease)!r}")
    print(f"Key put foo3 : {client.put('foo3', 'bar3', lease)!r}")
    print(f"Lease Keys : {lease.keys()!r}")

    print(f"Lease Revoke : {lease.revoke()!r}")

    print(f"Key get foox : {client.get('foox')!r}")

    print(f"Key put foo : {client.put('foo', 'bar')!r}")
    print(f"Key get foo : {client.get('foo')!r}")
    print(f"Key delete foo : {client.delete('foo')!r}")
    print(f"Key delete foo-unknown : {client.delete('foo-unknown')!r}")

    print('>>>> Lock')
    lock = Lock(f'xyz-{time.perf_counter()}', ttl=10000, client=client)
    print(f"acquire : {lock.acquire()!r}")
    print(f"refresh : {lock.refresh()!r}")
    print(f"is_acquired : {lock.is_acquired()!r}")
    print(f"release : {lock.release()!r}")
    print(f"is_acquired : {lock.is_acquired()!r}")


if __name__ == "__main__":
    main()
