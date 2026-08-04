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

from typing import Any, NotRequired, Required, TypedDict

"""Types for responses returned by etcd3.

Note that ``int64``/``uint64`` fields are serialised as JSON strings by the
gRPC-gateway, while ``bytes`` fields are base64-encoded. We handle this at the
call site.
"""


class KeyValue(TypedDict):
    """An etcd key-value pair as returned by the range and watch APIs.

    See ``KeyValue`` in
    https://github.com/etcd-io/etcd/blob/main/api/mvccpb/kv.proto for the
    canonical proto definition.

    ``key`` and ``mod_revision`` are always present. ``create_revision`` and
    ``version`` are absent when their proto3 default values apply (zero), which
    occurs on DELETE watch events. ``value`` is absent on DELETE watch events
    and absent from the range API response (extracted separately by the
    caller). ``lease`` is absent when no lease is attached to the key (the
    underlying ``int64`` field is 0, which proto3 JSON omits as the default
    value).
    """

    key: Required[bytes]
    mod_revision: Required[str]
    create_revision: NotRequired[str]
    version: NotRequired[str]
    value: NotRequired[bytes]
    lease: NotRequired[str]


class Event(TypedDict):
    """An etcd event as returned by the gRPC-gateway streaming API.

    See ``Event`` in
    https://github.com/etcd-io/etcd/blob/main/api/mvccpb/kv.proto for the
    canonical proto definition.

    ``kv`` is always present. ``type`` is absent for PUT events (proto3 JSON
    omits the default enum value of 0) and set to ``"DELETE"`` for delete
    events. ``prev_kv`` is only present when the watcher was created with the
    ``prev_kv`` option and requires etcd >= 3.1.
    """

    kv: Required[KeyValue]
    type: NotRequired[str]
    prev_kv: NotRequired[KeyValue]


class RangeResponse(TypedDict):
    """Response from a range (get) operation.

    See ``RangeResponse`` in
    https://github.com/etcd-io/etcd/blob/main/api/etcdserverpb/rpc.proto
    for the canonical proto definition.

    ``header`` is always present. ``kvs`` is absent when no keys match (the
    list is empty). ``more`` is absent when ``False`` (no further pages).
    ``count`` is absent when zero.
    """

    header: Required[dict[str, Any]]
    kvs: NotRequired[list[KeyValue]]
    more: NotRequired[bool]
    count: NotRequired[str]


class StatusResponse(TypedDict):
    """Response from :meth:`~etcd3gw.client.Etcd3Client.status`.

    See ``StatusResponse`` in
    https://github.com/etcd-io/etcd/blob/main/api/etcdserverpb/rpc.proto
    for the canonical proto definition.

    ``header``, ``version``, ``dbSize``, ``leader``, ``raftIndex``, and
    ``raftTerm`` are always present. The remaining fields are absent when
    their proto3 default values apply: ``raftAppliedIndex`` and
    ``dbSizeInUse`` when zero, ``errors`` when the list is empty, and
    ``isLearner`` when ``False``. ``raftAppliedIndex`` and ``dbSizeInUse``
    require etcd >= 3.4.
    """

    header: Required[dict[str, Any]]
    version: Required[str]
    dbSize: Required[str]
    leader: Required[str]
    raftIndex: Required[str]
    raftTerm: Required[str]
    raftAppliedIndex: NotRequired[str]
    errors: NotRequired[list[str]]
    dbSizeInUse: NotRequired[str]
    isLearner: NotRequired[bool]


class Member(TypedDict):
    """An etcd cluster member.

    See ``Member`` in
    https://github.com/etcd-io/etcd/blob/main/api/etcdserverpb/rpc.proto for
    the canonical proto definition.

    ``ID`` is always present. The remaining fields are absent when their proto3
    default values apply: ``name`` when the empty string, ``peerURLs`` and
    ``clientURLs`` when the list is empty (e.g. for an unstarted member), and
    ``isLearner`` when ``False``.
    """

    ID: Required[str]
    name: NotRequired[str]
    peerURLs: NotRequired[list[str]]
    clientURLs: NotRequired[list[str]]
    isLearner: NotRequired[bool]


class TxnResponse(TypedDict):
    """Response from a transaction operation.

    See https://github.com/etcd-io/etcd/blob/main/api/etcdserverpb/rpc.proto
    for the canonical ``TxnResponse`` proto definition.

    ``header`` is always present. ``succeeded`` is absent when ``False``
    (proto3 JSON omits the default boolean value). ``responses`` is absent when
    the list is empty.
    """

    header: Required[dict[str, Any]]
    succeeded: NotRequired[bool]
    # it would be nice to type this but it's pretty complicated
    # https://etcd.io/docs/v3.6/learning/api/#transaction
    responses: NotRequired[list[Any]]


class WatchResponse(TypedDict):
    """A streaming response chunk from the watch API.

    See ``WatchResponse`` in
    https://github.com/etcd-io/etcd/blob/main/api/etcdserverpb/rpc.proto
    for the canonical proto definition.

    ``header`` is always present. All other fields are absent when their
    proto3 default values apply: ``created``, ``canceled``, and ``fragment``
    when ``False``; ``watch_id`` and ``compact_revision`` when zero;
    ``cancel_reason`` when the empty string; ``events`` when the list is
    empty.
    """

    header: Required[dict[str, Any]]
    watch_id: NotRequired[str]
    created: NotRequired[bool]
    canceled: NotRequired[bool]
    compact_revision: NotRequired[str]
    cancel_reason: NotRequired[str]
    fragment: NotRequired[bool]
    events: NotRequired[list[Event]]
