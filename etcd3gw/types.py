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

from typing import Any, TypedDict

"""Types for responses returned by etcd3.

Note that ``int64``/``uint64`` fields are serialised as JSON strings by the
gRPC-gateway, while ``bytes`` fields are base64-encoded. We handle this at the
call site.
"""


# NOTE(stephenfin): We can remove the *Base definitions once we drop Python
# 3.10 support and can use Required/NotRequired (PEP-655)


class _KeyValueBase(TypedDict):
    """Required fields present on every KeyValue."""

    key: bytes
    mod_revision: str


class KeyValue(_KeyValueBase, total=False):
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

    create_revision: str
    version: str
    value: bytes
    lease: str


class _EventBase(TypedDict):
    """Required fields present on every event."""

    kv: KeyValue


class Event(_EventBase, total=False):
    """An etcd event as returned by the gRPC-gateway streaming API.

    See ``Event`` in
    https://github.com/etcd-io/etcd/blob/main/api/mvccpb/kv.proto for the
    canonical proto definition.

    ``kv`` is always present. ``type`` is absent for PUT events (proto3 JSON
    omits the default enum value of 0) and set to ``"DELETE"`` for delete
    events. ``prev_kv`` is only present when the watcher was created with the
    ``prev_kv`` option and requires etcd >= 3.1.
    """

    type: str
    prev_kv: KeyValue


class _RangeResponseBase(TypedDict):
    """Required fields present in every range response."""

    header: dict[str, Any]


class RangeResponse(_RangeResponseBase, total=False):
    """Response from a range (get) operation.

    See ``RangeResponse`` in
    https://github.com/etcd-io/etcd/blob/main/api/etcdserverpb/rpc.proto
    for the canonical proto definition.

    ``header`` is always present. ``kvs`` is absent when no keys match (the
    list is empty). ``more`` is absent when ``False`` (no further pages).
    ``count`` is absent when zero.
    """

    kvs: list[KeyValue]
    more: bool
    count: str


class _StatusResponseBase(TypedDict):
    """Required fields present in every status response."""

    header: dict[str, Any]
    version: str
    dbSize: str
    leader: str
    raftIndex: str
    raftTerm: str


class StatusResponse(_StatusResponseBase, total=False):
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

    raftAppliedIndex: str
    errors: list[str]
    dbSizeInUse: str
    isLearner: bool


class _MemberBase(TypedDict):
    """Required fields present on every cluster member."""

    ID: str


class Member(_MemberBase, total=False):
    """An etcd cluster member.

    See ``Member`` in
    https://github.com/etcd-io/etcd/blob/main/api/etcdserverpb/rpc.proto for
    the canonical proto definition.

    ``ID`` is always present. The remaining fields are absent when their proto3
    default values apply: ``name`` when the empty string, ``peerURLs`` and
    ``clientURLs`` when the list is empty (e.g. for an unstarted member), and
    ``isLearner`` when ``False``.
    """

    name: str
    peerURLs: list[str]
    clientURLs: list[str]
    isLearner: bool


class _TxnResponseBase(TypedDict):
    """Required fields present in every transaction response."""

    header: dict[str, Any]


class TxnResponse(_TxnResponseBase, total=False):
    """Response from a transaction operation.

    See https://github.com/etcd-io/etcd/blob/main/api/etcdserverpb/rpc.proto
    for the canonical ``TxnResponse`` proto definition.

    ``header`` is always present. ``succeeded`` is absent when ``False``
    (proto3 JSON omits the default boolean value). ``responses`` is absent when
    the list is empty.
    """

    succeeded: bool
    # it would be nice to type this but it's pretty complicated
    # https://etcd.io/docs/v3.6/learning/api/#transaction
    responses: list[Any]


class _WatchResponseBase(TypedDict):
    """Required fields present in every watch response."""

    header: dict[str, Any]


class WatchResponse(_WatchResponseBase, total=False):
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

    watch_id: str
    created: bool
    canceled: bool
    compact_revision: str
    cancel_reason: str
    fragment: bool
    events: list[Event]
