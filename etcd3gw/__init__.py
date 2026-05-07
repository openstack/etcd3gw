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

from typing import cast
import warnings

import pbr.version

from etcd3gw.client import client
from etcd3gw.client import Etcd3Client
from etcd3gw.lease import Lease
from etcd3gw.lock import Lock
from etcd3gw import types
from etcd3gw import utils

__all__ = (
    'Etcd3Client',
    'Lease',
    'Lock',
    'client',
    'types',
    'utils',
)


def __getattr__(name: str) -> str:
    if name == '__version__':
        warnings.warn(
            "Accessing etcd3gw.__version__ is deprecated and will be "
            "removed in a future release. Use importlib.metadata instead: "
            "importlib.metadata.version('etcd3gw')",
            DeprecationWarning,
            stacklevel=2,
        )
        return cast(str, pbr.version.VersionInfo('etcd3gw').version_string())
    raise AttributeError(f"module 'etcd3gw' has no attribute {name!r}")
