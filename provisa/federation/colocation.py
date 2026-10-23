# Copyright (c) 2026 Kenneth Stott
# Canary: 51a46bc8-fac0-4111-bf65-addbc1c0049f
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Which sources one direct statement may read together (REQ-863, REQ-028): those on the same
physical database under the same login. A statement that reads two of them runs on one source's
connection, under its credentials; two logins on one database are two grants, so sources that
differ in user, credential or how they authenticate are never combined, and the engine reads
them. The key names the login by its user and a digest of what it authenticates with -- the
password and every connection extra (a key, a token, an IAM role) -- never the secret itself."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping


def colocation_key(
    host: str,
    port: int | str | None,
    database: str,
    user: str,
    credential: str,
    extras: Mapping[str, str] | None = None,
) -> str:
    """The key two sources share only when one connection may read both: the database, the
    user, and a digest of the credential and connection extras."""
    material = json.dumps(
        {"credential": credential, "extras": dict(sorted((extras or {}).items()))},
        sort_keys=True,
    )
    digest = hashlib.sha256(material.encode()).hexdigest()[:16]
    return f"{host}:{port}/{database}|{user}|{digest}"
