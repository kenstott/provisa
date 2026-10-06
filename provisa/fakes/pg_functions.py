# Copyright (c) 2026 Kenneth Stott
# Canary: a023ef1f-a06a-4e48-b893-804f829b306f
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The fake functions inside the pg engine (REQ-1494): PL/Python (plpython3u) functions calling
this module, so PostgreSQL computes the same digest and the same fake methods as the embedded
DuckDB engine (``provisa.fakes.duckdb_functions``) -- identical by construction.

The key a statement names by its fingerprint is read inside the function from
``<PROVISA_FAKE_KEY_DIR>/<fingerprint>.key`` -- the directory the platform writes its key to,
given to the postmaster's environment -- never from statement text.
"""

# Requirements: REQ-1494

from __future__ import annotations

import os
from pathlib import Path

from provisa.fakes.digest import FakeKeyMissing, digest, fingerprint, tag
from provisa.fakes.duckdb_functions import fake_method

_keys: dict[str, bytes] = {}


def _key(key_fingerprint: str) -> bytes:
    key = _keys.get(key_fingerprint)
    if key is not None:
        return key
    directory = os.environ.get("PROVISA_FAKE_KEY_DIR")
    if not directory:
        raise FakeKeyMissing(
            "PROVISA_FAKE_KEY_DIR is not set: the engine holds no platform fake key"
        )
    path = Path(directory) / f"{key_fingerprint}.key"
    if not path.exists():
        raise FakeKeyMissing(f"this engine holds no fake key {key_fingerprint}")
    key = bytes.fromhex(path.read_text().strip())
    if fingerprint(key) != key_fingerprint:
        raise FakeKeyMissing(f"the fake key file {path} holds another key")
    _keys[key_fingerprint] = key
    return key


def pg_digest(key_fingerprint: str, text: str | None) -> int | None:
    return None if text is None else digest(_key(key_fingerprint), text)


def pg_digest_tag(value_digest: int | None) -> str | None:
    return None if value_digest is None else tag(value_digest)


def pg_fake_method(method: str, args: str, value_digest: int | None, def_hash: int) -> str | None:
    return fake_method(method, args, value_digest, def_hash)


#: The functions, as the pg engine creates them once plpython3u is installed. No parameter is
#: named ``args``: PL/Python binds that name to the list of every argument.
FUNCTIONS_SQL = """
CREATE OR REPLACE FUNCTION provisa_digest(fp text, value text) RETURNS bigint
LANGUAGE plpython3u IMMUTABLE PARALLEL SAFE AS $$
from provisa.fakes.pg_functions import pg_digest
return pg_digest(fp, value)
$$;
CREATE OR REPLACE FUNCTION provisa_digest_tag(value_digest bigint) RETURNS text
LANGUAGE plpython3u IMMUTABLE PARALLEL SAFE AS $$
from provisa.fakes.pg_functions import pg_digest_tag
return pg_digest_tag(value_digest)
$$;
CREATE OR REPLACE FUNCTION provisa_seed(value_digest bigint, def_hash bigint) RETURNS bigint
LANGUAGE plpython3u IMMUTABLE PARALLEL SAFE AS $$
from provisa.fakes.duckdb_functions import value_seed
return value_seed(value_digest, def_hash)
$$;
CREATE OR REPLACE FUNCTION provisa_fake_method(
    method text, arguments text, value_digest bigint, def_hash bigint
) RETURNS text LANGUAGE plpython3u IMMUTABLE PARALLEL SAFE AS $$
from provisa.fakes.pg_functions import pg_fake_method
return pg_fake_method(method, arguments, value_digest, def_hash)
$$;
CREATE OR REPLACE FUNCTION provisa_stable_fake(
    method text, version integer, value_digest bigint, def_hash bigint
) RETURNS text LANGUAGE plpython3u IMMUTABLE PARALLEL SAFE AS $$
from provisa.fakes.portable import stable_fake
return stable_fake(method, version, value_digest, def_hash)
$$;
"""
