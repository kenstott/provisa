# Copyright (c) 2026 Kenneth Stott
# Canary: b467a186-36ee-4fe9-8c53-3c8cc023bb45
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The fake functions inside the embedded DuckDB engine (REQ-1494).

* ``provisa_digest(fingerprint, text) -> BIGINT``: the keyed digest of ``text`` under the key
  ``fingerprint`` names (``provisa.fakes.digest``); refused when this process holds another.
* ``provisa_digest_tag(digest) -> VARCHAR``: the digest in base 36, the short part a fake that
  must stay distinct carries.
* ``provisa_fake_method(method, args, digest) -> VARCHAR``: the named fake method called with
  ``args`` (a JSON object), its generator seeded by ``digest``, so one value always gives one fake.
  This engine's own implementation of the method: consistent within the engine, and not the
  portable definition a stable fake is computed by.

The platform key is read inside the function from the process (``digest.platform_key``); no
statement carries it.
"""

# Requirements: REQ-1494

from __future__ import annotations

import datetime as _dt
import json
import threading
from typing import Any

from provisa.fakes.digest import FakeKeyMissing, digest, fingerprint, platform_key, tag
from provisa.fakes.methods import LOCALE, column_value

_local = threading.local()


def _generator() -> Any:
    gen = getattr(_local, "generator", None)
    if gen is None:
        from faker import Faker

        gen = _local.generator = Faker(LOCALE)
    return gen


def fake_digest(key_fingerprint: str, text: str | None) -> int | None:
    if text is None:
        return None
    key = platform_key()
    if fingerprint(key) != key_fingerprint:
        raise FakeKeyMissing(f"this engine holds no fake key {key_fingerprint}")
    return digest(key, text)


def digest_tag(value_digest: int | None) -> str | None:
    return None if value_digest is None else tag(value_digest)


def fake_method(method: str, args: str, seed: int | None) -> str | None:
    """The method's value for the value whose digest is ``seed``, as text; NULL for NULL."""
    if seed is None:
        return None
    gen = _generator()
    gen.seed_instance(seed)
    value = column_value(method, getattr(gen, method)(**json.loads(args)))
    if value is None:
        return None  # a method that makes NULL (null_boolean) shows NULL
    if isinstance(value, _dt.datetime):
        return value.isoformat(sep=" ")
    if isinstance(value, _dt.date):
        return value.isoformat()
    return str(value)


def register(con: Any) -> None:
    """Create the fake functions on DuckDB connection ``con``."""
    con.create_function(
        "provisa_digest",
        fake_digest,
        ["VARCHAR", "VARCHAR"],
        "BIGINT",
        null_handling="special",
        side_effects=False,
    )
    con.create_function(
        "provisa_digest_tag",
        digest_tag,
        ["BIGINT"],
        "VARCHAR",
        null_handling="special",
        side_effects=False,
    )
    con.create_function(
        "provisa_fake_method",
        fake_method,
        ["VARCHAR", "VARCHAR", "BIGINT"],
        "VARCHAR",
        null_handling="special",
        side_effects=False,
    )
