# Copyright (c) 2026 Kenneth Stott
# Canary: 1b73ecd2-c048-4978-ac29-b87d20e47b2c
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Provisa's fake functions inside the Trino engine (REQ-1494): the digest is the published
definition, keyed by the platform key the deployment mounts into the engine and never sent in a
statement; a fake method gives one fake per value."""

from __future__ import annotations

import os
import secrets
from pathlib import Path

import pytest
import trino

from provisa.fakes.digest import digest, fingerprint

pytestmark = [pytest.mark.integration]


@pytest.fixture
def key():
    """A key written to the engine's key directory, as a Provisa sharing the engine writes its own."""
    value = secrets.token_bytes(32)
    path = Path(os.environ["PROVISA_FAKE_KEY_DIR"]) / f"{fingerprint(value)}.key"
    path.write_text(value.hex())
    yield value
    path.unlink()


def _one(conn, sql: str):
    cur = conn.cursor()
    cur.execute(sql)
    return cur.fetchall()[0][0]


def test_the_engine_digest_is_the_published_definition(trino_conn, key):
    fp = fingerprint(key)
    assert _one(trino_conn, f"SELECT provisa_digest('{fp}', 'ann@example.com')") == digest(
        key, "ann@example.com"
    )
    assert _one(trino_conn, f"SELECT provisa_digest('{fp}', CAST(NULL AS VARCHAR))") is None


def test_two_keys_side_by_side_each_digest_under_its_own(trino_conn, key):
    """Two platforms sharing the engine: each statement names its key, and gets its own digest."""
    other = secrets.token_bytes(32)
    path = Path(os.environ["PROVISA_FAKE_KEY_DIR"]) / f"{fingerprint(other)}.key"
    path.write_text(other.hex())
    try:
        for k in (key, other):
            sql = f"SELECT provisa_digest('{fingerprint(k)}', 'x')"
            assert _one(trino_conn, sql) == digest(k, "x")
    finally:
        path.unlink()


def test_a_fake_method_gives_one_fake_per_value(trino_conn, key):
    cur = trino_conn.cursor()
    cur.execute(
        f"SELECT v, provisa_fake_method('email', '{{}}', provisa_digest('{fingerprint(key)}', v)) "
        "FROM (VALUES 'a', 'b', 'a') AS t(v)"
    )
    fakes: dict[str, set[str]] = {}
    for v, f in cur.fetchall():
        fakes.setdefault(v, set()).add(f)
        assert "@" in f
    assert len(fakes["a"]) == 1 and fakes["a"] != fakes["b"]


def test_a_key_the_engine_does_not_hold_is_refused_by_fingerprint(trino_conn):
    with pytest.raises(trino.exceptions.TrinoUserError, match="holds no fake key 0123456789abcdef"):
        _one(trino_conn, "SELECT provisa_digest('0123456789abcdef', 'x')")


def test_a_method_the_engine_has_not_is_refused_by_name(trino_conn, key):
    with pytest.raises(trino.exceptions.TrinoUserError, match="no fake method nonesuch"):
        _one(trino_conn, "SELECT provisa_fake_method('nonesuch', '{}', 1)")
