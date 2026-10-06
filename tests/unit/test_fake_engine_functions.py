# Copyright (c) 2026 Kenneth Stott
# Canary: f942850b-d327-4e9c-8c97-5e2709afecfe
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The platform fake key and the fake functions inside the embedded engine (REQ-1494)."""

from __future__ import annotations

import hashlib
import hmac

import duckdb
import pytest

from provisa.fakes import digest as digest_mod
from provisa.fakes import platform_key
from provisa.fakes.duckdb_functions import register

_KEY = b"k" * 32
_FP = "1d169c852c85cba1"  # fingerprint(_KEY), as the engine plugin's own test pins it


@pytest.fixture
def con():
    previous = digest_mod._key
    digest_mod.set_key(_KEY)
    c = duckdb.connect()
    register(c)
    yield c
    digest_mod._key = previous


def test_the_digest_is_the_published_definition(con):
    expected = int.from_bytes(
        hmac.new(_KEY, b"ann@example.com", hashlib.sha256).digest()[:8], "big", signed=True
    )
    assert digest_mod.fingerprint(_KEY) == _FP
    (got,) = con.execute(f"SELECT provisa_digest('{_FP}', 'ann@example.com')").fetchone()
    assert got == expected == digest_mod.digest(_KEY, "ann@example.com")
    (null,) = con.execute(f"SELECT provisa_digest('{_FP}', NULL)").fetchone()
    assert null is None


def test_a_statement_names_the_key_by_fingerprint_and_another_key_is_refused(con):
    with pytest.raises(duckdb.Error, match="holds no fake key 0000000000000000"):
        con.execute("SELECT provisa_digest('0000000000000000', 'x')").fetchone()


def test_a_fake_method_is_one_fake_per_value(con):
    rows = con.execute(
        f"SELECT v, provisa_fake_method('email', '{{}}', provisa_digest('{_FP}', v), 5) AS f "
        "FROM (VALUES ('a'), ('b'), ('a')) t(v)"
    ).fetchall()
    fakes = {v: set() for v, _ in rows}
    for v, f in rows:
        fakes[v].add(f)
        assert "@" in f
    assert len(fakes["a"]) == 1 and fakes["a"] != fakes["b"]
    (with_args,) = con.execute(
        "SELECT provisa_fake_method('pyint', '{\"min_value\": 3, \"max_value\": 3}', 1, 5)"
    ).fetchone()
    assert with_args == "3"


def test_every_method_is_a_function_of_the_digest_alone(monkeypatch):
    """REQ-1494 (determinism): the same digest gives the same value, whatever the clock, the
    time zone or the process-wide random say between the two computations."""
    import random
    import time

    from provisa.fakes.duckdb_functions import fake_method
    from provisa.fakes.methods import UNSUPPORTED, method_names

    declarable = [m for m in method_names() if m not in UNSUPPORTED]
    seeds = (1, -7, 2**62)
    first = {(m, s): fake_method(m, "{}", s, 5) for m in declarable for s in seeds}
    random.seed(12345)
    time.sleep(1.1)  # the clock moves past a second: a method reading it would differ
    monkeypatch.setenv("TZ", "Asia/Tokyo")
    time.tzset()
    try:
        second = {(m, s): fake_method(m, "{}", s, 5) for m in declarable for s in seeds}
    finally:
        monkeypatch.undo()
        time.tzset()
    assert {k for k in first if first[k] != second[k]} == set()


def test_the_seed_is_the_published_definition():
    """The values FakeFunctionsTest.theSeedIsThePublishedDefinition pins on the Trino side."""
    from provisa.fakes.digest import definition_hash, seed

    assert definition_hash("email", {}, None, "varchar") == 5367564509125871640
    assert seed(5975387752016995628, 4780743034503023799) == -2257588482968385356
    assert seed(-1, 1) == -927672734069774303
    assert seed(0, 0) == -2152535657050944081


def test_the_type_enters_the_definition_by_its_family():
    """REQ-1494: int and bigint keys, or varchar and text, agree; a decimal keeps its precision
    and scale, and timestamp and timestamptz stay apart."""
    from provisa.fakes.digest import canonical_type, definition_hash

    def h(t: str) -> int:
        return definition_hash("hash", {}, None, t)

    assert h("int") == h("BIGINT") == h("int8") and h("varchar(20)") == h("text")
    assert h("int") != h("text")
    assert canonical_type("numeric(10, 2)") == "decimal(10,2)" != canonical_type("numeric(12,2)")
    assert canonical_type("timestamp") != canonical_type("timestamp with time zone")
    assert canonical_type("double precision") == canonical_type("float8") == "float"


def test_the_engine_s_seed_is_the_published_mix(con):
    from provisa.fakes.digest import seed

    for d, h in [(0, 0), (-1, 1), (5975387752016995628, 4780743034503023799)]:
        assert con.execute(f"SELECT provisa_seed({d}, {h})").fetchone()[0] == seed(d, h)
    assert con.execute("SELECT provisa_seed(NULL, 1)").fetchone()[0] is None


def test_a_method_s_value_moves_with_its_definition(con):
    """REQ-1494: one value faked under two definitions draws two seeds."""
    from provisa.fakes.duckdb_functions import fake_method

    assert fake_method("name", "{}", 99, 1) != fake_method("name", "{}", 99, 2)
    assert fake_method("name", "{}", 99, 1) == fake_method("name", "{}", 99, 1)


def test_without_the_key_no_fake_is_computed():
    previous = digest_mod._key
    digest_mod._key = None
    try:
        c = duckdb.connect()
        register(c)
        with pytest.raises(duckdb.Error, match="platform fake key is not loaded"):
            c.execute(f"SELECT provisa_digest('{_FP}', 'x')").fetchone()
    finally:
        digest_mod._key = previous


class _Reversing:
    """A stand-in encryption provider whose ciphertext is visibly not the plaintext."""

    def encrypt(self, plaintext: bytes) -> bytes:
        return b"enc:" + plaintext[::-1]

    def decrypt(self, blob: bytes) -> bytes:
        return blob[4:][::-1]


@pytest.fixture
def control_plane(tmp_path, monkeypatch):
    from provisa.core import config_stamp, deployment_settings, settings_registry
    from provisa.core.database import Database, create_engine_from_url
    from provisa.core.schema_admin import metadata
    from provisa.encryption import runtime

    monkeypatch.setattr(runtime, "_service", _Reversing())
    monkeypatch.setattr(deployment_settings, "_db", None)
    monkeypatch.setattr(deployment_settings, "_held", None)
    monkeypatch.delenv(settings_registry.IGNORE_STORED_ENV, raising=False)
    monkeypatch.delenv(platform_key.KEY_DIR_ENV, raising=False)
    monkeypatch.delenv(platform_key.DEPLOYMENT_KEY_ENV, raising=False)
    engine = create_engine_from_url(f"sqlite+pysqlite:///{tmp_path / 'cp.db'}")
    with engine.begin() as conn:
        metadata.create_all(
            conn, tables=[metadata.tables["deployment_settings"], metadata.tables["config_stamp"]]
        )
        config_stamp.install(conn, config_stamp.PLATFORM_TABLES)
    db = Database(engine, name="platform")
    deployment_settings.bind(db)
    yield db
    engine.dispose()


def _stored(db) -> str:
    from provisa.core.schema_admin import deployment_settings as table

    with db.engine.connect() as conn:
        return conn.execute(table.select().where(table.c.key == "fakes.key")).one().value


def test_the_platform_key_is_created_once_and_sealed(control_plane):
    first = platform_key.ensure(control_plane)
    assert len(first) == 32
    assert platform_key.ensure(control_plane) == first
    assert first.hex() not in _stored(control_plane)


def test_a_second_creation_keeps_the_first_key(control_plane):
    from provisa.core import deployment_settings

    first = platform_key.ensure(control_plane)
    assert not deployment_settings.create(control_plane, "fakes.key", "other", updated_by="x")
    assert platform_key.ensure(control_plane) == first


def test_the_key_is_written_to_the_engine_key_directory_by_fingerprint(
    control_plane, tmp_path, monkeypatch
):
    monkeypatch.setenv(platform_key.KEY_DIR_ENV, str(tmp_path / "keys"))
    key = platform_key.ensure(control_plane)
    path = tmp_path / "keys" / f"{digest_mod.fingerprint(key)}.key"
    assert path.read_text() == key.hex()


def test_two_platforms_sharing_a_key_directory_keep_their_own_keys(tmp_path, monkeypatch):
    """Two control planes (two test stacks, two servers) writing into one engine's directory: each
    key file is named by its own fingerprint, so neither overwrites the other."""
    from provisa.core import config_stamp, deployment_settings, settings_registry
    from provisa.core.database import Database, create_engine_from_url
    from provisa.core.schema_admin import metadata
    from provisa.encryption import runtime

    monkeypatch.setattr(runtime, "_service", _Reversing())
    monkeypatch.delenv(settings_registry.IGNORE_STORED_ENV, raising=False)
    monkeypatch.delenv(platform_key.DEPLOYMENT_KEY_ENV, raising=False)
    monkeypatch.setenv(platform_key.KEY_DIR_ENV, str(tmp_path / "keys"))
    keys = []
    for name in ("one", "two"):
        engine = create_engine_from_url(f"sqlite+pysqlite:///{tmp_path / name}.db")
        with engine.begin() as conn:
            metadata.create_all(
                conn,
                tables=[metadata.tables["deployment_settings"], metadata.tables["config_stamp"]],
            )
            config_stamp.install(conn, config_stamp.PLATFORM_TABLES)
        db = Database(engine, name=name)
        deployment_settings.bind(db)
        monkeypatch.setattr(deployment_settings, "_held", None)
        keys.append(platform_key.ensure(db))
        engine.dispose()
    assert keys[0] != keys[1]
    for key in keys:
        path = tmp_path / "keys" / f"{digest_mod.fingerprint(key)}.key"
        assert path.read_text() == key.hex()


def test_the_platform_key_is_created_from_the_deployments_key(control_plane, monkeypatch):
    deployed = bytes(range(32))
    monkeypatch.setenv(platform_key.DEPLOYMENT_KEY_ENV, deployed.hex())
    assert platform_key.ensure(control_plane) == deployed
    monkeypatch.setenv(platform_key.DEPLOYMENT_KEY_ENV, (b"x" * 32).hex())
    with pytest.raises(platform_key.FakeKeyMismatch, match="is not the platform's"):
        platform_key.ensure(control_plane)
