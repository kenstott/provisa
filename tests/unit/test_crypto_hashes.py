# Copyright (c) 2026 Kenneth Stott
# Canary: 6a343cbd-8eac-4516-b6e7-74fbafc2900b
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""AN ANSWER THAT DIFFERS IS REFUSED (REQ-2171), for md5 and the SHA-2 digests: each answers as
postgres:17 does or is refused by name, and no dialect is written another algorithm.

RECORDED is postgres:17.11's answer to each statement. The same statements, transpiled, were
replayed on Trino 481, Spark 3.5.1, ClickHouse 24.3, MariaDB 11.8 and SQL Server 2022 by hand, and
matched where they are not refused; DuckDB replays them here, through the crypto extension, which
this module stages as an install would (a network INSTALL, into a directory of its own)."""

# Requirements: REQ-2171

from __future__ import annotations

import hashlib
import re

import duckdb
import pytest
import sqlglot
from sqlglot.dialects.dialect import Dialect

from provisa.federation import duckdb_extensions
from provisa.federation.duckdb_extensions import (
    EXTENSION_REPOSITORIES,
    REPOSITORY_ANCHORS,
    running_version_platform,
    write_manifest,
)
from provisa.federation.duckdb_pg_functions import register
from provisa.transpiler.function_macros import FunctionRefused
from provisa.transpiler.transpile import transpile, transpile_to_duckdb

_VALUES = {
    "text": "'abc'",
    "utf8": "'é'",
    "empty_literal": "''",
    "hex_literal": "'\\x00ff'",
    "bytea": "'\\x00ff'::bytea",
    "empty_bytea": "'\\x'::bytea",
    "null_bytea": "NULL::bytea",
}
_MD5_VALUES = {
    **{k: v for k, v in _VALUES.items() if k != "hex_literal"},
    "cast_text": "'abc'::text",
    "null_text": "NULL::text",
    "backslash_text": "'\\x00ff'",  # md5's literal is text: PostgreSQL prefers that overload
}
_BITS = (224, 256, 384, 512)
_STATEMENTS = {
    **{f"md5/{k}": f"SELECT md5({v}) AS v" for k, v in _MD5_VALUES.items()},
    **{
        f"sha{bits}/{k}": f"SELECT encode(sha{bits}({v}), 'hex') AS v"
        for bits in _BITS
        for k, v in _VALUES.items()
    },
}

#: postgres:17.11, the statements above.
RECORDED: dict[str, str | None] = {
    "md5/text": "900150983cd24fb0d6963f7d28e17f72",
    "md5/utf8": "66ddcd97cfdeabb2f6fb8a999b4bc76f",
    "md5/empty_literal": "d41d8cd98f00b204e9800998ecf8427e",
    "md5/bytea": "d07d34efac6328007ad67c7e0a985e00",
    "md5/empty_bytea": "d41d8cd98f00b204e9800998ecf8427e",
    "md5/null_bytea": None,
    "md5/cast_text": "900150983cd24fb0d6963f7d28e17f72",
    "md5/null_text": None,
    "md5/backslash_text": "a6477e08ad78f99971aeac6571b4882a",
    "sha224/text": "23097d223405d8228642a477bda255b32aadbce4bda0b3f7e36c9da7",
    "sha224/utf8": "33dd9448e5538d16ddc6df32dd7f16240cac6cb5238c1c22cb33ae66",
    "sha224/empty_literal": "d14a028c2a3a2bc9476102bb288234c415a2b01f828ea62ac5b3e42f",
    "sha224/hex_literal": "f6e6401dd7d060d232a0ff6c0ad9283550990e0216ecbe9499e1b20b",
    "sha224/bytea": "f6e6401dd7d060d232a0ff6c0ad9283550990e0216ecbe9499e1b20b",
    "sha224/empty_bytea": "d14a028c2a3a2bc9476102bb288234c415a2b01f828ea62ac5b3e42f",
    "sha224/null_bytea": None,
    "sha256/text": "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad",
    "sha256/utf8": "4a99557e4033c3539de2eb65472017cad5f9557f7a0625a09f1c3f6e2ba69c4c",
    "sha256/empty_literal": "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
    "sha256/hex_literal": "06eb7d6a69ee19e5fbdf749018d3d2abfa04bcbd1365db312eb86dc7169389b8",
    "sha256/bytea": "06eb7d6a69ee19e5fbdf749018d3d2abfa04bcbd1365db312eb86dc7169389b8",
    "sha256/empty_bytea": "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
    "sha256/null_bytea": None,
    "sha384/text": "cb00753f45a35e8bb5a03d699ac65007272c32ab0eded1631a8b605a43ff5bed8086072ba1e7cc2358baeca134c825a7",
    "sha384/utf8": "41a2ddb34dca0b400fa9b73a368307acbed758e85ce50527d1655a263a10e20eefa18f8116767a128877a97c11160a26",
    "sha384/empty_literal": "38b060a751ac96384cd9327eb1b1e36a21fdb71114be07434c0cc7bf63f6e1da274edebfe76f65fbd51ad2f14898b95b",
    "sha384/hex_literal": "af1f0bd8769ed1365727dc5f8ba9d5e66ee64bbadc61cf38293abc8d71d851f5bbd3ac9b3a8ab899e6a01efd7e82ee08",
    "sha384/bytea": "af1f0bd8769ed1365727dc5f8ba9d5e66ee64bbadc61cf38293abc8d71d851f5bbd3ac9b3a8ab899e6a01efd7e82ee08",
    "sha384/empty_bytea": "38b060a751ac96384cd9327eb1b1e36a21fdb71114be07434c0cc7bf63f6e1da274edebfe76f65fbd51ad2f14898b95b",
    "sha384/null_bytea": None,
    "sha512/text": "ddaf35a193617abacc417349ae20413112e6fa4e89a97ea20a9eeee64b55d39a2192992a274fc1a836ba3c23a3feebbd454d4423643ce80e2a9ac94fa54ca49f",
    "sha512/utf8": "9e2ad28633f24451bd4f3c1cb20586a21a44c3aeedbdc01b9cc8fa72917ea7bd689c82b8bf1fef89b911cf8cc46fa2c1ccc10087b2094fd4d3350ecd88526a2c",
    "sha512/empty_literal": "cf83e1357eefb8bdf1542850d66d8007d620e4050b5715dc83f4a921d36ce9ce47d0d13c5d85f2b0ff8318d2877eec2f63b931bd47417a81a538327af927da3e",
    "sha512/hex_literal": "fb9c0429487655082fe197f64eb8f045a706a4a5de8817478ee3ab47e208c27fd87502458026613794e69b0d1b3fc6916456754bd08088422144503682db5986",
    "sha512/bytea": "fb9c0429487655082fe197f64eb8f045a706a4a5de8817478ee3ab47e208c27fd87502458026613794e69b0d1b3fc6916456754bd08088422144503682db5986",
    "sha512/empty_bytea": "cf83e1357eefb8bdf1542850d66d8007d620e4050b5715dc83f4a921d36ce9ce47d0d13c5d85f2b0ff8318d2877eec2f63b931bd47417a81a538327af927da3e",
    "sha512/null_bytea": None,
}


@pytest.fixture(scope="session")
def crypto_extension(tmp_path_factory):
    """The crypto extension staged, as an install's step does: register only loads it. On DuckDB
    2.0 crypto is Query.Farm's, from its own repository, which DuckDB trusts only by the anchor it
    keeps in its extension_repository_directory: the repository is created there (the staged
    directory's, extension_config's), crypto installed from it into repositories/query_farm/,
    and the manifest written, as stage_bundled_extensions leaves a staged directory."""
    directory = tmp_path_factory.mktemp("duckdb-extensions")
    version, platform = running_version_platform()
    staged = directory / version / platform
    repository, prefix = EXTENSION_REPOSITORIES["crypto"]
    con = duckdb.connect(
        config={
            "extension_directory": str(directory),
            "extension_repository_directory": str(staged / REPOSITORY_ANCHORS),
        }
    )
    con.execute("SET allow_extension_repositories = 'allowed'")
    con.execute(f"CREATE EXTENSION REPOSITORY IF NOT EXISTS {repository} WITH PREFIX '{prefix}'")
    con.execute(f"INSTALL crypto FROM {repository}")
    for extension in ("icu", "inet"):  # what a connection on this directory autoloads no more
        con.execute(f"INSTALL {extension}")
    con.close()
    write_manifest(staged, version, platform)
    return str(directory)


@pytest.fixture
def crypto_staged(crypto_extension, monkeypatch):
    """The staged directory named to the process (PROVISA_DUCKDB_EXT_DIR) for this test alone."""
    monkeypatch.setenv("PROVISA_DUCKDB_EXT_DIR", crypto_extension)
    return crypto_extension


@pytest.fixture
def con(crypto_staged):
    # The engine's own connection: opened on the staged directory (duckdb_extensions.connect).
    connection = duckdb_extensions.connect()
    register(connection)
    return connection


@pytest.mark.parametrize("key", list(_STATEMENTS))
def test_duckdb_answers_as_postgresql(con, key):
    assert con.execute(transpile_to_duckdb(_STATEMENTS[key])).fetchone() == (RECORDED[key],)


@pytest.mark.parametrize("bits", _BITS)
@pytest.mark.parametrize(
    ("value", "digits"),
    [("'abc'::bytea", "abc"), ("'\\x'::bytea", ""), ("NULL::bytea", None)],
)
def test_a_digest_is_bytes_on_duckdb_as_it_is_on_postgresql(con, bits, value, digits):
    (got,) = con.execute(transpile_to_duckdb(f"SELECT sha{bits}({value}) AS v")).fetchone()
    if digits is None:
        assert got is None
    else:
        assert bytes(got) == hashlib.new(f"sha{bits}", digits.encode()).digest()


def test_sha512_is_never_sha256_on_duckdb():
    for form in (transpile_to_duckdb, lambda sql: transpile(sql, "duckdb")):
        assert "SHA256" not in form("SELECT sha512(a) FROM t").upper()
        assert "SHA256" not in form("SELECT sha384(a) FROM t").upper()


def test_an_unstaged_extension_refuses_the_call_by_name(monkeypatch, tmp_path):
    monkeypatch.setenv("PROVISA_DUCKDB_EXT_DIR", str(tmp_path))
    connection = duckdb.connect()
    register(connection)
    with pytest.raises(duckdb.Error, match=r"sha512\(\) has no form on duckdb"):
        connection.execute(transpile_to_duckdb("SELECT sha512('abc'::bytea) AS v"))


def test_an_unstaged_extension_leaves_sha256_and_md5_answering(monkeypatch, tmp_path):
    monkeypatch.setenv("PROVISA_DUCKDB_EXT_DIR", str(tmp_path))
    connection = duckdb.connect()
    register(connection)
    for key in ("md5/text", "sha256/text"):
        assert connection.execute(transpile_to_duckdb(_STATEMENTS[key])).fetchone() == (
            RECORDED[key],
        )


def _algorithms(sql: str) -> set[str]:
    """The digests ``sql`` names, on any spelling a dialect has."""
    found = {f"sha{m}" for m in re.findall(r"(?:SHA2?[-_]?(?:2[-_])?)(224|256|384|512)", sql, re.I)}
    found |= {f"sha{m}" for m in re.findall(r"SHA2\([^()]*,\s*(224|256|384|512)\)", sql, re.I)}
    if re.search(r"MD5", sql, re.I):
        found.add("md5")
    return found


@pytest.mark.parametrize("dialect", sorted(d for d in Dialect.classes if d not in ("", "postgres")))
@pytest.mark.parametrize("function", ["md5", "sha224", "sha256", "sha384", "sha512"])
def test_no_dialect_is_written_another_algorithm(dialect, function):
    statement = f"SELECT {function}(a) AS v FROM t"
    try:
        written = transpile(statement, dialect)
    except FunctionRefused as refused:
        assert f"{function}()" in str(refused)
    else:
        assert _algorithms(written) == {function}, written


@pytest.mark.parametrize(
    ("statement", "dialect", "name"),
    [
        # HASHBYTES('SHA2_384') is NULL, not an error.
        ("SELECT sha384(a) FROM t", "tsql", "sha384"),
        ("SELECT sha224(a) FROM t", "tsql", "sha224"),
        # Trino has no sha224 or sha384.
        ("SELECT sha224(a) FROM t", "trino", "sha224"),
        ("SELECT sha384(a) FROM t", "trino", "sha384"),
        # sqlglot writes sha384 as HASH_SHA512.
        ("SELECT sha384(a) FROM t", "exasol", "sha384"),
        # HASHBYTES of an nvarchar is the digest of its UTF-16.
        ("SELECT md5(a) FROM t", "tsql", "md5"),
        # Its empty string is NULL.
        ("SELECT md5(a) FROM t", "oracle", "md5"),
        ("SELECT sha256(a) FROM t", "oracle", "sha256"),
        ("SELECT md5(a) FROM t", "sqlite", "md5"),
        # sqlglot reads the format as a charset.
        ("SELECT encode(a, 'base64') FROM t", "trino", "encode"),
        # PostgreSQL: function sha256(text) does not exist.
        ("SELECT sha256('abc'::text) AS v", "duckdb", "sha256"),
        ("SELECT sha256(a::varchar) FROM t", "trino", "sha256"),
        # PostgreSQL: invalid input syntax for type bytea.
        ("SELECT sha256('a\\b') AS v", "duckdb", "sha256"),
    ],
)
def test_what_differs_is_refused_by_name(statement, dialect, name):
    with pytest.raises(FunctionRefused, match=rf"{name}\(\) has no form on {dialect}"):
        transpile_to_duckdb(statement) if dialect == "duckdb" else transpile(statement, dialect)


@pytest.mark.parametrize(
    ("statement", "dialect", "replayed"),
    [
        ("SELECT sha512(a) AS v FROM t", "trino", "SELECT SHA512(a) AS v FROM t"),
        ("SELECT sha512(a) AS v FROM t", "spark", "SELECT UNHEX(SHA2(a, 512)) AS v FROM t"),
        ("SELECT sha224(a) AS v FROM t", "mysql", "SELECT UNHEX(SHA2(a, 224)) AS v FROM t"),
        ("SELECT sha384(a) AS v FROM t", "clickhouse", "SELECT SHA384(a) AS v FROM t"),
        ("SELECT sha512(a) AS v FROM t", "tsql", "SELECT HASHBYTES('SHA2_512', a) AS v FROM t"),
        (
            "SELECT md5(a) AS v FROM t",
            "trino",
            "SELECT LOWER(TO_HEX(MD5(CAST(a AS VARBINARY)))) AS v FROM t",
        ),
        ("SELECT encode(a, 'hex') AS v FROM t", "trino", "SELECT LOWER(TO_HEX(a)) AS v FROM t"),
    ],
)
def test_a_dialect_gets_the_form_that_was_replayed(statement, dialect, replayed):
    assert transpile(statement, dialect) == replayed


def test_a_bytea_literal_is_its_bytes_on_any_dialect():
    assert "FROM_HEX('616263')" in transpile("SELECT sha256('abc') AS v", "trino")
    assert "0x00ff" in transpile("SELECT sha256('\\x00ff') AS v", "tsql")
    assert "UNHEX('5c')" in transpile("SELECT sha256('\\\\') AS v", "spark")
    assert "UNHEX('01')" in transpile("SELECT sha256('\\001') AS v", "mysql")


def test_a_recorded_answer_keeps_its_statement():
    assert set(RECORDED) == set(_STATEMENTS)
    for statement in _STATEMENTS.values():
        sqlglot.parse_one(statement, read="postgres")
