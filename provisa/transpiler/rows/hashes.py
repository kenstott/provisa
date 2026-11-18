# Copyright (c) 2026 Kenneth Stott
# Canary: 887cdad1-7eef-450b-90ea-c6af5055ec73
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The function table's rows of digests, encodings and hash functions (REQ-2167, REQ-2171):
``ROWS``, which provisa.transpiler.rows collects into provisa.transpiler.function_macros.MACROS."""

# Requirements: REQ-2167, REQ-2171, REQ-2236

from __future__ import annotations

from provisa.transpiler import pg_forms_hash
from provisa.transpiler.rows.base import _DUCKDB, _SPARK, Forms, Macro, Pg

#: md5 and the SHA-2 digests (REQ-2171, AN ANSWER THAT DIFFERS IS REFUSED). A dialect is listed
#: where replaying postgres:17's answers on its engine matched (Trino 481, Spark 3.5.1, ClickHouse
#: 24.3, MariaDB 11.8, SQL Server 2022, DuckDB); a dialect replay has not reached is refused, as
#: Oracle (its empty string is NULL, so a digest of nothing is NULL) and SQLite (no digest) are.
#: sqlglot's own writing is not an answer: it writes sha384 and sha512 as SHA256 on DuckDB and
#: sha384 as HASH_SHA512 on Exasol, a digest as its hex text on Trino, and HASHBYTES('SHA2_384'),
#: which is NULL, on SQL Server. Athena and Databricks stand with the engines they run.
_TRINOS = frozenset({"trino", "athena"})
_MD5S = _SPARK | {"duckdb", "mysql", "clickhouse"}
#: md5(text) hashes what the dialect holds of the text; SQL Server's nvarchar holds UTF-16, so only
#: a value the statement says is a bytea has PostgreSQL's digest there.
_MD5_FORMS: Forms = {
    _MD5S: Pg("md5(value)"),
    _TRINOS: "lower(to_hex(md5(CAST(value AS VARBINARY))))",
}
#: SHA2 answers the digest's hex text here; PostgreSQL's digest is its bytes.
_SHA2_HEX = _SPARK | {"mysql"}


def _sha(bits: int) -> Macro:
    """sha<bits>, of a bytea: PostgreSQL has no overload of text."""
    extension = f"crypto_hash('sha2-{bits}', CAST(value AS BLOB))"
    forms: dict[frozenset[str], str] = {
        _DUCKDB: "unhex(sha256(CAST(value AS BLOB)))" if bits == 256 else extension,
        _SHA2_HEX: f"unhex(sha2(value, {bits}))",
        frozenset({"clickhouse"}): f"SHA{bits}(value)",
    }
    if bits in (256, 512):  # the digests Trino and SQL Server have
        forms[_TRINOS] = f"sha{bits}(value)"
        forms[frozenset({"tsql"})] = Pg(f"sha{bits}(value)")
    return Macro(f"sha{bits}", ("value",), forms)


_DIGESTS: tuple[Macro, ...] = (
    Macro("md5", ("value",), _MD5_FORMS),
    Macro(
        "md5_bytea",
        ("value",),
        {
            **_MD5_FORMS,
            frozenset({"tsql"}): "LOWER(CONVERT(VARCHAR(32), HASHBYTES('MD5', value), 2))",
        },
    ),
    *(_sha(bits) for bits in (224, 256, 384, 512)),
    # encode's formats: sqlglot reads the format as a charset, which it writes as TO_UTF8 on Trino.
    Macro(
        "encode_hex",
        ("value",),
        {
            _DUCKDB: "lower(hex(value))",
            _TRINOS: "lower(to_hex(value))",
            _SPARK | {"mysql", "clickhouse"}: "lower(hex(value))",
            frozenset({"tsql"}): "LOWER(CONVERT(VARCHAR(MAX), value, 2))",
        },
    ),
    # base64: PostgreSQL wraps a line at 76 characters, which no engine's own does.
    Macro("encode_base64", ("value",), {_DUCKDB: "to_base64(value)"}),
    Macro("encode_escape", ("value",), {_DUCKDB: "provisa_encode_escape(value)"}),
)
#: The rows whose argument is a bytea: a literal there is bytea's input, not text.
_BYTEA_INPUT = frozenset({"md5_bytea", "sha224", "sha256", "sha384", "sha512"})
#: What a statement says of a value PostgreSQL has no bytea overload for.
_TEXT_TYPES = frozenset({"text", "varchar", "character varying", "bpchar", "char", "character"})


def _hashes() -> tuple[Macro, ...]:
    """Each hash function on each engine with a fold; on any other its call is refused by name."""
    forms: dict[tuple[str, str], dict[frozenset[str], str]] = {}
    for dialect, engine in pg_forms_hash.ENGINES:
        for key, form in pg_forms_hash.forms(engine).items():
            forms.setdefault(key, {})[frozenset({dialect})] = form
    # A composite of no said type has no row of its element's: it is refused by name.
    bare = tuple(
        Macro(name, tuple(params.split(", "))) for name, params in pg_forms_hash.COMPOSITES
    )
    return bare + tuple(
        Macro(name, tuple(params.split(", ")), written) for (name, params), written in forms.items()
    )


ROWS: tuple[Macro, ...] = (
    *_DIGESTS,
    *_hashes(),
)
