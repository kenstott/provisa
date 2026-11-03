# Copyright (c) 2026 Kenneth Stott
# Canary: 6b790c57-06e7-43f1-8ec2-212818e3e7f8
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""What a JDBC source is set up with (REQ-2191).

The settings are the source's ``federation_hints``, the bag its driver is configured from:

- ``url``: the connection URL. Its scheme names the SQLAlchemy dialect and driver
  (``postgresql+psycopg2://host/db``, ``sqlite:///{files}/shop.db``). It carries no password: the
  source's own password field does, as for every source, so a secret reference stays one.
- ``sql_dialect``: the flavor Provisa writes, one of SQLGlot's dialects or ``ansi``. It says
  nothing about the connection, so the two may differ.
- ``capabilities.<engine>``: the capabilities turned on for the source on that engine, a comma
  list of :data:`CAPABILITIES`. All are off to start. Each is held per engine, because each
  depends on what the engine can do with the source: after an engine swap nothing is on for the
  new engine until it is set there.

A setting this module does not read is left to whoever reads it (the ``@provisa`` session
vocabulary shares the bag); a value it reads and cannot accept is refused by name.
"""

# Requirements: REQ-2191

from __future__ import annotations

from dataclasses import dataclass

URL = "url"
SQL_DIALECT = "sql_dialect"
CAPABILITIES_PREFIX = "capabilities."

#: The source's live attach on the engine, and the pushdowns its reads may carry. Function
#: pushdown is not offered.
ATTACH, PREDICATE, JOIN, AGGREGATE, KEY_RANGE = (
    "attach",
    "predicate",
    "join",
    "aggregate",
    "key_range",
)
CAPABILITIES: tuple[str, ...] = (ATTACH, PREDICATE, JOIN, AGGREGATE, KEY_RANGE)

#: The generic dialect's name (provisa.jdbc_source.dialect).
ANSI = "ansi"


#: The SQLAlchemy backend a URL's scheme names (its dialect, without the driver) -> the built-in
#: source type of the same database.
NAMED_TYPES: dict[str, str] = {
    "postgresql": "postgresql",
    "mysql": "mysql",
    "mariadb": "mariadb",
    "mssql": "sqlserver",
    "oracle": "oracle",
    "sqlite": "sqlite",
    "duckdb": "duckdb",
    "clickhouse": "clickhouse",
    "snowflake": "snowflake",
    "bigquery": "bigquery",
    "databricks": "databricks",
    "redshift": "redshift",
    "trino": "trino",
    "hana": "saphana",
    "exa": "exasol",
    "firebird": "firebird",
    "cockroachdb": "cockroachdb",
    "singlestoredb": "singlestore",
    "awsathena": "athena",
    "druid": "druid",
    "pinot": "pinot",
    "hive": "hiveserver2",
}


def named_type(url: str) -> str | None:
    """The built-in source type ``url``'s scheme names; None for a jdbc: URL and for a scheme
    naming none."""
    return NAMED_TYPES.get(backend(url))


class InvalidJdbcSource(ValueError):
    """A JDBC source's settings that cannot be a source."""


def sql_dialects() -> tuple[str, ...]:
    """The SQL dialects a source may be written in: SQLGlot's, and ``ansi``."""
    from sqlglot.dialects import Dialects

    return (*sorted(d.value for d in Dialects if d.value), ANSI)


def backend(url: str) -> str:
    """The dialect a connection URL's scheme names, without its driver
    (``postgresql+psycopg2://...`` -> ``postgresql``); ``jdbc`` for a customer's JDBC driver
    (``provisa.jdbc_source.jdbc``)."""
    if url.strip().lower().startswith("jdbc:"):
        return "jdbc"
    scheme, sep, _ = url.partition("://")
    if not sep or not scheme:
        raise InvalidJdbcSource(f"{url!r} is not a connection URL: it names no scheme")
    return scheme.split("+", 1)[0].lower()


def capabilities_key(engine: str) -> str:
    """The setting that holds the capabilities turned on for the source on ``engine``."""
    return f"{CAPABILITIES_PREFIX}{engine}"


@dataclass(frozen=True)
class JdbcSourceSettings:
    url: str
    sql_dialect: str
    #: engine name -> the capabilities turned on there.
    capabilities: dict[str, frozenset[str]]

    def on(self, engine: str, capability: str) -> bool:
        """Whether ``capability`` is turned on for the source on ``engine``."""
        return capability in self.capabilities.get(engine, frozenset())


def _capabilities(url: str, key: str, value: str) -> frozenset[str]:
    named = frozenset(c.strip() for c in value.split(",") if c.strip())
    unknown = sorted(named - set(CAPABILITIES))
    if unknown:
        raise InvalidJdbcSource(
            f"{key}: {', '.join(unknown)} is not a capability; the capabilities are "
            f"{', '.join(CAPABILITIES)}"
        )
    if ATTACH in named:
        if named_type(url) is None:
            raise InvalidJdbcSource(
                f"{key}: a {backend(url)} URL has no engine attach to turn on: it names no "
                "built-in source type"
            )
    return named


def parse(hints: dict[str, str]) -> JdbcSourceSettings:
    """The settings ``hints`` hold, refused by name where they cannot be a source."""
    url = (hints.get(URL) or "").strip()
    if not url:
        raise InvalidJdbcSource("a JDBC source needs its connection URL")
    if backend(url) == "jdbc":
        from provisa.jdbc_source.jdbc import driver_settings

        driver_settings(hints)
    dialect = (hints.get(SQL_DIALECT) or "").strip()
    if dialect not in sql_dialects():
        raise InvalidJdbcSource(
            f"{dialect!r} is not a SQL dialect Provisa writes; choose one of "
            f"{', '.join(sql_dialects())}"
            if dialect
            else "a JDBC source needs the SQL dialect Provisa writes for it"
        )
    capabilities = {
        key[len(CAPABILITIES_PREFIX) :]: _capabilities(url, key, value)
        for key, value in hints.items()
        if key.startswith(CAPABILITIES_PREFIX)
    }
    return JdbcSourceSettings(url=url, sql_dialect=dialect, capabilities=capabilities)


def capability(hints: dict[str, str], engine: str):
    """The pushdown the source's settings turn on for ``engine``, as the connector capability the
    planner reads (``provisa.federation.connector_base.Capability``). Never a write: a JDBC
    source is written through its own driver."""
    from provisa.federation.connector_base import Capability

    settings = parse(hints)
    return Capability(
        predicate_pushdown=settings.on(engine, PREDICATE),
        join_pushdown=settings.on(engine, JOIN),
        aggregate_pushdown=settings.on(engine, AGGREGATE),
        key_range=settings.on(engine, KEY_RANGE),
    )
