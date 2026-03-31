# Copyright (c) 2025 Kenneth Stott
# Canary: a1f2052b-6d0c-4f11-ac0a-a427d9968419
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Trino dynamic catalog management via SQL CREATE/DROP CATALOG."""

import re

import trino

from provisa.core.models import Source

_VALID_IDENTIFIER = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")


def _validate_identifier(name: str) -> str:
    if not _VALID_IDENTIFIER.match(name):
        raise ValueError(f"Invalid Trino identifier: {name!r}")
    return name


def _escape_sql_string(value: str) -> str:
    """Escape single quotes for Trino SQL string literals."""
    return value.replace("'", "''")


def _to_catalog_name(source_id: str) -> str:
    return _validate_identifier(source_id.replace("-", "_"))


def _build_catalog_properties(source: Source, resolved_password: str) -> dict[str, str]:
    """Build Trino connector properties from a source definition."""
    props: dict[str, str] = {}
    jdbc_url = source.jdbc_url()
    if jdbc_url:
        props["connection-url"] = jdbc_url
        props["connection-user"] = source.username
        props["connection-password"] = resolved_password
    return props


def create_catalog(conn: trino.dbapi.Connection, source: Source, resolved_password: str) -> None:
    """Create a Trino dynamic catalog for a registered source."""
    catalog_name = _to_catalog_name(source.id)
    connector = _validate_identifier(source.connector)
    props = _build_catalog_properties(source, resolved_password)

    if not props:
        raise ValueError(
            f"Source type {source.type.value!r} has no JDBC connector properties; "
            f"cannot create Trino catalog for source {source.id!r}"
        )

    props_sql = ", ".join(
        f'"{k}" = \'{_escape_sql_string(v)}\'' for k, v in props.items()
    )
    sql = f"CREATE CATALOG IF NOT EXISTS {catalog_name} USING {connector} WITH ({props_sql})"
    cur = conn.cursor()
    cur.execute(sql)
    cur.fetchall()


def drop_catalog(conn: trino.dbapi.Connection, source_id: str) -> None:
    """Drop a Trino dynamic catalog."""
    catalog_name = _to_catalog_name(source_id)
    sql = f"DROP CATALOG IF EXISTS {catalog_name}"
    cur = conn.cursor()
    cur.execute(sql)
    cur.fetchall()


def catalog_exists(conn: trino.dbapi.Connection, source_id: str) -> bool:
    """Check if a Trino catalog exists."""
    catalog_name = _to_catalog_name(source_id)
    cur = conn.cursor()
    cur.execute("SHOW CATALOGS")
    catalogs = [row[0] for row in cur.fetchall()]
    return catalog_name in catalogs
