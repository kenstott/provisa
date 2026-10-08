# Copyright (c) 2026 Kenneth Stott
# Canary: d4c49311-e982-4e76-8760-5ea40c8cc337
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Where the engine reads a session's temporary tables (REQ-615, REQ-1926, REQ-1942): the
session's own schema of the engine's store."""

# Requirements: REQ-615, REQ-1926, REQ-1942

from __future__ import annotations

from typing import Any

from provisa.compiler import temp_tables
from provisa.compiler.temp_tables import TempSession


def schema_of(session: TempSession) -> str:
    from provisa.core.request_context import current_env, require_current_org
    from provisa.federation.replica_address import temp_schema

    return temp_schema(require_current_org(), current_env.get(), session.id)


def quoted(*parts: str | None) -> str:
    return ".".join('"' + p.replace('"', '""') + '"' for p in parts if p)


def address(pg_sql: str, state: Any) -> str:
    """``pg_sql`` with each of the session's temporary tables named at its address in the
    engine's store. A statement that names none is returned as written."""
    session = temp_tables.current()
    if session is None or not session.tables:
        return pg_sql
    import sqlglot
    import sqlglot.expressions as exp

    tree = sqlglot.parse_one(pg_sql, read="postgres")
    named = [t for t in tree.find_all(exp.Table) if not t.db and t.name in session.tables]
    if not named:
        return pg_sql
    catalog = state.federation_engine.engine.backend.replica_read_catalog(state)
    schema = schema_of(session)
    for t in named:
        alias = t.alias or t.name  # its columns are still named by the table's own name
        t.set("db", exp.to_identifier(schema, quoted=True))
        if catalog:
            t.set("catalog", exp.to_identifier(catalog, quoted=True))
        t.set("alias", exp.TableAlias(this=exp.to_identifier(alias, quoted=True)))
    return tree.sql(dialect="postgres")
