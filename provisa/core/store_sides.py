# Copyright (c) 2026 Kenneth Stott
# Canary: 11afa38e-0717-473c-9c09-5827711dbc07
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Which store an org table belongs to: the MODEL store or the STATE store (REQ-1919, REQ-1920,
REQ-1922).

An org's model, and everything that describes the org as a whole, is kept once, in the model
store, shared by every region the org selects. What one region does — its replica and MV
builds, its serving, its request record — is kept per region, in that region's state store
(:data:`STATE`). ``model_db`` holds the first and ``tenant_db`` the second; each refuses a
statement that touches the other's tables (:func:`foreign_tables`), so a statement that would
have to cross between two databases in a region deployment fails in every deployment, naming the
table.

This is not ``env_classes``' split. NEVER_RUNTIME means "never copied to another environment";
some of those tables still describe the org as a whole and so sit in the model store: the graph's
stable ids (one entity has one id in every region), the catalog bindings, the admin trail.
"""

# Requirements: REQ-1919, REQ-1920, REQ-1922

from __future__ import annotations

import functools
import re
from typing import Any

from provisa.core.env_classes import (
    CARRIED,
    IDENTITY_ONLY,
    NEVER_RUNTIME,
    NEVER_SENSITIVE,
    PARTIAL,
)

MODEL_SIDE = "model"
STATE_SIDE = "state"
RECORD_SIDE = "record"
PLATFORM_STATE_SIDE = "platform_state"
PLATFORM_ADMIN_SIDE = "platform_admin"

#: The handle that holds each side: the org's three (``OrgRuntime``) and the deployment's one
#: (``AppState.platform_state_db``).
HANDLE = {
    MODEL_SIDE: "model_db",
    STATE_SIDE: "tenant_db",
    RECORD_SIDE: "record_db",
    PLATFORM_STATE_SIDE: "platform_state_db",
    PLATFORM_ADMIN_SIDE: "admin_db",
}

#: Per (org, region): the request record (REQ-1922: the state store and the record are per org
#: and region; a region names the store its record is kept in, which may be its state store).
RECORD: frozenset[str] = frozenset({"query_audit_log", "query_sla_log"})

#: Per (org, region): what one region does.
STATE: frozenset[str] = frozenset(
    {
        # Builds into the region's own replica / MV stores (REQ-1912, REQ-1922).
        "replica_state",
        "preserved_snapshots",
        "mv_build_state",
        "mv_refresh_log",
        "mv_delta_ledger",
        # The region's serving: change events and their delivery, live queries, freshness, and
        # what this region's nodes read of its sources.
        "events",
        "event_status",
        "live_query_state",
        "node_freshness_state",
        "source_catalog_cache",
        "file_source_mtimes",
    }
)

#: In both stores, each with its own rows: ``config_stamp``'s ``model`` and ``settings`` rows are
#: advanced by trigger in the model store's transaction, its ``replica`` row with replica_state.
BOTH: frozenset[str] = frozenset({"config_stamp"})

_ORG_TABLES = CARRIED | IDENTITY_ONLY | NEVER_SENSITIVE | NEVER_RUNTIME | PARTIAL

#: The deployment's own operating state, in the platform database (``provisa/core/platform_state``).
#: Not an org table: no org handle reaches it, and its handle reaches no org table.
from provisa.core.platform_state import TABLES as PLATFORM_STATE  # noqa: E402

#: The platform's registry (orgs, users, invites, environments, billing, deployment settings), in
#: the platform database (``state.admin_db``): no org handle reaches it, and its handle reaches no
#: org table and none of the platform's operating state. ``config_stamp`` is in both planes too.
from provisa.core.schema_admin import metadata as _platform_metadata  # noqa: E402

PLATFORM_ADMIN: frozenset[str] = frozenset(_platform_metadata.tables) - PLATFORM_STATE - BOTH

TABLES: dict[str, frozenset[str]] = {
    MODEL_SIDE: _ORG_TABLES - STATE - RECORD - BOTH,
    STATE_SIDE: STATE,
    RECORD_SIDE: RECORD,
    PLATFORM_STATE_SIDE: PLATFORM_STATE,
    PLATFORM_ADMIN_SIDE: PLATFORM_ADMIN,
}


def side_of(table: str) -> str:
    """The side an org table is kept on (one of :data:`TABLES`' keys)."""
    for side, tables in TABLES.items():
        if table in tables:
            return side
    raise KeyError(f"{table!r} is kept in more than one store or is no org table")


# A table a raw statement names: after FROM / JOIN / INTO / UPDATE / TABLE, optionally
# schema-qualified and quoted. Over-matching (a column called "from") only names an identifier
# that is no org table, which is ignored.
_RAW_TABLE = re.compile(
    r"\b(?:FROM|JOIN|INTO|UPDATE|TABLE)\s+(?:ONLY\s+)?(?:\"?[\w$]+\"?\s*\.\s*)?\"?([\w$]+)\"?",
    re.IGNORECASE,
)


# Only a statement that reads or writes rows is checked. Schema statements (CREATE, ALTER, a DO
# block, a GRANT) lay out a store's tables; a schema is created in each store whole, and naming
# every table is what they are for.
_DML = re.compile(
    r"^\s*(?:--[^\n]*\n\s*)*(SELECT|INSERT|UPDATE|DELETE|WITH|MERGE)\b", re.IGNORECASE
)


@functools.lru_cache(maxsize=4096)
def _raw_tables(sql: str) -> frozenset[str]:
    if _DML.match(sql) is None:
        return frozenset()
    return frozenset(m.group(1).lower() for m in _RAW_TABLE.finditer(sql))


def statement_tables(stmt: Any) -> frozenset[str]:
    """The table names a statement that reads or writes rows touches: a SQLAlchemy Core
    statement's, or a raw text's. A schema statement touches none."""
    import sqlalchemy as sa
    from sqlalchemy.sql.dml import UpdateBase
    from sqlalchemy.sql.selectable import Selectable
    from sqlalchemy.sql.util import find_tables

    if isinstance(stmt, sa.TextClause):
        return _raw_tables(stmt.text)
    if isinstance(stmt, str):
        return _raw_tables(stmt)
    if not isinstance(stmt, (Selectable, UpdateBase)):
        return frozenset()
    return frozenset(
        str(t.name).lower()
        for t in find_tables(stmt, include_joins=True, include_aliases=True, include_crud=True)
        if isinstance(t, sa.Table)
    )


def foreign_tables(holds: str, stmt: Any) -> frozenset[str]:
    """The tables ``stmt`` touches that another store than ``holds`` keeps."""
    touched = statement_tables(stmt)
    return frozenset().union(
        *(touched & tables for side, tables in TABLES.items() if side != holds)
    )
