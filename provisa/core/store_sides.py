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

#: Per (org, region): what one region does.
STATE: frozenset[str] = frozenset(
    {
        # Builds into the region's own replica / MV stores (REQ-1912, REQ-1922).
        "replica_state",
        "preserved_snapshots",
        "mv_refresh_log",
        "mv_delta_ledger",
        # The record (REQ-1922: the state store and the record are per org and region).
        "query_audit_log",
        "query_sla_log",
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

TABLES: dict[str, frozenset[str]] = {
    MODEL_SIDE: _ORG_TABLES - STATE - BOTH,
    STATE_SIDE: STATE,
}

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
    """The tables ``stmt`` touches that belong to the other store than ``holds``."""
    other = STATE_SIDE if holds == MODEL_SIDE else MODEL_SIDE
    return statement_tables(stmt) & TABLES[other]
