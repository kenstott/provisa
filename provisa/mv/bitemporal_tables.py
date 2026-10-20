# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Reading a bitemporal table that keeps every version as a row (REQ-2109).

A table maps three values to its columns: ``business_key``, ``valid_time`` and
``recorded_time``. When its business key is not its primary key, a correction is a new row with
the same key, so the table already holds every version and nothing is captured: a read of it is
a filter over it, put in at the semantic stage (:func:`provisa.mv.view_read.semantic_statement`)
where every surface's statement passes, so governance judges the table it reads and a statement
over one source still runs whole at that source.

* A plain read answers the latest version of each business key: the one recorded last.
* ``as_of`` X (a point) answers, of the versions whose valid time is at or before X, the latest
  of each key; a range ``[start, end)`` answers every version whose valid time falls in it.
* ``as_recorded`` Y leaves out every version recorded after Y first: what was known at Y.

A table whose business key is its primary key is updated in place; its versions are a log Provisa
keeps (REQ-2109, captured), not rows of the table, and it is not read here.
"""

# Requirements: REQ-2109

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from provisa.compiler.stage2 import GovernanceContext

#: The version number a read ranks each key's versions by; never a column of the answer.
_VERSION = "_provisa_version"


@dataclass(frozen=True)
class VersionedTable:
    """A table whose rows are its versions: what identifies a record, and its two times."""

    table_id: int
    business_key: tuple[str, ...]
    valid_time: str
    recorded_time: str


def versioned_tables(state: Any) -> dict[int, VersionedTable]:
    """Every registered table (not a view) mapping the three values whose business key is not its
    primary key, by table id."""
    out: dict[int, VersionedTable] = {}
    for table in getattr(state, "tables", None) or []:
        key = table.get("business_key") or []
        if not key or table.get("view_sql"):
            continue
        primary = {c["column_name"] for c in table.get("columns") or [] if c.get("is_primary_key")}
        if primary == set(key):
            continue  # updated in place: its versions are a captured log
        out[table["id"]] = VersionedTable(
            table_id=table["id"],
            business_key=tuple(key),
            valid_time=table["valid_time"],
            recorded_time=table["recorded_time"],
        )
    return out


def _q(ident: str) -> str:
    return '"' + ident.replace('"', '""') + '"'


def version_read_sql(ref: str, table: VersionedTable, columns: list[str], as_of: str | None) -> str:
    """The read of ``ref`` (the table's semantic reference) answering ``columns``, under the
    request's time travel ``as_of`` (``AsOf.key``; None: current)."""
    from provisa.mv.bitemporal import AsOf

    point = AsOf.from_key(as_of) if as_of else None
    valid, recorded = _q(table.valid_time), _q(table.recorded_time)
    where: list[str] = []
    if point is not None and point.recorded_sql is not None:
        where.append(f"{recorded} <= {point.recorded_sql}")
    if point is not None and point.is_range:
        where.append(f"{valid} >= {point.start_sql} AND {valid} < {point.end_sql}")
    elif point is not None and point.start_sql is not None:
        where.append(f"{valid} <= {point.start_sql}")
    cols = ", ".join(_q(c) for c in columns)
    filtered = f"SELECT {cols} FROM {ref}" + (f" WHERE {' AND '.join(where)}" if where else "")
    if point is not None and point.is_range:
        return filtered  # every version in the range
    key = ", ".join(_q(c) for c in table.business_key)
    ranked = (
        f"SELECT {cols}, ROW_NUMBER() OVER (PARTITION BY {key} ORDER BY {recorded} DESC) "
        f"AS {_q(_VERSION)} FROM ({filtered}) AS {_q('_provisa_versions')}"
    )
    return f"SELECT {cols} FROM ({ranked}) AS {_q('_provisa_ranked')} WHERE {_q(_VERSION)} = 1"


def read_versions(sql: str, state: Any, gov: "GovernanceContext", as_of: str | None) -> str:
    """``sql`` with every reference to a versioned table replaced by its read (one pass: the read
    names the table itself, which stays the registered table governance judges)."""
    tables = versioned_tables(state)
    if not tables:
        return sql
    import sqlglot  # noqa: PLC0415
    from sqlglot import exp  # noqa: PLC0415

    from provisa.compiler.naming import table_id_of  # noqa: PLC0415

    tree = sqlglot.parse_one(sql, read="postgres")
    found = [
        (node, tables[tid])
        for node in tree.find_all(exp.Table)
        if (tid := table_id_of(node, gov.table_map)) in tables
    ]
    if not found:
        return sql
    for node, table in found:
        columns = _readable_columns(table, gov)
        alias = node.alias or node.name
        bare = node.copy()
        bare.set("alias", None)
        read = sqlglot.parse_one(
            version_read_sql(bare.sql(dialect="postgres"), table, columns, as_of), read="postgres"
        )
        node.replace(exp.Subquery(this=read, alias=exp.TableAlias(this=exp.to_identifier(alias))))
    return tree.sql(dialect="postgres")


def _readable_columns(table: VersionedTable, gov: "GovernanceContext") -> list[str]:
    """The table's columns this reader may read: the read answers no column governance would
    refuse it. Its business key and two times must be among them, or the read cannot be drawn."""
    names = [name for name, _ in gov.all_columns.get(table.table_id, [])]
    visible = gov.visible_columns.get(table.table_id)
    columns = names if visible is None else [n for n in names if n in visible]
    needed = [*table.business_key, table.valid_time, table.recorded_time]
    hidden = [c for c in needed if c not in columns]
    if hidden:
        raise PermissionError(
            f"a versioned table's read needs its business key and times, and this role does not "
            f"see {', '.join(hidden)}"
        )
    return columns
