# Copyright (c) 2026 Kenneth Stott
# Canary: 7d2e9b41-6c3a-4f18-a5d7-3b8e0c62f9a1
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Snowflake-native materialization store: the landing terminal a self-only Snowflake engine writes
its replicas through (REQ-1637, REQ-1653), and the keys those replicas carry (REQ-1652).

Layout (what the account holds, and what the compiler's physical names resolve to):

* a source's rows LAND in the landing database's store schema, under the same mangled name every
  backend's ``landing_target`` produces -- ``"<landing db>"."mat"."<source>__<schema>__<table>"``;
* the compiler's physical name for the source -- ``"<catalog(source id)>"."<schema>"."<table>"`` --
  is a SECURE VIEW over that replica, so governed SQL resolves natively and the replica is one
  object however many environments read it.

Keys are part of the replica's shape, not of any catalog publish: the landed table gets its
declared PRIMARY KEY and every relationship between landed tables its FOREIGN KEY (Snowflake keeps
both as informational metadata, which is what Horizon Catalog renders as keys and join paths), and
because a Snowflake view can hold no constraint, the per-source view mirrors them as column TAGs.

Every function here takes a DBAPI cursor and is synchronous; the runtime runs them off the event
loop. Pure DDL builders take no cursor at all so their shape is testable without an account.
"""

# Requirements: REQ-1637, REQ-1651, REQ-1652, REQ-1653, REQ-1654, REQ-1655, REQ-965

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from provisa.core.ir_types import to_ir

if TYPE_CHECKING:
    from provisa.federation.landed_keys import ForeignKeyEdge

log = logging.getLogger(__name__)

Parts = tuple[str, str, str]

# IR type -> Snowflake column type. A landed replica carries the source's own values, so integers
# and decimals map to their widest safe Snowflake type. JSON lands as VARIANT.
_IR_TO_SNOWFLAKE: dict[str, str] = {
    "smallint": "NUMBER(38,0)",
    "integer": "NUMBER(38,0)",
    "bigint": "NUMBER(38,0)",
    "text": "VARCHAR",
    "boolean": "BOOLEAN",
    "float": "FLOAT",
    "double": "FLOAT",
    "numeric": "NUMBER(38,9)",
    "date": "DATE",
    "timestamp": "TIMESTAMP_NTZ",
    "time": "TIME",
    "uuid": "VARCHAR",
    "bytea": "BINARY",
    "json": "VARIANT",
}

TAGS_SCHEMA = "PROVISA_GOVERNANCE"
PK_TAG = "PRIMARY_KEY"
FK_TAG = "FOREIGN_KEY"
OWNED_FK_PREFIX = "provisa_fk_"
OWNED_PK_PREFIX = "provisa_pk_"

_VALID_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_$]*$")


def identifier(raw: str) -> str:
    """A constraint or tag name Snowflake accepts unquoted: hyphens become underscores; anything
    else outside the identifier alphabet is a caller error, never silently mangled."""
    name = raw.replace("-", "_")
    if not _VALID_IDENTIFIER.match(name):
        raise ValueError(f"invalid Snowflake identifier: {raw!r}")
    return name


def _escape(value: str) -> str:
    return value.replace("'", "''")


def ddl_type(ir_type: str) -> str:
    """Snowflake column type for a canonical IR type name -- raises on an unknown type (never a
    silent widen), mirroring the store-DDL discipline of the other engines."""
    canonical = to_ir(ir_type)
    sql_type = _IR_TO_SNOWFLAKE.get(canonical)
    if sql_type is None:
        raise ValueError(
            f"no Snowflake type mapping for IR type {ir_type!r} (canonical {canonical!r})"
        )
    return sql_type


def qualified(parts: Parts) -> str:
    return ".".join(f'"{part}"' for part in parts)


def replica_parts(
    landing_database: str, store_schema: str, source_id: str, schema: str, table: str
) -> Parts:
    """Where a source table's rows land: the store schema of the landing database, under the name
    every backend's ``landing_target`` mangles from the source id and the physical schema/table."""
    return landing_database, store_schema, f"{source_id}__{schema}__{table}"


# -- replica DDL / DML ---------------------------------------------------------------------------


def ensure_namespace(cur: Any, database: str, schema: str) -> None:
    cur.execute(f'CREATE DATABASE IF NOT EXISTS "{database}"')
    cur.execute(f'CREATE SCHEMA IF NOT EXISTS "{database}"."{schema}"')


def existing_columns(cur: Any, parts: Parts) -> list[str]:
    """The table's current column names in position order, ``[]`` if it does not exist."""
    database, schema, table = parts
    cur.execute(
        f'SELECT column_name FROM "{database}".information_schema.columns '
        f"WHERE table_schema = '{_escape(schema)}' AND table_name = '{_escape(table)}' "
        "ORDER BY ordinal_position"
    )
    return [str(r[0]) for r in cur.fetchall()]


def existing_primary_key(cur: Any, parts: Parts) -> tuple[str, ...]:
    """The table's PRIMARY KEY columns in key order, ``()`` when it has none."""
    cur.execute(f"SHOW PRIMARY KEYS IN TABLE {qualified(parts)}")
    cols = [d[0] for d in cur.description]
    rows = [dict(zip(cols, r, strict=False)) for r in cur.fetchall()]
    return tuple(str(r["column_name"]) for r in sorted(rows, key=lambda r: int(r["key_sequence"])))


def existing_foreign_keys(cur: Any, parts: Parts) -> frozenset[str]:
    """The names of the table's FOREIGN KEY constraints."""
    cur.execute(f"SHOW IMPORTED KEYS IN TABLE {qualified(parts)}")
    cols = [d[0] for d in cur.description]
    return frozenset(str(dict(zip(cols, r, strict=False))["fk_name"]) for r in cur.fetchall())


@dataclass
class ObjectTags:
    """The PROVISA_GOVERNANCE tags currently on one object: ``table`` = tag -> value on the object
    itself, ``columns`` = column -> tag -> value."""

    table: dict[str, str]
    columns: dict[str, dict[str, str]]


def existing_tags(cur: Any, tags_database: str, parts: Parts) -> ObjectTags:
    """Every PROVISA_GOVERNANCE tag set on the object and its columns (one read per object)."""
    cur.execute(
        f'SELECT level, column_name, tag_name, tag_value FROM TABLE("{tags_database}"'
        f".information_schema.tag_references_all_columns('{_escape(qualified(parts))}', 'table')) "
        f"WHERE tag_database = '{_escape(tags_database)}' AND tag_schema = '{TAGS_SCHEMA}'"
    )
    out = ObjectTags(table={}, columns={})
    for level, column, tag, value in cur.fetchall():
        if str(level).upper() == "COLUMN" and column is not None:
            out.columns.setdefault(str(column), {})[str(tag)] = str(value or "")
        else:
            out.table[str(tag)] = str(value or "")
    return out


def object_exists(cur: Any, parts: Parts) -> bool:
    return bool(existing_columns(cur, parts))


def create_ddl(
    parts: Parts, columns: list[tuple[str, str]], pk_columns: tuple[str, ...] = ()
) -> str:
    cols = ", ".join(f'"{name}" {ddl_type(ir_type)}' for name, ir_type in columns)
    pk = ""
    if pk_columns:
        pk = (
            f', CONSTRAINT "{identifier(OWNED_PK_PREFIX + parts[2])}" PRIMARY KEY '
            f"({', '.join(f'"{c}"' for c in pk_columns)})"
        )
    return f"CREATE TABLE IF NOT EXISTS {qualified(parts)} ({cols}{pk})"


def reconcile_snowflake_native(
    cur: Any,
    *,
    parts: Parts,
    columns: list[tuple[str, str]],
    pk_columns: list[str] | None = None,
) -> str:
    """Converge the landed replica to ``columns`` + ``pk_columns`` (DDL only, no data). Returns
    ``created`` | ``kept`` | ``recreated``: an existing matching table survives a restart; a drifted
    one (column set/order OR primary key, REQ-1651) is recreated and its data re-lands on the next
    refresh. A dropped replica takes its foreign keys with it; the key reconcile restores them."""
    database, schema, _ = parts
    ensure_namespace(cur, database, schema)
    want = [name for name, _ in columns]
    pk = tuple(pk_columns or ())
    have = existing_columns(cur, parts)
    if not have:
        cur.execute(create_ddl(parts, columns, pk))
        return "created"
    if have == want and existing_primary_key(cur, parts) == pk:
        return "kept"
    cur.execute(f"DROP TABLE IF EXISTS {qualified(parts)}")
    cur.execute(create_ddl(parts, columns, pk))
    return "recreated"


def _bind_value(value: Any) -> Any:
    import datetime
    import decimal
    import json

    if value is None or isinstance(value, (str, int, float, bool, decimal.Decimal, bytes)):
        return value
    if isinstance(value, (datetime.date, datetime.datetime, datetime.time)):
        return value
    if isinstance(value, (dict, list)):
        return json.dumps(value, default=str)
    return str(value)


def land_snowflake_native(
    cur: Any,
    *,
    parts: Parts,
    columns: list[tuple[str, str]],
    rows: list[dict],
    shape: str,
    pk_columns: list[str] | None = None,
) -> str:
    """Land ``rows`` into the replica at ``parts``: REPLACE deletes then inserts (a full refresh of
    the CONTENTS, never of the table -- its DDL, keys and grants are the reconcile's), APPEND only
    inserts. CDC is not a landing shape this store implements: refusing loudly here beats a replica
    that silently drifts from its source."""
    if shape == "cdc":
        raise NotImplementedError(
            "Snowflake native landing has no CDC shape; use replace or append"
        )
    if shape not in ("replace", "append"):
        raise ValueError(f"unknown landing shape {shape!r}")
    fq = qualified(parts)
    if shape == "replace":
        cur.execute(f"DELETE FROM {fq}")
    if rows:
        names = [name for name, _ in columns]
        json_cols = {name for name, ir_type in columns if to_ir(ir_type) == "json"}
        placeholders = ", ".join("PARSE_JSON(%s)" if name in json_cols else "%s" for name in names)
        sql = f"INSERT INTO {fq} ({', '.join(f'"{n}"' for n in names)}) SELECT {placeholders}"
        cur.executemany(sql, [[_bind_value(r.get(n)) for n in names] for r in rows])
    del pk_columns  # identity is the reconcile's; a batch land needs none
    return fq


def expose_view(cur: Any, *, view: Parts, replica: Parts, replace: bool) -> None:
    """The per-source SECURE VIEW at the compiler's physical name over the replica. Created when
    absent; REPLACED only when the replica's shape changed, because CREATE OR REPLACE drops the
    view's column tags and comments (the key reconcile re-sets the key tags right after)."""
    database, schema, _ = view
    ensure_namespace(cur, database, schema)
    verb = "CREATE OR REPLACE SECURE VIEW" if replace else "CREATE SECURE VIEW IF NOT EXISTS"
    cur.execute(f"{verb} {qualified(view)} AS SELECT * FROM {qualified(replica)}")


# -- keys (REQ-1652) -----------------------------------------------------------------------------


@dataclass(frozen=True)
class KeyTarget:
    """One landed table's addresses: the replica that carries constraints, and the view (if the
    source has one) that mirrors them as tags -- plus the descriptions both carry as COMMENTs."""

    replica: Parts
    view: Parts | None
    primary_key: tuple[str, ...]
    description: str = ""
    column_descriptions: dict[str, str] | None = None
    # Steward classifications (REQ-1655), ``(tag id, value)`` on the object and per column.
    tags: tuple[tuple[str, str], ...] = ()
    column_tags: dict[str, tuple[tuple[str, str], ...]] | None = None


# -- descriptions (REQ-1654) ---------------------------------------------------------------------


def existing_comments(cur: Any, parts: Parts) -> tuple[str, dict[str, str]]:
    """The object's current COMMENT and its columns' COMMENTs, from information_schema (a view is
    a row of ``tables`` too)."""
    database, schema, table = parts
    cur.execute(
        f'SELECT comment FROM "{database}".information_schema.tables '
        f"WHERE table_schema = '{_escape(schema)}' AND table_name = '{_escape(table)}'"
    )
    rows = cur.fetchall()
    table_comment = str(rows[0][0] or "") if rows else ""
    cur.execute(
        f'SELECT column_name, comment FROM "{database}".information_schema.columns '
        f"WHERE table_schema = '{_escape(schema)}' AND table_name = '{_escape(table)}'"
    )
    return table_comment, {str(c): str(comment or "") for c, comment in cur.fetchall()}


def comment_statements(
    targets: dict[str, KeyTarget],
    existing: dict[Parts, tuple[str, dict[str, str]]],
) -> list[str]:
    """COMMENT DDL carrying the model's table and column descriptions onto every replica and its
    backing view. A description is written when the object's current comment does not already
    begin with it -- so a comment another writer extended (the catalog export appends governance
    facts to the same comment) is left standing, and an empty description never erases anything."""

    def needs(current: str, description: str) -> bool:
        return bool(description) and not current.startswith(description)

    stmts: list[str] = []
    for identity in sorted(targets):
        target = targets[identity]
        for parts, kind in ((target.replica, "TABLE"), (target.view, "VIEW")):
            if parts is None:
                continue
            current_table, current_columns = existing.get(parts, ("", {}))
            fq = f"ALTER {kind} {qualified(parts)}"
            if needs(current_table, target.description):
                stmts.append(f"{fq} SET COMMENT = '{_escape(target.description)}'")
            column_verb = "ALTER COLUMN" if kind == "VIEW" else "MODIFY COLUMN"
            for column, description in sorted((target.column_descriptions or {}).items()):
                if needs(current_columns.get(column, ""), description):
                    stmts.append(
                        f"{fq} {column_verb} \"{column}\" COMMENT '{_escape(description)}'"
                    )
    return stmts


def constraint_statements(
    targets: dict[str, KeyTarget],
    edges: list[ForeignKeyEdge],
    existing_primary_keys: dict[Parts, tuple[str, ...]],
    existing_foreign_keys: dict[Parts, frozenset[str]],
) -> list[str]:
    """PRIMARY KEY and FOREIGN KEY DDL on the replicas. ``targets`` is keyed by the landed table's
    identity (``source_id.schema.table``); ``edges`` are already validated against it (both ends
    landed, referenced columns = the referenced key). Idempotent: a matching PRIMARY KEY is left
    alone, a differing one replaced (a table holds at most one); a FOREIGN KEY of the same name is
    skipped; a ``provisa_fk_*`` key nothing declares any more is withdrawn -- keys of any other
    origin are never touched."""
    stmts: list[str] = []
    for identity in sorted(targets):
        target = targets[identity]
        if not target.primary_key:
            continue
        current = existing_primary_keys.get(target.replica, ())
        if current == target.primary_key:
            continue
        fq = qualified(target.replica)
        if current:
            stmts.append(f"ALTER TABLE {fq} DROP PRIMARY KEY")
        cols = ", ".join(f'"{c}"' for c in target.primary_key)
        stmts.append(
            f'ALTER TABLE {fq} ADD CONSTRAINT "{identifier(OWNED_PK_PREFIX + target.replica[2])}" '
            f"PRIMARY KEY ({cols})"
        )
    wanted: dict[Parts, set[str]] = {}
    for edge in edges:
        holder = targets[edge.holder].replica
        referenced = targets[edge.referenced].replica
        wanted.setdefault(holder, set()).add(edge.name)
        if edge.name in existing_foreign_keys.get(holder, frozenset()):
            continue
        cols = ", ".join(f'"{c}"' for c in edge.holder_columns)
        ref_cols = ", ".join(f'"{c}"' for c in edge.referenced_columns)
        stmts.append(
            f'ALTER TABLE {qualified(holder)} ADD CONSTRAINT "{edge.name}" FOREIGN KEY ({cols}) '
            f"REFERENCES {qualified(referenced)} ({ref_cols})"
        )
    for holder, names in existing_foreign_keys.items():
        for name in sorted(names):
            if name.startswith(OWNED_FK_PREFIX) and name not in wanted.get(holder, set()):
                stmts.append(f'ALTER TABLE {qualified(holder)} DROP CONSTRAINT "{name}"')
    return stmts


def key_tag_statements(
    tags_database: str,
    targets: dict[str, KeyTarget],
    edges: list[ForeignKeyEdge],
    existing: dict[Parts, ObjectTags],
) -> list[str]:
    """Key TAGs on the per-source views: ``PRIMARY_KEY`` = the column's 1-based position in the
    key, ``FOREIGN_KEY`` = the referenced physical ``"db"."schema"."table"."column"`` (several,
    ``; ``-joined, when one column holds more than one). A key tag an earlier reconcile set that
    this one does not re-set is unset. Nothing is emitted -- not even the tag objects -- when no
    view carries a key."""

    def tag_fq(name: str) -> str:
        return f'"{tags_database}"."{TAGS_SCHEMA}"."{name}"'

    def col_tag(view: Parts, column: str, tag: str, value: str) -> str:
        return (
            f'ALTER VIEW {qualified(view)} MODIFY COLUMN "{column}" '
            f"SET TAG {tag_fq(tag)} = '{_escape(value)}'"
        )

    stmts: list[str] = []
    kept: set[tuple[Parts, str, str]] = set()
    for identity in sorted(targets):
        target = targets[identity]
        if target.view is None:
            continue
        for position, column in enumerate(target.primary_key, start=1):
            stmts.append(col_tag(target.view, column, PK_TAG, str(position)))
            kept.add((target.view, column, PK_TAG))
    references: dict[tuple[Parts, str], list[str]] = {}
    for edge in edges:
        view = targets[edge.holder].view
        if view is None:
            continue
        referenced_view = targets[edge.referenced].view or targets[edge.referenced].replica
        for column, ref_column in zip(edge.holder_columns, edge.referenced_columns, strict=True):
            references.setdefault((view, column), []).append(
                ".".join(f'"{part}"' for part in (*referenced_view, ref_column))
            )
    for (view, column), refs in references.items():
        stmts.append(col_tag(view, column, FK_TAG, "; ".join(refs)))
        kept.add((view, column, FK_TAG))
    for view in sorted(existing):
        for column, tags in sorted(existing[view].columns.items()):
            for tag in sorted(tags):
                if tag in (PK_TAG, FK_TAG) and (view, column, tag) not in kept:
                    stmts.append(
                        f'ALTER VIEW {qualified(view)} MODIFY COLUMN "{column}" UNSET TAG {tag_fq(tag)}'
                    )
    if not stmts:
        return []
    return [
        f'CREATE SCHEMA IF NOT EXISTS "{tags_database}"."{TAGS_SCHEMA}"',
        f"CREATE TAG IF NOT EXISTS {tag_fq(PK_TAG)}",
        f"CREATE TAG IF NOT EXISTS {tag_fq(FK_TAG)}",
        *stmts,
    ]


def model_tag_statements(
    tags_database: str,
    targets: dict[str, KeyTarget],
    existing: dict[Parts, ObjectTags],
    known_tags: frozenset[str],
) -> list[str]:
    """The stewards' classifications as native TAGs on every replica and its backing view
    (REQ-1655): ``PROVISA_GOVERNANCE.<TAG_ID>`` = the assignment's reason (else the tag id), on the
    object and on each tagged column. A tag the model defines (``known_tags``) that sits on an
    object but is no longer assigned there is unset; a tag of any other origin is never touched.
    The key tags are the key reconcile's, not this one's."""

    def tag_fq(name: str) -> str:
        return f'"{tags_database}"."{TAGS_SCHEMA}"."{name}"'

    known = {identifier(t.upper()) for t in known_tags} - {PK_TAG, FK_TAG}
    stmts: list[str] = []
    used: set[str] = set()
    for identity_ in sorted(targets):
        target = targets[identity_]
        wanted_table = {identifier(tag.upper()): value for tag, value in target.tags}
        wanted_columns = {
            column: {identifier(tag.upper()): value for tag, value in tags}
            for column, tags in (target.column_tags or {}).items()
        }
        for parts, kind in ((target.replica, "TABLE"), (target.view, "VIEW")):
            if parts is None:
                continue
            current = existing.get(parts, ObjectTags(table={}, columns={}))
            fq = f"ALTER {kind} {qualified(parts)}"
            for tag, value in sorted(wanted_table.items()):
                used.add(tag)
                if current.table.get(tag) != value:
                    stmts.append(f"{fq} SET TAG {tag_fq(tag)} = '{_escape(value)}'")
            for tag in sorted((set(current.table) & known) - set(wanted_table)):
                stmts.append(f"{fq} UNSET TAG {tag_fq(tag)}")
            column_verb = "ALTER COLUMN" if kind == "VIEW" else "MODIFY COLUMN"
            for column in sorted(set(wanted_columns) | set(current.columns)):
                wanted = wanted_columns.get(column, {})
                have = current.columns.get(column, {})
                for tag, value in sorted(wanted.items()):
                    used.add(tag)
                    if have.get(tag) != value:
                        stmts.append(
                            f'{fq} {column_verb} "{column}" SET TAG {tag_fq(tag)} = '
                            f"'{_escape(value)}'"
                        )
                for tag in sorted((set(have) & known) - set(wanted)):
                    stmts.append(f'{fq} {column_verb} "{column}" UNSET TAG {tag_fq(tag)}')
    if not stmts:
        return []
    return [
        f'CREATE SCHEMA IF NOT EXISTS "{tags_database}"."{TAGS_SCHEMA}"',
        *(f"CREATE TAG IF NOT EXISTS {tag_fq(tag)}" for tag in sorted(used)),
        *stmts,
    ]


def merge_snowflake_native(
    cur: Any,
    *,
    parts: Parts,
    columns: list[tuple[str, str]],
    rows: list[dict],
    pk_columns: list[str],
) -> str:
    """UPSERT ``rows`` by ``pk_columns`` (an MV's ``persist: upsert``, REQ-965) with a MERGE from a
    bound source row -- one prepared statement per batch, never a per-row round trip."""
    if not pk_columns:
        raise ValueError(f"upsert into {qualified(parts)} requires primary key columns")
    if not rows:
        return qualified(parts)
    names = [name for name, _ in columns]
    json_cols = {name for name, ir_type in columns if to_ir(ir_type) == "json"}
    src_cols = ", ".join(f'"{n}"' for n in names)
    on = " AND ".join(f't."{k}" = s."{k}"' for k in pk_columns)
    update = ", ".join(f't."{n}" = s."{n}"' for n in names if n not in pk_columns)
    insert_vals = ", ".join(f's."{n}"' for n in names)
    placeholders = ", ".join("PARSE_JSON(%s)" if n in json_cols else "%s" for n in names)
    sql = (
        f"MERGE INTO {qualified(parts)} t USING (SELECT {placeholders}) s ({src_cols}) ON {on} "
        + (f"WHEN MATCHED THEN UPDATE SET {update} " if update else "")
        + f"WHEN NOT MATCHED THEN INSERT ({src_cols}) VALUES ({insert_vals})"
    )
    cur.executemany(sql, [[_bind_value(r.get(n)) for n in names] for r in rows])
    return qualified(parts)


def reconcile_metadata_native(
    cur: Any,
    *,
    tags_database: str,
    targets: dict[str, KeyTarget],
    edges: list[ForeignKeyEdge],
    known_tags: frozenset[str] = frozenset(),
) -> int:
    """Read each object's current keys, tags and comments, then apply exactly the statements that
    change something (REQ-1652, REQ-1654, REQ-1655). An object that does not exist yet (an MV whose
    first refresh has not run) is left for the converge that creates it; the edges touching it wait
    with it. Returns the number of statements run."""
    present = {ident: t for ident, t in targets.items() if object_exists(cur, t.replica)}
    edges = [e for e in edges if e.holder in present and e.referenced in present]
    pks: dict[Parts, tuple[str, ...]] = {}
    fks: dict[Parts, frozenset[str]] = {}
    tags: dict[Parts, ObjectTags] = {}
    comments: dict[Parts, tuple[str, dict[str, str]]] = {}
    for target in present.values():
        if target.replica not in pks:
            pks[target.replica] = existing_primary_key(cur, target.replica)
            fks[target.replica] = existing_foreign_keys(cur, target.replica)
            tags[target.replica] = existing_tags(cur, tags_database, target.replica)
            comments[target.replica] = existing_comments(cur, target.replica)
        if target.view is not None and target.view not in tags and object_exists(cur, target.view):
            tags[target.view] = existing_tags(cur, tags_database, target.view)
            comments[target.view] = existing_comments(cur, target.view)
    statements = (
        constraint_statements(present, edges, pks, fks)
        + key_tag_statements(tags_database, present, edges, tags)
        + model_tag_statements(tags_database, present, tags, known_tags)
        + comment_statements(present, comments)
    )
    for stmt in statements:
        cur.execute(stmt)
    return len(statements)
