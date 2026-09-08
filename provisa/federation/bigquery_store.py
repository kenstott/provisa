# Copyright (c) 2026 Kenneth Stott
# Canary: 9b4e2d17-6c3a-4f58-a1d9-0e7c5b83f2a6
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""BigQuery landing store: the DDL-only converge of a landed table (REQ-1658) and the landed model's
metadata on it -- keys (REQ-1652), descriptions (REQ-1654) and tags (REQ-1655).

BigQuery holds ``NOT ENFORCED`` PRIMARY/FOREIGN KEY constraints, a description on the table and on
each column, and key/value labels on the table. There is no view layer (the landed table is the
compiler's physical ``project.dataset.table``), so every target is its replica alone. Constraints
are DDL; descriptions and labels go through the client API (``Table.description``, ``labels``,
``SchemaField.description``), which reads and writes them without parsing ``INFORMATION_SCHEMA``'s
literal-quoted option values. Confirmed live: ``INFORMATION_SCHEMA.TABLE_CONSTRAINTS`` names a
PRIMARY KEY ``<table>.pk$`` and a FOREIGN KEY ``<table>.<name>``; ``DROP PRIMARY KEY`` takes the
FOREIGN KEYs referencing it with it.

Labels are BigQuery's only table-level classification: a key or value is lowercase letters, digits,
``_`` and ``-``, at most 63 characters, so a tag id and its reason are folded to that alphabet. A
column-level tag has no BigQuery counterpart short of a policy-tag taxonomy, which Provisa does not
own; column tags are reported withheld, never silently dropped.
"""

# Requirements: REQ-1658, REQ-1652, REQ-1654, REQ-1655

from __future__ import annotations

import logging
import re
from typing import TYPE_CHECKING, Any

from provisa.core.ir_types import to_ir
from provisa.federation.landed_keys import FK_PREFIX, KeyTarget, Parts

if TYPE_CHECKING:
    from provisa.federation.landed_keys import ForeignKeyEdge

log = logging.getLogger(__name__)

# Canonical IR name -> BigQuery standard-SQL type (landed-table DDL / load schema).
_IR_TO_BQ: dict[str, str] = {
    "smallint": "INT64",
    "integer": "INT64",
    "bigint": "INT64",
    "text": "STRING",
    "boolean": "BOOL",
    "float": "FLOAT64",
    "double": "FLOAT64",
    "numeric": "NUMERIC",
    "date": "DATE",
    "timestamp": "TIMESTAMP",
    "time": "TIME",
    "uuid": "STRING",
    "bytea": "BYTES",
    "json": "JSON",
}

#: Prefix of every label Provisa sets: ``provisa_governance_<tag id>``.
LABEL_PREFIX = "provisa_governance_"

_LABEL_DISALLOWED = re.compile(r"[^a-z0-9_-]")


def bq_type(ir_type: str) -> str:
    canonical = to_ir(ir_type)
    t = _IR_TO_BQ.get(canonical)
    if t is None:
        raise ValueError(
            f"no BigQuery type mapping for IR type {ir_type!r} (canonical {canonical!r})"
        )
    return t


def qualified(parts: Parts) -> str:
    return "`" + ".".join(parts) + "`"


def label_key(tag_id: str) -> str:
    """The label key for a model tag id, in BigQuery's label alphabet."""
    return (LABEL_PREFIX + _LABEL_DISALLOWED.sub("_", tag_id.lower()))[:63]


def label_value(value: str) -> str:
    return _LABEL_DISALLOWED.sub("_", value.lower())[:63]


def _pk_name(table: str) -> str:
    return f"{table}.pk$"


# -- DDL-only converge (REQ-1658) ---------------------------------------------------------------


def existing_columns(client: Any, parts: Parts) -> list[str]:
    """The table's current column names, or ``[]`` if it does not exist."""
    from google.api_core.exceptions import NotFound

    try:
        table = client.get_table(".".join(parts))
    except NotFound:
        return []
    return [f.name for f in table.schema]


def existing_primary_key(client: Any, parts: Parts) -> tuple[str, ...]:
    project, dataset, table = parts
    rows = client.query(
        f"SELECT column_name FROM `{project}.{dataset}.INFORMATION_SCHEMA.KEY_COLUMN_USAGE` "
        f"WHERE table_name = @t AND constraint_name = @c ORDER BY ordinal_position",
        job_config=_params(t=table, c=_pk_name(table)),
    ).result()
    return tuple(str(r["column_name"]) for r in rows)


def existing_foreign_keys(client: Any, parts: Parts) -> frozenset[str]:
    """The table's FOREIGN KEY names as declared (``INFORMATION_SCHEMA`` prefixes them with the
    table name)."""
    project, dataset, table = parts
    rows = client.query(
        f"SELECT constraint_name FROM `{project}.{dataset}.INFORMATION_SCHEMA.TABLE_CONSTRAINTS` "
        f"WHERE table_name = @t AND constraint_type = 'FOREIGN KEY'",
        job_config=_params(t=table),
    ).result()
    prefix = f"{table}."
    return frozenset(str(r["constraint_name"]).removeprefix(prefix) for r in rows)


def _params(**values: str) -> Any:
    from google.cloud import bigquery

    return bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter(k, "STRING", v) for k, v in values.items()]
    )


def create_ddl(parts: Parts, columns: list[tuple[str, str]], pk_columns: list[str] | None) -> str:
    cols = ", ".join(f"`{name}` {bq_type(ir_type)}" for name, ir_type in columns)
    keys = list(pk_columns or [])
    constraint = f", PRIMARY KEY ({', '.join(f'`{c}`' for c in keys)}) NOT ENFORCED" if keys else ""
    return f"CREATE TABLE IF NOT EXISTS {qualified(parts)} ({cols}{constraint})"


def reconcile_bigquery_native(
    client: Any,
    *,
    parts: Parts,
    columns: list[tuple[str, str]],
    pk_columns: list[str] | None = None,
) -> str:
    """Converge the landed table to ``columns`` + ``pk_columns`` (DDL only, no data). Returns
    ``created`` | ``kept`` | ``recreated``: a matching table survives; a drifted one (columns or the
    key, REQ-1651) is recreated -- the registration is authoritative, data re-lands on refresh."""
    from google.cloud import bigquery

    project, dataset, _ = parts
    client.create_dataset(bigquery.Dataset(f"{project}.{dataset}"), exists_ok=True)
    have = existing_columns(client, parts)
    want = [name for name, _ in columns]
    if not have:
        client.query(create_ddl(parts, columns, pk_columns)).result()
        return "created"
    if have == want and existing_primary_key(client, parts) == tuple(pk_columns or ()):
        return "kept"
    client.query(f"DROP TABLE IF EXISTS {qualified(parts)}").result()
    client.query(create_ddl(parts, columns, pk_columns)).result()
    return "recreated"


# -- landed metadata -------------------------------------------------------------------------------


def constraint_statements(
    targets: dict[str, KeyTarget],
    edges: list[ForeignKeyEdge],
    existing_primary_keys: dict[Parts, tuple[str, ...]],
    existing_foreign_keys: dict[Parts, frozenset[str]],
) -> list[str]:
    """PRIMARY KEY and FOREIGN KEY DDL, all ``NOT ENFORCED``. A matching PRIMARY KEY is left alone;
    a differing one is replaced (``DROP PRIMARY KEY`` takes the FOREIGN KEYs referencing it, so
    every edge into a replaced key is re-added); a FOREIGN KEY of the same name is skipped; a
    ``provisa_fk_*`` key nothing declares any more is withdrawn -- keys of any other origin are
    never touched."""
    stmts: list[str] = []
    replaced: set[Parts] = set()
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
            replaced.add(target.replica)
        cols = ", ".join(f"`{c}`" for c in target.primary_key)
        stmts.append(f"ALTER TABLE {fq} ADD PRIMARY KEY ({cols}) NOT ENFORCED")
    wanted: dict[Parts, set[str]] = {}
    for edge in edges:
        holder = targets[edge.holder].replica
        referenced = targets[edge.referenced].replica
        wanted.setdefault(holder, set()).add(edge.name)
        if (
            edge.name in existing_foreign_keys.get(holder, frozenset())
            and referenced not in replaced
        ):
            continue
        cols = ", ".join(f"`{c}`" for c in edge.holder_columns)
        ref_cols = ", ".join(f"`{c}`" for c in edge.referenced_columns)
        stmts.append(
            f"ALTER TABLE {qualified(holder)} ADD CONSTRAINT `{edge.name}` FOREIGN KEY ({cols}) "
            f"REFERENCES {qualified(referenced)} ({ref_cols}) NOT ENFORCED"
        )
    for holder, names in existing_foreign_keys.items():
        for name in sorted(names):
            if name.startswith(FK_PREFIX) and name not in wanted.get(holder, set()):
                stmts.append(f"ALTER TABLE {qualified(holder)} DROP CONSTRAINT `{name}`")
    return stmts


def described_table(table: Any, target: KeyTarget, known_tags: frozenset[str]) -> list[str]:
    """Mutates the client's ``Table`` in place to carry ``target``'s description, column
    descriptions and labels; returns the fields that changed (``[]`` when nothing did). A
    description is written when the current one does not already begin with it, so a description
    another writer extended stands and an empty one never erases anything. A label the model
    defines that sits on the table but is no longer assigned is removed; any other label stays."""
    from google.cloud import bigquery

    changed: list[str] = []
    if target.description and not (table.description or "").startswith(target.description):
        table.description = target.description
        changed.append("description")
    wanted_columns = target.column_descriptions or {}
    schema = []
    schema_changed = False
    for field in table.schema:
        description = wanted_columns.get(field.name)
        if description and not (field.description or "").startswith(description):
            schema_changed = True
            field = bigquery.SchemaField(
                field.name,
                field.field_type,
                mode=field.mode,
                description=description,
                fields=field.fields,
            )
        schema.append(field)
    if schema_changed:
        table.schema = schema
        changed.append("schema")
    known = {label_key(t) for t in known_tags}
    wanted_labels = {label_key(tag): label_value(value) for tag, value in target.tags}
    labels = dict(table.labels or {})
    for key in list(labels):
        if key in known and key not in wanted_labels:
            labels[key] = None  # the API deletes a label set to None
    labels.update(wanted_labels)
    if {k: v for k, v in labels.items()} != dict(table.labels or {}):
        table.labels = labels
        changed.append("labels")
    return changed


def reconcile_metadata_native(
    client: Any,
    *,
    targets: dict[str, KeyTarget],
    edges: list[ForeignKeyEdge],
    known_tags: frozenset[str] = frozenset(),
) -> int:
    """Read each table's current keys, descriptions and labels, then apply exactly what changes. A
    table that does not exist yet (an MV before its first refresh) is left for the converge that
    creates it, with the edges touching it. Returns the number of DDL statements and table updates
    applied."""
    from google.api_core.exceptions import NotFound

    tables: dict[Parts, Any] = {}
    for target in targets.values():
        if target.replica in tables:
            continue
        try:
            tables[target.replica] = client.get_table(".".join(target.replica))
        except NotFound:
            continue
    present = {ident: t for ident, t in targets.items() if t.replica in tables}
    edges = [e for e in edges if e.holder in present and e.referenced in present]
    pks = {parts: existing_primary_key(client, parts) for parts in tables}
    fks = {parts: existing_foreign_keys(client, parts) for parts in tables}
    applied = 0
    for stmt in constraint_statements(present, edges, pks, fks):
        try:
            client.query(stmt).result()
            applied += 1
        except Exception as exc:  # noqa: BLE001 - the client's error type is Google's, not ours
            # One table's refusal must not withhold every other table's keys and descriptions.
            # Logged verbatim; nothing is retried or hidden.
            log.warning("landed metadata statement refused: %s -- %s", stmt, exc)
    for identity in sorted(present):
        target = present[identity]
        if target.column_tags:
            log.info(
                "%s: column tags withheld -- BigQuery holds labels on tables only: %s",
                ".".join(target.replica),
                sorted(target.column_tags),
            )
        table = tables[target.replica]
        fields = described_table(table, target, known_tags)
        if not fields:
            continue
        try:
            client.update_table(table, fields)
            applied += 1
        except Exception as exc:  # noqa: BLE001 - see above
            log.warning(
                "landed metadata update refused: %s %s -- %s", ".".join(target.replica), fields, exc
            )
    return applied
