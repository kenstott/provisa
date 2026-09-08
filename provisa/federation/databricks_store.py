# Copyright (c) 2026 Kenneth Stott
# Canary: 7895e614-e0b3-44c1-8f4f-9b2b162ef8bb
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Databricks materialization-store write face (REQ-987, REQ-990).

Databricks is a self-only warehouse engine: materialized sources LAND into the warehouse itself,
through the engine's OWN connection (it cannot attach an external store). This module is the one place
that write happens — DDL (create schema/table) then a capability-gated columnar ingest.

Ingest path (REQ-990): a LARGE batch lands via the target store's bulk ``COPY INTO`` from a staged
Parquet object (columnar, MPP-parallel); a SMALL batch (or a run with no object stage configured —
the target then lacks bulk) lands as one multi-row ``INSERT … VALUES`` (a single columnar Delta
commit). The choice is explicit and capability-gated on ``COPY_INTO_ROW_THRESHOLD`` + stage presence
— never a silent fallback masking a failure. ``change_signal`` selects replace (truncate) vs append.
"""

# Requirements: REQ-987, REQ-990, REQ-1657

from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from provisa.core.change_signal import APPEND, select_landing_shape
from provisa.core.ir_types import to_ir
from provisa.federation.databricks_uc import ensure_external_link
from provisa.federation.landed_keys import FK_PREFIX, KeyTarget, Parts

if TYPE_CHECKING:
    from provisa.federation.landed_keys import ForeignKeyEdge

log = logging.getLogger(__name__)

# REQ-990: at/above this row count the bulk COPY-INTO (stage a Parquet object + MPP ingest) wins over
# a multi-row INSERT; below it the single INSERT statement is cheaper than staging an object. Explicit
# gate — a batch this size or larger takes COPY INTO whenever a stage is configured, never a fallback.
COPY_INTO_ROW_THRESHOLD = 1000

# Canonical IR name → pyarrow type for the staged Parquet batch. COPY INTO coerces to the Delta column
# types (_IR_TO_DATABRICKS); these fix the on-disk Parquet spelling so an all-NULL column is not
# inferred as the null type. JSON lands as a STRING column (source's serialized text).
_IR_TO_ARROW: dict[str, str] = {
    "smallint": "int16",
    "integer": "int32",
    "bigint": "int64",
    "text": "string",
    "boolean": "bool",
    "float": "float32",
    "double": "float64",
    "numeric": "decimal",
    "date": "date32",
    "timestamp": "timestamp",
    "time": "string",
    "uuid": "string",
    "bytea": "binary",
    "json": "string",
}

# Canonical IR name → Databricks/Delta SQL type. Delta has no unsigned/serial spellings; a landed
# replica carries the source's own key values, so integers/decimals map to their widest safe Delta
# type. JSON is stored as STRING (Databricks parses on read via from_json / : path access).
_IR_TO_DATABRICKS: dict[str, str] = {
    "smallint": "SMALLINT",
    "integer": "INT",
    "bigint": "BIGINT",
    "text": "STRING",
    "boolean": "BOOLEAN",
    "float": "FLOAT",
    "double": "DOUBLE",
    "numeric": "DECIMAL(38,9)",
    "date": "DATE",
    "timestamp": "TIMESTAMP",
    "time": "STRING",  # Delta has no bare TIME type
    "uuid": "STRING",
    "bytea": "BINARY",
    "json": "STRING",
}


def _ddl_type(ir_type: str) -> str:
    """Databricks column type for a canonical IR type name — raises on an unknown type (never a
    silent widen), mirroring the store-DDL discipline of the other engines."""
    canonical = to_ir(ir_type)  # normalize any native/dialect spelling to the IR vocabulary
    sql_type = _IR_TO_DATABRICKS.get(canonical)
    if sql_type is None:
        raise ValueError(
            f"no Databricks type mapping for IR type {ir_type!r} (canonical {canonical!r})"
        )
    return sql_type


def _qualified(catalog: str, schema: str, table: str) -> str:
    return f"`{catalog}`.`{schema}`.`{table}`"


def _ensure_namespace(cur: Any, catalog: str, schema: str) -> None:
    """Create the Unity Catalog + schema that hold the landed replica (idempotent). A self-only
    Databricks engine lands each source into a UC catalog named for the source (matching the
    compiler's physical name ``source_id.schema.table``), so the governed query resolves natively."""
    cur.execute(f"CREATE CATALOG IF NOT EXISTS `{catalog}`")
    cur.execute(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`")


def _existing_columns(cur: Any, catalog: str, schema: str, table: str) -> list[str]:
    """The store table's current column names, or ``[]`` if it does not exist."""
    # The catalog's own information_schema: unqualified, it is the session catalog's, which never
    # holds another catalog's tables -- the probe then reports every landed table as absent.
    cur.execute(
        f"SELECT column_name FROM `{catalog}`.information_schema.columns "
        "WHERE table_catalog = ? AND table_schema = ? AND table_name = ? "
        "ORDER BY ordinal_position",
        [catalog, schema, table],
    )
    return [r[0] for r in cur.fetchall()]


def _pk_name(table: str) -> str:
    return f"provisa_pk_{_tag_safe(table)}"


def _create_ddl(
    catalog: str,
    schema: str,
    table: str,
    columns: list[tuple[str, str]],
    pk_columns: list[str] | None = None,
) -> str:
    """Delta DDL for the landing table. The registration's PRIMARY KEY is declared inline (REQ-1657):
    Unity Catalog holds it as an informational constraint, and refuses one over a nullable column
    ("its child column(s) `id` is nullable", confirmed live), so each key column is NOT NULL."""
    keys = list(pk_columns or [])
    cols = ", ".join(
        f"`{name}` {_ddl_type(ir_type)}{' NOT NULL' if name in keys else ''}"
        for name, ir_type in columns
    )
    constraint = (
        f", CONSTRAINT `{_pk_name(table)}` PRIMARY KEY ({', '.join(f'`{c}`' for c in keys)})"
        if keys
        else ""
    )
    return (
        f"CREATE TABLE IF NOT EXISTS {_qualified(catalog, schema, table)} ({cols}{constraint}) "
        "USING DELTA"
    )


def reconcile_databricks_native(
    cur: Any,
    *,
    catalog: str,
    schema: str,
    table: str,
    columns: list[tuple[str, str]],
    pk_columns: list[str] | None = None,
) -> str:
    """Converge the Databricks landing table to ``columns`` + ``pk_columns`` (DDL only, no data).
    Returns ``created`` | ``kept`` | ``recreated`` — an existing matching table survives; a drifted
    one (columns or, REQ-1651, the key) is recreated (a config/schema change is authoritative; data
    re-lands on the next refresh)."""
    _ensure_namespace(cur, catalog, schema)
    parts = (catalog, schema, table)
    have = _existing_columns(cur, catalog, schema, table)
    want = [name for name, _ in columns]
    if not have:
        cur.execute(_create_ddl(catalog, schema, table, columns, pk_columns))
        return "created"
    if have == want and existing_primary_key(cur, parts) == tuple(pk_columns or ()):
        return "kept"
    cur.execute(f"DROP TABLE IF EXISTS {_qualified(catalog, schema, table)}")
    cur.execute(_create_ddl(catalog, schema, table, columns, pk_columns))
    return "recreated"


def _coerce(value: Any, ir_type: str) -> Any:
    """JSON columns take the source's serialized text; a dict/list is re-serialized so the STRING
    column holds valid JSON text. Everything else passes through to the driver's bind."""
    if to_ir(ir_type) == "json" and value is not None and not isinstance(value, str):
        return json.dumps(value)
    return value


@dataclass(frozen=True)
class DatabricksStage:
    """Object-storage staging substrate for the bulk COPY-INTO ingest (REQ-987/REQ-990).

    ``root_url`` is the ``r2://…/`` (S3-compatible) prefix each Parquet batch is written under;
    ``credential`` carries the R2 keys Unity Catalog installs to read them; ``uc_host``/``uc_token``
    reach the UC REST API to install the storage credential + external location before the COPY."""

    root_url: str  # r2://bucket@acct.r2.cloudflarestorage.com/prefix/
    endpoint_url: str  # S3-compatible endpoint for the Parquet upload
    credential: dict  # {access_key_id, secret_access_key, account_id}
    uc_host: str
    uc_token: str


def _arrow_type(ir_type: str) -> Any:
    """pyarrow type for a canonical IR type — raises on an unknown type (never a silent widen)."""
    import pyarrow as pa

    spelling = _IR_TO_ARROW.get(to_ir(ir_type))
    if spelling is None:
        raise ValueError(f"no Databricks-stage Arrow type for IR type {ir_type!r}")
    if spelling == "decimal":
        return pa.decimal128(38, 9)  # matches the Delta DECIMAL(38,9) landing type
    if spelling == "timestamp":
        return pa.timestamp("us")
    return getattr(pa, spelling)()


def _arrow_value(value: Any, ir_type: str) -> Any:
    """A row value coerced to what its Arrow column type accepts: JSON → text, numeric → Decimal."""
    if value is None:
        return None
    canonical = to_ir(ir_type)
    if canonical == "json" and not isinstance(value, str):
        return json.dumps(value)
    if canonical == "numeric":
        from decimal import Decimal

        return Decimal(str(value))
    return value


def _rows_to_arrow(columns: list[tuple[str, str]], rows: list[dict]) -> Any:
    """The batch as a ``pyarrow.Table`` typed by the columns' IR types (for the staged Parquet)."""
    import pyarrow as pa

    arrays = [
        pa.array([_arrow_value(r.get(name), ir_type) for r in rows], type=_arrow_type(ir_type))
        for name, ir_type in columns
    ]
    return pa.Table.from_arrays(arrays, names=[name for name, _ in columns])


def _stage_parquet(stage: DatabricksStage, key: str, arrow_table: Any) -> str:
    """Write ``arrow_table`` as Parquet and upload it under the stage root; return the object URL."""
    import io
    from urllib.parse import urlparse

    import boto3
    import pyarrow.parquet as pq

    u = urlparse(stage.root_url)
    bucket = u.netloc.split("@", 1)[0]
    object_key = (u.path.lstrip("/") + key).lstrip("/")  # root path already ends with '/'
    buf = io.BytesIO()
    pq.write_table(arrow_table, buf)
    s3 = boto3.client(
        "s3",
        endpoint_url=stage.endpoint_url,
        aws_access_key_id=stage.credential["access_key_id"],
        aws_secret_access_key=stage.credential["secret_access_key"],
        region_name="auto",
    )
    s3.put_object(Bucket=bucket, Key=object_key, Body=buf.getvalue())
    return f"{u.scheme}://{u.netloc}/{object_key}"


def _unstage(stage: DatabricksStage, object_url: str) -> None:
    """Delete a staged Parquet object after the COPY (it is already ingested into Delta)."""
    from urllib.parse import urlparse

    import boto3

    u = urlparse(stage.root_url)
    bucket = u.netloc.split("@", 1)[0]
    object_key = urlparse(object_url).path.lstrip("/")
    boto3.client(
        "s3",
        endpoint_url=stage.endpoint_url,
        aws_access_key_id=stage.credential["access_key_id"],
        aws_secret_access_key=stage.credential["secret_access_key"],
        region_name="auto",
    ).delete_object(Bucket=bucket, Key=object_key)


def _land_via_copy_into(
    cur: Any,
    stage: DatabricksStage,
    *,
    catalog: str,
    schema: str,
    table: str,
    columns: list[tuple[str, str]],
    rows: list[dict],
    replace: bool,
) -> None:
    """Bulk COPY-INTO ingest (REQ-990): stage the batch as Parquet, install+validate the UC credential
    that lets Databricks read it, then ``COPY INTO`` the Delta table (columnar, MPP). Replace truncates
    first (same shape as the INSERT path). The staged object is removed after ingest."""
    import uuid

    key = f"{catalog}/{schema}/{table}/{uuid.uuid4().hex}.parquet"
    object_url = _stage_parquet(stage, key, _rows_to_arrow(columns, rows))
    try:
        # Install + VALIDATE the UC storage credential + external location so Databricks can read the
        # staged object — a bad credential / unreachable path raises here (never a silent skip).
        ensure_external_link(
            stage.uc_host, stage.uc_token, location=object_url, credential=stage.credential
        )
        qualified = _qualified(catalog, schema, table)
        if replace:
            cur.execute(f"TRUNCATE TABLE {qualified}")
        collist = ", ".join(f"`{name}`" for name, _ in columns)
        cur.execute(
            f"COPY INTO {qualified} FROM (SELECT {collist} FROM '{object_url}') "
            f"FILEFORMAT = PARQUET COPY_OPTIONS ('force' = 'true')"
        )
    finally:
        _unstage(stage, object_url)


def land_databricks_native(
    cur: Any,
    *,
    catalog: str,
    schema: str,
    table: str,
    columns: list[tuple[str, str]],
    rows: list[dict],
    change_signal: str = "ttl",
    watermark_column: str | None = None,
    stage: DatabricksStage | None = None,
    pk_columns: list[str] | None = None,
) -> str:
    """Land ``rows`` into the Databricks table through the engine's own cursor (REQ-987, REQ-990).

    Shape from ``change_signal`` (REQ-932): a poll signal with a watermark APPENDS the delta; every
    other batch REPLACES (``TRUNCATE`` first). Ingest path is capability-gated (REQ-990): a batch of
    ``COPY_INTO_ROW_THRESHOLD`` rows or more lands via the bulk ``COPY INTO`` from a staged Parquet
    object when ``stage`` is configured; a smaller batch (or no stage — the target then lacks bulk)
    lands as one multi-row ``INSERT … VALUES``. Explicit gate, never a silent fallback. Returns the
    qualified name."""
    _ensure_namespace(cur, catalog, schema)
    cur.execute(_create_ddl(catalog, schema, table, columns, pk_columns))
    qualified = _qualified(catalog, schema, table)
    replace = select_landing_shape(change_signal, watermark_column) != APPEND
    # Ingest gate (REQ-990): large batch + a configured object stage → columnar bulk COPY INTO; a
    # small batch or no stage → multi-row INSERT (acceptable per REQ-990 for tiny/no-bulk writes).
    if stage is not None and len(rows) >= COPY_INTO_ROW_THRESHOLD:
        _land_via_copy_into(
            cur,
            stage,
            catalog=catalog,
            schema=schema,
            table=table,
            columns=columns,
            rows=rows,
            replace=replace,
        )
        return qualified
    if replace:
        cur.execute(f"TRUNCATE TABLE {qualified}")
    if rows:
        colnames = [name for name, _ in columns]
        collist = ", ".join(f"`{cn}`" for cn in colnames)
        row_ph = "(" + ", ".join("?" * len(colnames)) + ")"
        placeholders = ", ".join([row_ph] * len(rows))
        params: list[Any] = []
        for r in rows:
            for name, ir_type in columns:
                params.append(_coerce(r.get(name), ir_type))
        cur.execute(f"INSERT INTO {qualified} ({collist}) VALUES {placeholders}", params)
    return qualified


# -- landed metadata (REQ-1657: keys REQ-1652, descriptions REQ-1654, tags REQ-1655) -------------
#
# Unity Catalog holds informational PRIMARY/FOREIGN KEY constraints, COMMENTs on tables and
# columns, and key/value TAGs on both. There is no view layer on Databricks (the landed Delta table
# IS the compiler's physical name), so every target is its replica alone. All facts confirmed live:
# a PRIMARY KEY column must be NOT NULL; dropping a PRIMARY KEY with dependent FOREIGN KEYs needs
# CASCADE; a tag key may not contain ``.`` ``=`` ``>`` ``<`` ``%`` ``&`` ``?`` ``\``.

#: Prefix of every tag Provisa sets: ``provisa_governance:<tag id>``.
TAG_PREFIX = "provisa_governance:"

_TAG_RESERVED = re.compile(r"[.=<>%&?\\\s]")


def _tag_safe(raw: str) -> str:
    return _TAG_RESERVED.sub("_", raw.replace("-", "_"))


def tag_key(tag_id: str) -> str:
    """The Unity Catalog tag key for a model tag id."""
    return f"{TAG_PREFIX}{_tag_safe(tag_id.lower())}"


def _lit(value: str) -> str:
    """A Databricks SQL string literal body: backslash is the escape character, a quote doubles."""
    return value.replace("\\", "\\\\").replace("'", "''")


def _where(parts: Parts, alias: str = "") -> str:
    _, schema, table = parts
    return f"{alias}table_schema = '{_lit(schema)}' AND {alias}table_name = '{_lit(table)}'"


def object_exists(cur: Any, parts: Parts) -> bool:
    catalog = parts[0]
    cur.execute(
        f"SELECT 1 FROM `{catalog}`.information_schema.tables WHERE {_where(parts)}",
    )
    return bool(cur.fetchall())


def existing_primary_key(cur: Any, parts: Parts) -> tuple[str, ...]:
    catalog = parts[0]
    cur.execute(
        "SELECT k.column_name FROM "
        f"`{catalog}`.information_schema.table_constraints c "
        f"JOIN `{catalog}`.information_schema.key_column_usage k "
        "ON k.constraint_catalog = c.constraint_catalog "
        "AND k.constraint_schema = c.constraint_schema "
        "AND k.constraint_name = c.constraint_name "
        f"WHERE c.constraint_type = 'PRIMARY KEY' AND {_where(parts, 'c.')} "
        "ORDER BY k.ordinal_position"
    )
    return tuple(str(r[0]) for r in cur.fetchall())


def existing_foreign_keys(cur: Any, parts: Parts) -> frozenset[str]:
    catalog = parts[0]
    cur.execute(
        f"SELECT constraint_name FROM `{catalog}`.information_schema.table_constraints "
        f"WHERE constraint_type = 'FOREIGN KEY' AND {_where(parts)}"
    )
    return frozenset(str(r[0]) for r in cur.fetchall())


def existing_comments(cur: Any, parts: Parts) -> tuple[str, dict[str, str]]:
    catalog = parts[0]
    cur.execute(f"SELECT comment FROM `{catalog}`.information_schema.tables WHERE {_where(parts)}")
    rows = cur.fetchall()
    table_comment = str(rows[0][0] or "") if rows else ""
    cur.execute(
        f"SELECT column_name, comment FROM `{catalog}`.information_schema.columns "
        f"WHERE {_where(parts)}"
    )
    return table_comment, {str(c): str(comment or "") for c, comment in cur.fetchall()}


@dataclass(frozen=True)
class ObjectTags:
    table: dict[str, str]
    columns: dict[str, dict[str, str]]


def existing_tags(cur: Any, parts: Parts) -> ObjectTags:
    """The Provisa-prefixed tags on the table and its columns (a tag of any other origin is not
    read, so it can never be touched)."""
    catalog, schema, table = parts
    where = f"schema_name = '{_lit(schema)}' AND table_name = '{_lit(table)}'"
    cur.execute(
        f"SELECT tag_name, tag_value FROM `{catalog}`.information_schema.table_tags WHERE {where}"
    )
    table_tags = {str(k): str(v or "") for k, v in cur.fetchall() if str(k).startswith(TAG_PREFIX)}
    cur.execute(
        f"SELECT column_name, tag_name, tag_value FROM `{catalog}`.information_schema.column_tags "
        f"WHERE {where}"
    )
    columns: dict[str, dict[str, str]] = {}
    for column, k, v in cur.fetchall():
        if str(k).startswith(TAG_PREFIX):
            columns.setdefault(str(column), {})[str(k)] = str(v or "")
    return ObjectTags(table=table_tags, columns=columns)


def _q(parts: Parts) -> str:
    return _qualified(*parts)


def constraint_statements(
    targets: dict[str, KeyTarget],
    edges: list[ForeignKeyEdge],
    existing_primary_keys: dict[Parts, tuple[str, ...]],
    existing_foreign_keys: dict[Parts, frozenset[str]],
) -> list[str]:
    """PRIMARY KEY and FOREIGN KEY DDL. A matching PRIMARY KEY is left alone; a differing one is
    replaced (``DROP ... CASCADE`` takes its dependent FOREIGN KEYs with it, so every edge into a
    replaced key is re-added); a FOREIGN KEY of the same name is skipped; a ``provisa_fk_*`` key
    nothing declares any more is withdrawn -- keys of any other origin are never touched."""
    stmts: list[str] = []
    replaced: set[Parts] = set()
    for identity in sorted(targets):
        target = targets[identity]
        if not target.primary_key:
            continue
        current = existing_primary_keys.get(target.replica, ())
        if current == target.primary_key:
            continue
        fq = _q(target.replica)
        if current:
            stmts.append(f"ALTER TABLE {fq} DROP PRIMARY KEY CASCADE")
            replaced.add(target.replica)
        for column in target.primary_key:
            stmts.append(f"ALTER TABLE {fq} ALTER COLUMN `{column}` SET NOT NULL")
        cols = ", ".join(f"`{c}`" for c in target.primary_key)
        stmts.append(
            f"ALTER TABLE {fq} ADD CONSTRAINT `{_pk_name(target.replica[2])}` PRIMARY KEY ({cols})"
        )
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
            f"ALTER TABLE {_q(holder)} ADD CONSTRAINT `{edge.name}` FOREIGN KEY ({cols}) "
            f"REFERENCES {_q(referenced)} ({ref_cols})"
        )
    for holder, names in existing_foreign_keys.items():
        for name in sorted(names):
            if name.startswith(FK_PREFIX) and name not in wanted.get(holder, set()):
                stmts.append(f"ALTER TABLE {_q(holder)} DROP CONSTRAINT `{name}`")
    return stmts


def comment_statements(
    targets: dict[str, KeyTarget], existing: dict[Parts, tuple[str, dict[str, str]]]
) -> list[str]:
    """COMMENT DDL for the model's descriptions. Written when the current comment does not already
    begin with the description, so a comment another writer extended stands, and an empty
    description never erases anything."""

    def needs(current: str, description: str) -> bool:
        return bool(description) and not current.startswith(description)

    stmts: list[str] = []
    for identity in sorted(targets):
        target = targets[identity]
        current_table, current_columns = existing.get(target.replica, ("", {}))
        fq = _q(target.replica)
        if needs(current_table, target.description):
            stmts.append(f"COMMENT ON TABLE {fq} IS '{_lit(target.description)}'")
        for column, description in sorted((target.column_descriptions or {}).items()):
            if needs(current_columns.get(column, ""), description):
                stmts.append(
                    f"ALTER TABLE {fq} ALTER COLUMN `{column}` COMMENT '{_lit(description)}'"
                )
    return stmts


def tag_statements(
    targets: dict[str, KeyTarget],
    existing: dict[Parts, ObjectTags],
    known_tags: frozenset[str],
) -> list[str]:
    """The stewards' classifications as Unity Catalog tags (REQ-1655): ``provisa_governance:<tag>``
    = the assignment's reason (else the tag id), on the table and each tagged column. A tag the
    model defines that sits on an object but is no longer assigned there is unset."""
    known = {tag_key(t) for t in known_tags}
    stmts: list[str] = []
    for identity in sorted(targets):
        target = targets[identity]
        wanted_table = {tag_key(tag): value for tag, value in target.tags}
        wanted_columns = {
            column: {tag_key(tag): value for tag, value in tags}
            for column, tags in (target.column_tags or {}).items()
        }
        current = existing.get(target.replica, ObjectTags(table={}, columns={}))
        fq = f"ALTER TABLE {_q(target.replica)}"
        for tag, value in sorted(wanted_table.items()):
            if current.table.get(tag) != value:
                stmts.append(f"{fq} SET TAGS ('{tag}' = '{_lit(value)}')")
        for tag in sorted((set(current.table) & known) - set(wanted_table)):
            stmts.append(f"{fq} UNSET TAGS ('{tag}')")
        for column in sorted(set(wanted_columns) | set(current.columns)):
            wanted = wanted_columns.get(column, {})
            have = current.columns.get(column, {})
            for tag, value in sorted(wanted.items()):
                if have.get(tag) != value:
                    stmts.append(
                        f"{fq} ALTER COLUMN `{column}` SET TAGS ('{tag}' = '{_lit(value)}')"
                    )
            for tag in sorted((set(have) & known) - set(wanted)):
                stmts.append(f"{fq} ALTER COLUMN `{column}` UNSET TAGS ('{tag}')")
    return stmts


def reconcile_metadata_native(
    cur: Any,
    *,
    targets: dict[str, KeyTarget],
    edges: list[ForeignKeyEdge],
    known_tags: frozenset[str] = frozenset(),
) -> int:
    """Read each table's current keys, tags and comments, then apply exactly the statements that
    change something. A table that does not exist yet (an MV before its first refresh) is left for
    the converge that creates it, with the edges touching it. Returns the statements run."""
    present = {ident: t for ident, t in targets.items() if object_exists(cur, t.replica)}
    edges = [e for e in edges if e.holder in present and e.referenced in present]
    pks: dict[Parts, tuple[str, ...]] = {}
    fks: dict[Parts, frozenset[str]] = {}
    tags: dict[Parts, ObjectTags] = {}
    comments: dict[Parts, tuple[str, dict[str, str]]] = {}
    for target in present.values():
        if target.replica in pks:
            continue
        pks[target.replica] = existing_primary_key(cur, target.replica)
        fks[target.replica] = existing_foreign_keys(cur, target.replica)
        tags[target.replica] = existing_tags(cur, target.replica)
        comments[target.replica] = existing_comments(cur, target.replica)
    statements = (
        constraint_statements(present, edges, pks, fks)
        + tag_statements(present, tags, known_tags)
        + comment_statements(present, comments)
    )
    applied = 0
    for stmt in statements:
        try:
            cur.execute(stmt)
            applied += 1
        except Exception as exc:  # noqa: BLE001 - the DBAPI's error type is the driver's, not ours
            # One table's refusal (a key column holding NULLs) must not withhold every other
            # table's keys, tags and comments. Logged verbatim; nothing is retried or hidden.
            log.warning("landed metadata statement refused: %s -- %s", stmt, exc)
    return applied
