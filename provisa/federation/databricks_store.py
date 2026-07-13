# Copyright (c) 2026 Kenneth Stott
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

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

from provisa.core.change_signal import APPEND, select_landing_shape
from provisa.core.ir_types import to_ir
from provisa.federation.databricks_uc import ensure_external_link

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
    cur.execute(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_catalog = ? AND table_schema = ? AND table_name = ? "
        "ORDER BY ordinal_position",
        [catalog, schema, table],
    )
    return [r[0] for r in cur.fetchall()]


def _create_ddl(catalog: str, schema: str, table: str, columns: list[tuple[str, str]]) -> str:
    cols = ", ".join(f"`{name}` {_ddl_type(ir_type)}" for name, ir_type in columns)
    return f"CREATE TABLE IF NOT EXISTS {_qualified(catalog, schema, table)} ({cols}) USING DELTA"


def reconcile_databricks_native(
    cur: Any,
    *,
    catalog: str,
    schema: str,
    table: str,
    columns: list[tuple[str, str]],
) -> str:
    """Converge the Databricks landing table to ``columns`` (DDL only, no data). Returns
    ``created`` | ``kept`` | ``recreated`` — an existing matching table survives; a drifted one is
    recreated (a config/schema change is authoritative; data re-lands on the next refresh)."""
    _ensure_namespace(cur, catalog, schema)
    have = _existing_columns(cur, catalog, schema, table)
    want = [name for name, _ in columns]
    if not have:
        cur.execute(_create_ddl(catalog, schema, table, columns))
        return "created"
    if have == want:
        return "kept"
    cur.execute(f"DROP TABLE IF EXISTS {_qualified(catalog, schema, table)}")
    cur.execute(_create_ddl(catalog, schema, table, columns))
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
) -> str:
    """Land ``rows`` into the Databricks table through the engine's own cursor (REQ-987, REQ-990).

    Shape from ``change_signal`` (REQ-932): a poll signal with a watermark APPENDS the delta; every
    other batch REPLACES (``TRUNCATE`` first). Ingest path is capability-gated (REQ-990): a batch of
    ``COPY_INTO_ROW_THRESHOLD`` rows or more lands via the bulk ``COPY INTO`` from a staged Parquet
    object when ``stage`` is configured; a smaller batch (or no stage — the target then lacks bulk)
    lands as one multi-row ``INSERT … VALUES``. Explicit gate, never a silent fallback. Returns the
    qualified name."""
    _ensure_namespace(cur, catalog, schema)
    cur.execute(_create_ddl(catalog, schema, table, columns))
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
