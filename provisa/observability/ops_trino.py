# Copyright (c) 2026 Kenneth Stott
# Canary: e0702f16-a9c7-4981-8f12-2fbf0dd95409
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# complexity-gate: allow-ble=6 reason="best-effort Iceberg DDL; each except logs exc_info and is non-fatal (catalog-not-ready is retried), extracted verbatim from app.py"

"""Trino-specific provisioning of the ops (telemetry) domain (REQ-016).

Creates the Iceberg schema/tables/views in Trino for the ``otel.signals`` catalog
and evolves them idempotently. Only relevant when Trino is the bound federation
engine — native engines land telemetry in the dedicated ops store (see
``ops_schema``/``otlp2sql``), so this module is never imported on that path.
"""

from __future__ import annotations

import logging
import re

from provisa.observability.ops_schema import OPS_TABLES

# Postgres column types (ops_schema's source of truth) mapped to Trino/Iceberg types.
from provisa.compiler.type_map import OPS_PG_TO_PHYSICAL
from provisa.federation.trino_types import TrinoConnection, TrinoError
from provisa.transpiler.transpile import transpile_to_trino


#: Trino's answer for an Iceberg table whose catalog row names metadata the object store lacks.
_MISSING_METADATA = "ICEBERG_MISSING_METADATA"


def _names_missing_metadata(exc: BaseException) -> bool:
    """Whether ``exc``, or what it was raised from, is Trino's ICEBERG_MISSING_METADATA."""
    seen: BaseException | None = exc
    while seen is not None:
        if getattr(seen, "error_name", None) == _MISSING_METADATA:
            return True
        seen = seen.__cause__ or seen.__context__
    return False


def metadata_missing(execute, table: str) -> bool:  # REQ-016, REQ-302
    """Whether ``otel.signals.<table>`` is registered with its metadata gone from the object store;
    when it is, an ERROR names the table and the location Trino could not read. ``execute`` runs
    one statement on the engine. Nothing is changed: the caller leaves the table alone.

    The ``otel`` catalog keeps each table's registration in the control DB and its metadata and
    data in the telemetry bucket. A table whose metadata cannot be read is most often one whose
    bucket is served from the wrong place for a while -- a disk not mounted yet -- and its metadata
    is intact once it is. Making the table again would turn that into the loss of its history (and
    let orphan-file removal delete the old files), so it is recovered by hand: the deployment guide's
    "Telemetry table whose metadata is missing"."""
    try:
        # A count reads the table's snapshot through its manifest list: what a cached table
        # handle (iceberg.metadata-cache) does not answer for it, and a write reads too.
        execute(f"SELECT count(*) FROM otel.signals.{table}")
        return False
    except (
        Exception
    ) as exc:  # allow-ble: a probe of one condition; the caller's statements report anything else
        if not _names_missing_metadata(exc):
            # A table not created yet, or an engine that cannot answer: the statements the caller
            # runs next create it, or fail with the cause.
            return False
        missing = exc
    _say_missing(table, missing)  # Trino's message names the metadata location it could not find
    return True


def _say_missing(table: str, where: object) -> None:
    logging.getLogger(__name__).error(
        "otel.signals.%s is registered but its metadata cannot be read from the telemetry bucket "
        "(%s): its signals are not compacted or changed until it is recovered by hand -- see the "
        "deployment guide, 'Telemetry table whose metadata is missing'",
        table,
        where,
    )


_LOCATION = re.compile(r"location = '(s3://[^']+)'")


def metadata_files_gone(rows, keys_under, table: str) -> bool:  # REQ-016, REQ-302
    """Whether ``otel.signals.<table>``'s location in the bucket holds no metadata file; when it
    holds none, the same ERROR as :func:`metadata_missing` names the table and the location.

    What :func:`metadata_missing`'s probe cannot see: the catalog caches a table's metadata
    (``iceberg.metadata-cache.enabled``), and a table with no snapshot yet is answered from that
    cache without reading the bucket -- and is then written, its lost metadata replaced, as if
    nothing had happened. ``rows`` runs one statement on the engine and returns its rows;
    ``keys_under(bucket, prefix)`` lists the bucket's keys under ``prefix``."""
    [[ddl]] = rows(f"SHOW CREATE TABLE otel.signals.{table}")
    found = _LOCATION.search(ddl)
    if found is None:
        raise ValueError(f"otel.signals.{table} names no location: {ddl}")
    bucket, prefix = found.group(1).removeprefix("s3://").split("/", 1)
    metadata = f"{prefix.rstrip('/')}/metadata/"
    if any(key.endswith(".metadata.json") for key in keys_under(bucket, metadata)):
        return False
    _say_missing(table, f"no metadata file under s3://{bucket}/{metadata}")
    return True


def otel_bucket_keys():
    """A ``keys_under(bucket, prefix)`` for :func:`metadata_files_gone` over the telemetry
    bucket's object store (``otel_object_store``)."""
    import boto3

    from provisa.core.trino_system_catalogs import otel_object_store

    store = otel_object_store()
    s3 = boto3.client(
        "s3",
        endpoint_url=store["endpoint"],
        aws_access_key_id=store["access_key"],
        aws_secret_access_key=store["secret_key"],
        region_name=store["region"],
    )

    def keys_under(bucket: str, prefix: str):
        for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                yield obj["Key"]

    return keys_under


def _ops_physical(pg_type: str) -> str:
    if pg_type not in OPS_PG_TO_PHYSICAL:
        raise ValueError(f"unmapped ops pg type: {pg_type}")
    return OPS_PG_TO_PHYSICAL[pg_type]


def seed_ops_trino(  # REQ-016
    trino_conn: TrinoConnection,
    ops_views: list[tuple[str, list[tuple[str, str, bool]], str]],
) -> None:
    """Create Iceberg schema/tables/views in Trino for the ops domain (idempotent). Each view's
    SELECT is PostgreSQL (``startup_seed._OPS_VIEWS``), transpiled to Trino's SQL here."""
    _log = logging.getLogger(__name__)

    def _exec(ddl: str) -> None:
        cur = trino_conn.cursor()
        cur.execute(ddl)
        cur.fetchall()

    # Schema + physical tables — one exception aborts table creation (catalog not ready).
    _tables_ready = True
    unreadable: set[str] = set()
    try:
        _exec("CREATE SCHEMA IF NOT EXISTS otel.signals")
        for tbl_name, cols in OPS_TABLES.items():
            if metadata_missing(_exec, tbl_name):
                unreadable.add(tbl_name)  # left registered, and untouched below
                continue
            col_defs = [f'"{col_name}" {_ops_physical(pg_type)}' for col_name, pg_type, _ in cols]
            col_names_lower = {col_name.lower() for col_name, _, _ in cols}
            partition_cols = (
                ["'table_name'", "'_date'"] if "table_name" in col_names_lower else ["'_date'"]
            )
            _exec(
                f"CREATE TABLE IF NOT EXISTS otel.signals.{tbl_name} "
                f"({', '.join(col_defs)}) "
                f"WITH (partitioning = ARRAY[{', '.join(partition_cols)}], format = 'PARQUET')"
            )
    except Exception as exc:
        # A statement the engine cut off at its run-time limit is not "catalog not ready yet": the
        # otel store behind the catalog did not answer, and boot must stop and say so.
        if getattr(exc, "error_name", None) == "EXCEEDED_TIME_LIMIT":
            raise
        _log.warning(
            "ops Iceberg DDL failed — will retry before next schema introspection", exc_info=True
        )
        _tables_ready = False

    if _tables_ready:
        # Column additions are non-fatal and isolated per table.
        for tbl_name, cols in OPS_TABLES.items():
            if tbl_name in unreadable:
                continue
            try:
                cur = trino_conn.cursor()
                cur.execute(f"SHOW COLUMNS FROM otel.signals.{tbl_name}")
                existing_cols = {row[0].lower() for row in cur.fetchall()}
                for col_name, pg_type, _ in cols:
                    if col_name.lower() not in existing_cols:
                        trino_type = _ops_physical(pg_type)
                        try:
                            _exec(
                                f'ALTER TABLE otel.signals.{tbl_name} ADD COLUMN "{col_name}" {trino_type}'
                            )
                            _log.info("ops Iceberg: added column %s.%s", tbl_name, col_name)
                        except Exception:
                            _log.warning(
                                "ops Iceberg: could not add column %s.%s",
                                tbl_name,
                                col_name,
                                exc_info=True,
                            )
            except Exception:
                _log.warning(
                    "ops Iceberg: could not inspect columns for %s", tbl_name, exc_info=True
                )

        # Evolve partition spec on existing tables to match what CREATE TABLE declares
        # (non-destructive). Trino spells partition evolution as SET PROPERTIES, not Spark's
        # ADD PARTITION FIELD, which every run raised SYNTAX_ERROR on.
        for tbl_name, cols in OPS_TABLES.items():
            if tbl_name in unreadable:
                continue
            col_names_lower = {col_name.lower() for col_name, _, _ in cols}
            partition_cols = (
                ["'table_name'", "'_date'"] if "table_name" in col_names_lower else ["'_date'"]
            )
            try:
                _exec(
                    f"ALTER TABLE otel.signals.{tbl_name} "
                    f"SET PROPERTIES partitioning = ARRAY[{', '.join(partition_cols)}]"
                )
            except TrinoError:
                pass  # already present, unsupported, or Trino transient — best-effort, not fatal

        # Warm up Iceberg metadata: first query on a cold Iceberg table can take >60s;
        # running a zero-row scan here ensures metadata is loaded before user requests arrive.
        for tbl_name in OPS_TABLES:
            if tbl_name in unreadable:
                continue
            try:
                _exec(f"SELECT 1 FROM otel.signals.{tbl_name} LIMIT 0")
            except Exception:
                _log.warning(
                    "ops Iceberg: warm-up scan on %s failed (non-fatal)", tbl_name, exc_info=True
                )

    # Views — always attempted; independent of column-addition and table-creation failures.
    # If the initial DDL block failed because Trino wasn't ready, these will also fail (caught
    # individually). If tables already exist from a prior run, views are created/refreshed here.
    # Use DROP IF EXISTS + CREATE VIEW for broad Trino version compatibility
    # (CREATE OR REPLACE VIEW for Iceberg requires Trino 418+).
    for view_name, _, view_select in ops_views:
        try:
            _exec(f"DROP VIEW IF EXISTS otel.signals.{view_name}")
            _exec(f"CREATE VIEW otel.signals.{view_name} AS {transpile_to_trino(view_select)}")
        except Exception:
            _log.warning("ops view %s: create failed", view_name, exc_info=True)

    # Snapshot/orphan-file reclamation is the scheduler's (REQ-302/303 reclaim_otel_storage): it
    # passes retention as the duration string the procedures take and sets the matching
    # min-retention session properties. Seeding attempted the same procedures with a TIMESTAMP,
    # which Trino rejects as INVALID_PROCEDURE_ARGUMENT on every boot.
