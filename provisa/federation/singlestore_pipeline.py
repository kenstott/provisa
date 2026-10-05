# Copyright (c) 2026 Kenneth Stott
# Canary: c0dcf3b0-1b7f-4047-a6ba-c6b43e1b3cc8
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""SingleStore PIPELINE landing (REQ-990, REQ-848): with SingleStore as the engine's own store, a
Kafka topic or an object-store file lands through a ``CREATE PIPELINE … LOAD DATA`` the database runs
itself, instead of Provisa relaying the rows.

This module is pure: it decides which origins land by pipeline (:func:`lands_by_pipeline`) and
renders the DDL (:func:`object_store_link_ddl`, :func:`file_pipeline_ddl`, :func:`kafka_pipeline_ddl`).
Running
it belongs to the build and the push wiring.

Scope (maintainer, 2026-10-04):

- Kafka JSON and Avro land by pipeline. Kafka Protobuf never does; it keeps the Provisa relay.
- CSV and Parquet on S3, GCS or Azure land by pipeline, reading the store's credentials from the
  source's ``federation_hints`` (maintainer, 2026-10-05): S3 access keys, GCS HMAC keys, an Azure
  account key. Without them the landing is refused by name. A local file never lands by pipeline
  (the database cannot read it); it keeps the relay.
- Iceberg on S3 lands by pipeline from a GLUE, REST, JDBC or SNOWFLAKE catalog, only on a workspace
  that enables ``enable_iceberg_ingest`` (refused by name otherwise). Iceberg elsewhere, Delta Lake,
  Hive and every other type keep the relay.
- A copy that needs a region filter is refused by name until the filtered regional copy exists.

A pipeline that fails raises with SingleStore's own error; it is never retried through the relay.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Any

#: The store backend (SQLAlchemy dialect name) whose engine lands by pipeline.
PIPELINE_STORE = "singlestoredb"

_KAFKA_PIPELINE_FORMATS = frozenset({"json", "avro"})
_FILE_PIPELINE_FORMATS = frozenset({"csv", "parquet"})
_GCS_SCHEMES = frozenset({"gs", "gcs"})
_AZURE_SCHEMES = frozenset({"az", "azure", "abfs", "abfss", "wasb", "wasbs"})


class PipelineRefused(RuntimeError):
    """A landing the SingleStore PIPELINE face cannot do, refused by name (never relayed)."""


@dataclass(frozen=True)
class LandOrigin:
    """Where a table's rows come from, as the pipeline face reads it: the source type, the
    location (a file URL, or a Kafka topic), and the format (``json``/``avro``/``protobuf`` for
    Kafka; ``csv``/``parquet``/``iceberg`` for files)."""

    source_type: str
    location: str
    format: str


def _scheme(location: str) -> str:
    head, sep, _ = location.partition("://")
    return head.lower() if sep else ""


def _field(holder: Any, name: str) -> Any:
    """``name`` of a registered-table row (a dict) or of a model object; None when absent."""
    if holder is None:
        return None
    if isinstance(holder, dict):
        return holder.get(name)
    return getattr(holder, name, None)


def origin_of(source: Any, table: Any = None) -> LandOrigin:
    """The :class:`LandOrigin` of ``table`` (a registered-table row or a model table) on
    ``source``. A Kafka table names its topic and format in ``live.kafka`` (REQ-813); a file
    source's location is its ``path`` and its format is its type."""
    source_type = source.type.value if hasattr(source.type, "value") else str(source.type)
    if source_type == "kafka":
        kafka = _field(_field(table, "live"), "kafka")
        return LandOrigin(
            "kafka", _field(kafka, "topic") or "", (_field(kafka, "format") or "json").lower()
        )
    return LandOrigin(source_type, source.path or "", source_type)


def lands_by_pipeline(origin: LandOrigin) -> bool:
    """Whether ``origin`` lands through a SingleStore PIPELINE. False means it keeps the Provisa
    relay; that is decided by type, not tried and then fallen back to. Raises
    :class:`PipelineRefused` for an origin in the pipeline's scope that cannot land yet."""
    if origin.source_type == "kafka":
        return origin.format in _KAFKA_PIPELINE_FORMATS
    if origin.source_type not in _FILE_PIPELINE_FORMATS | {"iceberg"}:
        return False
    store = object_store(origin.location)
    if origin.source_type == "iceberg":
        return store == "S3"  # iceberg off S3: the relay
    return store is not None  # a local file: the relay


def object_store(location: str) -> str | None:
    """The object store a file location is in, as SingleStore names it (``S3``, ``GCS``,
    ``AZURE``), or None for a local path or any other transport."""
    scheme = _scheme(location)
    if scheme == "s3":
        return "S3"
    if scheme in _GCS_SCHEMES:
        return "GCS"
    if scheme in _AZURE_SCHEMES:
        return "AZURE"
    return None


def refuse_region_filter(region_filter: str | None, *, table: str) -> None:
    """Refuse a pipeline landing that needs a region filter: the filtered regional copy does not
    exist yet, and a pipeline that loaded unfiltered rows would carry them out of their region."""
    if region_filter is not None:
        raise PipelineRefused(
            f"{table} needs a region filter at load; a filtered regional copy is not available, "
            f"so it cannot land through a SingleStore pipeline"
        )


# -- names ----------------------------------------------------------------------------------------


def _short(kind: str, schema: str, table: str) -> str:
    digest = hashlib.sha256(f"{schema}.{table}".encode()).hexdigest()[:20]
    return f"provisa_{kind}_{digest}"


def pipeline_name(schema: str, table: str) -> str:
    """The pipeline that lands ``schema.table``: one per replica, so a retire finds it by name."""
    return _short("pl", schema, table)


def link_name(schema: str, table: str) -> str:
    """The credential LINK the pipeline for ``schema.table`` reads its storage credentials from."""
    return _short("ln", schema, table)


def procedure_name(schema: str, table: str) -> str:
    """The stored procedure a Kafka pipeline for ``schema.table`` applies each batch through."""
    return _short("pp", schema, table)


# -- SQL rendering ---------------------------------------------------------------------------------


def _ident(name: str) -> str:
    return "`" + name.replace("`", "``") + "`"


def _qualified(schema: str, name: str) -> str:
    return f"{_ident(schema)}.{_ident(name)}"


def _literal(value: str) -> str:
    return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"


def _json_literal(value: dict) -> str:
    return _literal(json.dumps(value, separators=(",", ":"), sort_keys=True))


def store_path(location: str) -> str:
    """A file location as a SingleStore pipeline names it: ``bucket/key`` (S3, GCS) or
    ``container/path`` (Azure). An ADLS/WASB URL names its container before the ``@``:
    ``abfss://container@account.dfs.core.windows.net/path`` is ``container/path``."""
    rest = location.split("://", 1)[1]
    if _scheme(location) in {"abfs", "abfss", "wasb", "wasbs"}:
        authority, _, path = rest.partition("/")
        return f"{authority.split('@', 1)[0]}/{path}"
    return rest


#: The federation_hints each object store's pipeline credentials are read from (REQ-990).
_STORE_HINTS = {
    "S3": ("access_key_id", "secret_access_key", "region"),
    "GCS": ("gcs_access_id", "gcs_secret_key"),
    "AZURE": ("azure_account_name", "azure_account_key"),
}


def object_store_link_ddl(schema: str, link: str, location: str, hints: dict) -> str:
    """``CREATE OR REPLACE LINK`` holding the credentials of the object store ``location`` is in,
    so the pipeline DDL carries none. ``hints`` are the source's ``federation_hints`` with secret
    references already resolved. A missing credential is refused by name."""
    store = object_store(location)
    if store is None:
        raise PipelineRefused(f"{location} is not in an object store a SingleStore pipeline reads")
    missing = [k for k in _STORE_HINTS[store] if not hints.get(k)]
    if missing:
        raise PipelineRefused(
            f"a {store} pipeline needs federation_hints {', '.join(missing)} on the source"
        )
    if store == "S3":
        return s3_link_ddl(schema, link, hints)
    if store == "GCS":
        credentials = {"access_id": hints["gcs_access_id"], "secret_key": hints["gcs_secret_key"]}
    else:
        credentials = {
            "account_name": hints["azure_account_name"],
            "account_key": hints["azure_account_key"],
        }
    return (
        f"CREATE OR REPLACE LINK {_qualified(schema, link)} AS {store} "
        f"CREDENTIALS {_json_literal(credentials)} CONFIG '{{}}'"
    )


def s3_link_ddl(schema: str, link: str, hints: dict) -> str:
    """``CREATE OR REPLACE LINK`` holding a file source's S3 credentials, so the pipeline DDL carries
    none (one left by a build that died is replaced).
    ``hints`` are the source's ``federation_hints`` with secret references already resolved
    (``access_key_id``, ``secret_access_key``, ``region``, optional ``endpoint``)."""
    missing = [k for k in ("access_key_id", "secret_access_key", "region") if not hints.get(k)]
    if missing:
        raise PipelineRefused(
            f"an S3 pipeline needs federation_hints {', '.join(missing)} on the source"
        )
    config: dict[str, str] = {"region": hints["region"]}
    if hints.get("endpoint"):
        config["endpoint_url"] = hints["endpoint"]
    credentials = {
        "aws_access_key_id": hints["access_key_id"],
        "aws_secret_access_key": hints["secret_access_key"],
    }
    return (
        f"CREATE OR REPLACE LINK {_qualified(schema, link)} AS S3 "
        f"CREDENTIALS {_json_literal(credentials)} CONFIG {_json_literal(config)}"
    )


# -- Iceberg ---------------------------------------------------------------------------------------

#: The workspace setting an Iceberg pipeline needs; read before a build, refused by name when off.
ICEBERG_GATE_QUERY = "SELECT @@enable_iceberg_ingest"

#: The catalog types an Iceberg source may name, and the federation_hints each one requires on top
#: of the S3 credentials and region (maintainer, 2026-10-05: all four). JDBC's user and password
#: are optional (a catalog database without login takes none).
ICEBERG_CATALOG_HINTS = {
    "GLUE": (),
    "REST": ("iceberg_catalog_uri",),
    "JDBC": ("iceberg_catalog_name", "iceberg_catalog_warehouse", "iceberg_catalog_uri"),
    "SNOWFLAKE": (
        "iceberg_catalog_uri",
        "iceberg_catalog_user",
        "iceberg_catalog_password",
        "iceberg_catalog_role",
    ),
}

# federation_hint → the CONFIG key SingleStore reads it from.
_ICEBERG_CONFIG_KEYS = {
    "iceberg_catalog_name": "catalog_name",
    "iceberg_catalog_uri": "catalog.uri",
    "iceberg_catalog_warehouse": "catalog.warehouse",
    "iceberg_catalog_user": "catalog.jdbc.user",
    "iceberg_catalog_password": "catalog.jdbc.password",
    "iceberg_catalog_role": "catalog.jdbc.role",
}


def iceberg_pipeline_ddl(
    *, schema: str, pipeline: str, hints: dict, into_table: str, columns: list[str]
) -> str:
    """``CREATE PIPELINE`` that loads the latest snapshot of an Iceberg table, once, into
    ``into_table`` (``ingest_mode`` ``one_time``). The table is named by ``iceberg_table_id`` in
    the catalog ``iceberg_catalog_type`` names, reached with the source's S3 credentials.

    SingleStore takes no LINK with an Iceberg pipeline's CONFIG, so the credentials are inline. The
    S3 keys are in CREDENTIALS, which SingleStore redacts in its pipeline metadata. A JDBC or
    Snowflake catalog's password is a CONFIG key, which it does NOT redact: while the build's
    pipeline exists, that password is readable in information_schema.PIPELINES (maintainer,
    2026-10-05: accepted and documented)."""
    catalog = (hints.get("iceberg_catalog_type") or "").upper()
    if catalog not in ICEBERG_CATALOG_HINTS:
        raise PipelineRefused(
            "an Iceberg pipeline needs federation_hints iceberg_catalog_type, one of "
            + ", ".join(ICEBERG_CATALOG_HINTS)
        )
    required = (
        "iceberg_table_id",
        "access_key_id",
        "secret_access_key",
        "region",
        *ICEBERG_CATALOG_HINTS[catalog],
    )
    missing = [k for k in required if not hints.get(k)]
    if missing:
        raise PipelineRefused(
            f"an Iceberg {catalog} pipeline needs federation_hints {', '.join(missing)} on the source"
        )
    config: dict[str, str] = {
        "region": hints["region"],
        "catalog_type": catalog,
        "ingest_mode": "one_time",
    }
    if hints.get("endpoint"):
        config["endpoint_url"] = hints["endpoint"]
    for hint, key in _ICEBERG_CONFIG_KEYS.items():
        if hints.get(hint):
            config[key] = hints[hint]
    credentials = {
        "aws_access_key_id": hints["access_key_id"],
        "aws_secret_access_key": hints["secret_access_key"],
    }
    fields = ", ".join(f"{_ident(c)} <- {_ident(c)}" for c in columns)
    return (
        f"CREATE PIPELINE {_qualified(schema, pipeline)} AS "
        f"LOAD DATA S3 {_literal(hints['iceberg_table_id'])} "
        f"CONFIG {_json_literal(config)} CREDENTIALS {_json_literal(credentials)} "
        f"INTO TABLE {_qualified(schema, into_table)} ({fields}) FORMAT ICEBERG"
    )


def file_pipeline_ddl(
    *,
    schema: str,
    pipeline: str,
    link: str,
    origin: LandOrigin,
    into_table: str,
    columns: list[str],
    csv_header: list[str] | None = None,
) -> str:
    """``CREATE PIPELINE`` that loads ``origin`` (CSV or Parquet on S3) into ``into_table``.

    A CSV file is read by position, so the file's own header line (``csv_header``, read from the
    file) names what each position is: a header column the table does not declare is skipped, and
    a declared column the header lacks is refused by name. A Parquet file is read by field name.
    ``into_table`` has no key (the build table), so no duplicate-key policy is needed."""
    source = f"LOAD DATA LINK {_qualified(schema, link)} {_literal(store_path(origin.location))}"
    target = f"INTO TABLE {_qualified(schema, into_table)}"
    if origin.format == "parquet":
        fields = ", ".join(f"{_ident(c)} <- {_ident(c)}" for c in columns)
        body = f"{target} FORMAT PARQUET ({fields})"
    elif origin.format == "csv":
        if csv_header is None:
            raise ValueError("a CSV pipeline needs the file's header line")
        absent = [c for c in columns if c not in csv_header]
        if absent:
            raise PipelineRefused(
                f"{origin.location} has no column {', '.join(absent)} in its header line"
            )
        wanted = set(columns)
        positions = ", ".join(_ident(h) if h in wanted else "@skip" for h in csv_header)
        body = (
            f"{target} FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"' "
            f"LINES TERMINATED BY '\\n' IGNORE 1 LINES ({positions})"
        )
    else:
        raise PipelineRefused(f"{origin.format} files do not land through a SingleStore pipeline")
    return f"CREATE PIPELINE {_qualified(schema, pipeline)} AS {source} {body}"


def kafka_pipeline_ddl(
    *,
    schema: str,
    pipeline: str,
    procedure: str,
    bootstrap: str,
    origin: LandOrigin,
    table: str,
    columns: list[tuple[str, str]],
    pk_columns: list[str],
    field_mapping: dict[str, str] | None = None,
    schema_registry: str | None = None,
) -> list[str]:
    """The statements that land a Kafka topic into ``table`` continuously, in order.

    JSON goes through a stored procedure that applies each batch the way the relay does
    (``kafka_provider``): a message is either a row, or ``{"op": …, "row": {…}}``; ``op`` delete
    removes the key, anything else upserts it. Avro goes straight into the table, upserting on the
    key; its fields come from the schema registry, which an Avro pipeline requires.
    ``columns`` are (name, SingleStore column type). ``field_mapping`` maps a Kafka field to a
    table column (REQ-813)."""
    if not pk_columns:
        raise PipelineRefused(f"{table} has no primary key; a Kafka pipeline upserts by key")
    if origin.format not in _KAFKA_PIPELINE_FORMATS:
        raise PipelineRefused(f"Kafka {origin.format} does not land through a SingleStore pipeline")
    field_of = {column: field for field, column in (field_mapping or {}).items()}
    names = [name for name, _ in columns]
    non_key = [n for n in names if n not in pk_columns]
    upsert = ", ".join(f"{_ident(n)} = VALUES({_ident(n)})" for n in non_key) or (
        f"{_ident(pk_columns[0])} = VALUES({_ident(pk_columns[0])})"
    )
    source = f"LOAD DATA KAFKA {_literal(f'{bootstrap}/{origin.location}')}"
    target_table = _qualified(schema, table)
    if origin.format == "avro":
        if not schema_registry:
            raise PipelineRefused(f"Kafka Avro into {table} needs a schema registry URL")
        fields = ", ".join(f"{_ident(n)} <- %::{_ident(field_of.get(n, n))}" for n in names)
        return [
            f"CREATE PIPELINE {_qualified(schema, pipeline)} AS {source} "
            f"INTO TABLE {target_table} FORMAT AVRO SCHEMA REGISTRY {_literal(schema_registry)} "
            f"({fields}) ON DUPLICATE KEY UPDATE {upsert}"
        ]
    batch = ", ".join(["op TEXT"] + [f"{_ident(n)} {t}" for n, t in columns])
    cols = ", ".join(_ident(n) for n in names)
    key_match = " AND ".join(f"t.{_ident(k)} = b.{_ident(k)}" for k in pk_columns)
    proc = (
        f"CREATE OR REPLACE PROCEDURE {_qualified(schema, procedure)}(batch QUERY({batch})) AS "
        f"BEGIN "
        f"DELETE t FROM {target_table} t JOIN batch b ON {key_match} WHERE b.op = 'delete'; "
        f"INSERT INTO {target_table} ({cols}) SELECT {cols} FROM batch WHERE op <> 'delete' "
        f"ON DUPLICATE KEY UPDATE {upsert}; "
        f"END"
    )
    # A message is a flat row or an {"op", "row"} envelope: read both places, take the envelope's.
    reads = ["op <- op DEFAULT 'insert'"]
    sets = []
    for i, n in enumerate(names):
        field = _ident(field_of.get(n, n))
        reads.append(f"@f{i} <- {field} DEFAULT NULL")
        reads.append(f"@r{i} <- row::{field} DEFAULT NULL")
        sets.append(f"{_ident(n)} = COALESCE(@r{i}, @f{i})")
    pipe = (
        f"CREATE PIPELINE {_qualified(schema, pipeline)} AS {source} "
        f"INTO PROCEDURE {_qualified(schema, procedure)} FORMAT JSON ({', '.join(reads)}) "
        f"SET {', '.join(sets)}"
    )
    return [proc, pipe]


def start_foreground(schema: str, pipeline: str) -> str:
    """Run a one-shot pipeline to completion on the calling connection (a whole-table build)."""
    return f"START PIPELINE {_qualified(schema, pipeline)} FOREGROUND"


def start(schema: str, pipeline: str) -> str:
    """Start a continuous pipeline in the background (a Kafka table)."""
    return f"START PIPELINE {_qualified(schema, pipeline)}"


def teardown(
    schema: str, pipeline: str, *, link: str | None = None, procedure: str | None = None
) -> list[str]:
    """The statements that remove a pipeline and what it owns, in order. ``DROP PIPELINE`` stops a
    running pipeline first. SingleStore has no ``DROP LINK IF EXISTS``: dropping an absent link
    fails with :data:`NO_SUCH_LINK`, which a caller tidying up after an unknown state tolerates."""
    out = [f"DROP PIPELINE IF EXISTS {_qualified(schema, pipeline)}"]
    if procedure:
        out.append(f"DROP PROCEDURE IF EXISTS {_qualified(schema, procedure)}")
    if link:
        out.append(f"DROP LINK {_qualified(schema, link)}")
    return out


#: SingleStore's error number for ``DROP LINK`` of a link that does not exist.
NO_SUCH_LINK = 2332


# -- continuous Kafka pipelines ----------------------------------------------------------------------

#: Every continuous Kafka pipeline Provisa runs starts with this; a one-shot file build's pipeline
#: (``provisa_pl_``) never does, so the sweep of Kafka pipelines can never touch a running build.
KAFKA_PIPELINE_PREFIX = "provisa_kp_"
_KAFKA_PROCEDURE_PREFIX = "provisa_kq_"

#: SingleStore's error number for ``START PIPELINE`` of a pipeline that is already running.
ALREADY_RUNNING = 1939


def kafka_definition(
    origin: LandOrigin,
    columns: list[tuple[str, str]],
    pk_columns: list[str],
    field_mapping: dict[str, str] | None,
    schema_registry: str | None,
    bootstrap: str,
) -> str:
    """A digest of everything a Kafka pipeline's DDL is made from. A table whose digest changes
    gets a new pipeline (and loses the old one), and an unchanged table keeps its running
    pipeline and its offsets."""
    body = json.dumps(
        [
            origin.location,
            origin.format,
            columns,
            sorted(pk_columns),
            sorted((field_mapping or {}).items()),
            schema_registry,
            bootstrap,
        ],
        sort_keys=True,
    )
    return hashlib.sha256(body.encode()).hexdigest()[:8]


def kafka_table_prefix(schema: str, table: str) -> str:
    """The name prefix every Kafka pipeline of ``schema.table`` carries, whatever its definition."""
    digest = hashlib.sha256(f"{schema}.{table}".encode()).hexdigest()[:16]
    return f"{KAFKA_PIPELINE_PREFIX}{digest}_"


def kafka_pipeline_name(schema: str, table: str, definition: str) -> str:
    """The continuous pipeline that lands ``schema.table`` with this ``definition`` digest."""
    return f"{kafka_table_prefix(schema, table)}{definition}"


def kafka_procedure_for(pipeline: str) -> str:
    """The stored procedure a JSON Kafka pipeline applies its batches through (one per pipeline)."""
    return _KAFKA_PROCEDURE_PREFIX + pipeline[len(KAFKA_PIPELINE_PREFIX) :]


def offsets_latest(schema: str, pipeline: str) -> str:
    """A new Kafka pipeline starts at the topic's latest offsets, as the relay consumer does
    (``auto_offset_reset="latest"``): it lands what is produced from now on."""
    return f"ALTER PIPELINE {_qualified(schema, pipeline)} SET OFFSETS LATEST"


def kafka_pipelines_query(schema: str) -> str:
    """The continuous Kafka pipelines Provisa runs in ``schema`` and their state, by name."""
    return (
        "SELECT PIPELINE_NAME, STATE FROM information_schema.PIPELINES "
        f"WHERE DATABASE_NAME = {_literal(schema)} "
        f"AND PIPELINE_NAME LIKE {_literal(KAFKA_PIPELINE_PREFIX.replace('_', chr(92) + '_') + '%')}"
    )


def batches_since_query(schema: str, pipelines: dict[str, int]) -> str:
    """The successful batches each of ``pipelines`` (name → last batch id already seen) has
    landed since, with the rows each changed."""
    if not pipelines:
        raise ValueError("no pipelines to read batches of")
    since = " OR ".join(
        f"(PIPELINE_NAME = {_literal(name)} AND BATCH_ID > {int(last)})"
        for name, last in sorted(pipelines.items())
    )
    return (
        "SELECT PIPELINE_NAME, BATCH_ID, ROWS_INSERTED, ROWS_UPDATED, ROWS_DELETED "
        "FROM information_schema.PIPELINES_BATCHES_SUMMARY "
        f"WHERE DATABASE_NAME = {_literal(schema)} AND BATCH_STATE = 'Succeeded' AND ({since}) "
        "ORDER BY PIPELINE_NAME, BATCH_ID"
    )
