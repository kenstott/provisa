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
renders the DDL (:func:`s3_link_ddl`, :func:`file_pipeline_ddl`, :func:`kafka_pipeline_ddl`). Running
it belongs to the build and the push wiring.

Scope (maintainer, 2026-10-04):

- Kafka JSON and Avro land by pipeline. Kafka Protobuf never does; it keeps the Provisa relay.
- CSV and Parquet on S3 land by pipeline. On GCS or Azure they are refused by name until their
  credential model is decided. A local file never lands by pipeline (the database cannot read it);
  it keeps the relay.
- Iceberg on S3 lands by pipeline (a workspace must enable ``enable_iceberg_ingest``). Iceberg
  elsewhere, Delta Lake, Hive and every other type keep the relay.
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
    scheme = _scheme(origin.location)
    if scheme == "s3":
        return True
    if origin.source_type == "iceberg" or scheme == "":
        return False  # iceberg off S3, or a local file: the relay
    if scheme in _GCS_SCHEMES | _AZURE_SCHEMES:
        store = "GCS" if scheme in _GCS_SCHEMES else "Azure"
        raise PipelineRefused(
            f"{origin.source_type} on {store} ({origin.location}) cannot land into SingleStore yet: "
            f"no {store} credential model is decided for file sources"
        )
    return False


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


def _bucket_path(location: str) -> str:
    """``s3://bucket/key`` as a SingleStore S3 pipeline names it: ``bucket/key``."""
    return location.split("://", 1)[1]


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
    source = f"LOAD DATA LINK {_qualified(schema, link)} {_literal(_bucket_path(origin.location))}"
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
