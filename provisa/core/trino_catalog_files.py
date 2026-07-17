# Copyright (c) 2026 Kenneth Stott
# Canary: 94d828ff-b8af-4149-a8e5-4a6a7afa9128
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""Trino catalog + table-description file generation from Provisa config (REQ-250/251).

NoSQL/non-relational Trino connectors (redis, elasticsearch, prometheus, kafka) are
driven by a type-specific mapping DSL in the Provisa config, not hand-authored
``.properties``/table-description files. This module builds the typed source configs
from ``Source.mapping`` and routes to each connector's generator, and writes the
table-description JSON files into the Trino-mounted etc directory.
"""

from __future__ import annotations

import json
import os
from pathlib import Path

from typing import TYPE_CHECKING

from provisa.core.models import Source

if TYPE_CHECKING:
    from provisa.kafka.source import KafkaSourceConfig
from provisa.core.secrets import resolve_secrets

# Requirements: REQ-017, REQ-250, REQ-251

# Connector types whose catalog properties + table-defs come from the mapping DSL.
_MAPPING_DSL_TYPES = frozenset({"redis", "elasticsearch", "prometheus"})


def is_mapping_dsl_source(source: Source) -> bool:  # REQ-251
    return source.type.value in _MAPPING_DSL_TYPES


def trino_etc_dir() -> Path:
    """Root of the Trino etc tree Provisa writes connector files into.

    Overridable via ``PROVISA_TRINO_ETC_DIR`` (defaults to the repo ``trino`` dir).
    """
    return Path(os.environ.get("PROVISA_TRINO_ETC_DIR", "trino"))


# --- typed config builders (Source + Source.mapping -> connector dataclass) ---


def _redis_config(source: Source, resolved_password: str):
    from provisa.redis.source import RedisColumn, RedisSourceConfig, RedisTableConfig, ValueType

    tables = [
        RedisTableConfig(
            name=t["name"],
            key_pattern=t["key_pattern"],
            key_column=t.get("key_column", "key"),
            value_type=t.get("value_type", ValueType.HASH),
            columns=[
                RedisColumn(
                    name=c["name"], data_type=c.get("data_type", "VARCHAR"), field=c.get("field")
                )
                for c in t.get("columns", [])
            ],
        )
        for t in source.mapping.get("tables", [])
    ]
    return RedisSourceConfig(
        id=source.id,
        host=resolve_secrets(source.host or "localhost"),
        port=source.port or 6379,
        password=resolved_password or None,
        tables=tables,
    )


def _es_config(source: Source, resolved_password: str):
    from provisa.elasticsearch.source import ESColumn, ESSourceConfig, ESTableConfig

    m = source.mapping
    tables = [
        ESTableConfig(
            name=t["name"],
            index=t["index"],
            discover=t.get("discover", False),
            columns=[
                ESColumn(
                    name=c["name"], data_type=c.get("data_type", "VARCHAR"), path=c.get("path")
                )
                for c in t.get("columns", [])
            ],
        )
        for t in m.get("tables", [])
    ]
    return ESSourceConfig(
        id=source.id,
        host=resolve_secrets(source.host or "localhost"),
        port=source.port or 9200,
        tls=m.get("tls", False),
        auth_user=source.username or None,
        auth_password=resolved_password or None,
        tables=tables,
    )


def _prometheus_config(source: Source):
    from provisa.prometheus.source import PrometheusSourceConfig, PrometheusTableConfig

    m = source.mapping
    url = m.get("url") or (
        f"http://{source.host}:{source.port}" if source.host else "http://localhost:9090"
    )
    tables = [
        PrometheusTableConfig(
            name=t["name"],
            metric=t["metric"],
            labels_as_columns=t.get("labels_as_columns", []),
            value_column=t.get("value_column", "value"),
            default_range=t.get("default_range", "1h"),
        )
        for t in m.get("tables", [])
    ]
    return PrometheusSourceConfig(id=source.id, url=resolve_secrets(url), tables=tables)


# --- catalog properties routing (REQ-251) ---


def catalog_properties_for(
    source: Source, resolved_password: str
) -> dict[str, str] | None:  # REQ-251
    """Return connector catalog properties for a mapping-DSL source, else None.

    ``connector.name`` is stripped: a dynamic ``CREATE CATALOG ... USING <connector>``
    sets the connector, and Trino rejects ``connector.name`` in the WITH clause.
    """
    stype = source.type.value
    if stype == "redis":
        from provisa.redis.source import generate_catalog_properties

        props = generate_catalog_properties(_redis_config(source, resolved_password))
    elif stype == "elasticsearch":
        from provisa.elasticsearch.source import generate_catalog_properties

        props = generate_catalog_properties(_es_config(source, resolved_password))
    elif stype == "prometheus":
        from provisa.prometheus.source import generate_catalog_properties

        props = generate_catalog_properties(_prometheus_config(source))
    else:
        return None
    return {k: v for k, v in props.items() if k != "connector.name"}


# --- table-description file writing (REQ-250/251) ---


def _table_definitions(source: Source, resolved_password: str) -> tuple[str, list[dict]] | None:
    """Return (connector_subdir, table-definition dicts) for a mapping-DSL source.

    Only redis/elasticsearch use on-disk table-description files; prometheus
    auto-discovers metrics as tables and needs none.
    """
    stype = source.type.value
    if stype == "redis":
        from provisa.redis.source import generate_table_definitions

        return "redis", generate_table_definitions(_redis_config(source, resolved_password))
    if stype == "elasticsearch":
        from provisa.elasticsearch.source import generate_table_definitions

        return "elasticsearch", generate_table_definitions(_es_config(source, resolved_password))
    return None


def write_table_definitions(
    source: Source, resolved_password: str, etc_dir: Path | None = None
) -> list[Path]:  # REQ-250, REQ-251
    """Write per-table JSON table-description files; return the paths written."""
    result = _table_definitions(source, resolved_password)
    if result is None:
        return []
    subdir, definitions = result
    target_dir = (etc_dir or trino_etc_dir()) / subdir
    target_dir.mkdir(parents=True, exist_ok=True)
    written: list[Path] = []
    for d in definitions:
        name = d.get("tableName") or d.get("name") or "table"
        path = target_dir / f"{name}.json"
        path.write_text(json.dumps(d, indent=2), encoding="utf-8")
        written.append(path)
    return written


# --- Kafka catalog file generation (REQ-250) ---


def _kafka_source_config(kafka_source: dict):
    """Build a KafkaSourceConfig from a raw ``kafka_sources[]`` config entry."""
    from pydantic import TypeAdapter

    from provisa.core.auth_models import KafkaAuth
    from provisa.kafka.source import (
        KafkaColumn,
        KafkaSourceConfig,
        KafkaTopicConfig,
        SchemaSource,
        ValueFormat,
    )

    auth_raw = kafka_source.get("auth")
    auth = TypeAdapter(KafkaAuth).validate_python(auth_raw) if auth_raw else None
    topics = [
        KafkaTopicConfig(
            id=t.get("id", t["topic"]),
            topic=t["topic"],
            source_id=kafka_source["id"],
            schema_source=SchemaSource(t.get("schema_source", "registry")),
            value_format=ValueFormat(t.get("value_format", "json")),
            columns=[
                KafkaColumn(
                    name=c["name"],
                    data_type=c.get("data_type", "VARCHAR"),
                    is_complex=c.get("is_complex", False),
                )
                for c in t.get("columns", [])
            ],
            table_name=t.get("table_name"),
        )
        for t in kafka_source.get("topics", [])
    ]
    return KafkaSourceConfig(
        id=kafka_source["id"],
        bootstrap_servers=resolve_secrets(kafka_source.get("bootstrap_servers", "localhost:9092")),
        schema_registry_url=kafka_source.get("schema_registry_url"),
        topics=topics,
        auth=auth,
    )


def kafka_catalog_props(kafka_source: dict) -> dict[str, str]:  # REQ-147
    """Return the Trino kafka connector properties (minus ``connector.name``) for a
    ``kafka_sources[]`` entry, for a dynamic ``CREATE CATALOG ... USING kafka``.

    Reuses ``generate_trino_kafka_properties`` (the static-file content) as the
    single source of truth, parsed into a dict.
    """

    props: dict[str, str] = {}
    for line in generate_trino_kafka_properties(_kafka_source_config(kafka_source)).splitlines():
        line = line.strip()
        if not line or "=" not in line:
            continue
        key, value = line.split("=", 1)
        if key.strip() == "connector.name":
            continue  # supplied by USING kafka
        props[key.strip()] = value.strip()
    return props


def write_kafka_catalog_files(
    kafka_source: dict, etc_dir: Path | None = None, trino_conn=None
) -> list[Path]:  # REQ-147, REQ-250
    """Generate the Kafka catalog ``.properties`` (+ client props) from config.

    ``kafka_source`` is a raw ``kafka_sources[]`` config entry. Returns paths
    written. When ``trino_conn`` is given, also registers the catalog dynamically
    (CREATE CATALOG) so it loads regardless of Trino start order — the static
    files are staged under ``catalog-install/`` and not auto-loaded by a
    ``catalog.management=dynamic`` Trino.
    """
    from provisa.kafka.source import (
        generate_kafka_client_properties,
        generate_kafka_table_definitions,
    )

    cfg = _kafka_source_config(kafka_source)
    base = etc_dir or trino_etc_dir()
    install_dir = base / "catalog-install"
    install_dir.mkdir(parents=True, exist_ok=True)
    written: list[Path] = []

    props_path = install_dir / f"{cfg.id.replace('-', '_')}.properties"
    props_path.write_text(generate_trino_kafka_properties(cfg), encoding="utf-8")
    written.append(props_path)

    # No registry → FILE supplier: write per-topic table-description JSON from the
    # manual/sampled columns so Trino can read the topics without Confluent.
    if not cfg.schema_registry_url:
        kafka_dir = base / "kafka"
        kafka_dir.mkdir(parents=True, exist_ok=True)
        for d in generate_kafka_table_definitions(cfg):
            path = kafka_dir / f"{d['tableName']}.json"
            path.write_text(json.dumps(d, indent=2), encoding="utf-8")
            written.append(path)

    client_props = generate_kafka_client_properties(cfg)
    if client_props is not None:
        client_path = base / "kafka-client.properties"
        client_path.write_text(client_props, encoding="utf-8")
        written.append(client_path)

    if trino_conn is not None:
        from provisa.core.catalog import create_kafka_catalog

        create_kafka_catalog(trino_conn, kafka_source)

    return written


def generate_trino_kafka_properties(source: "KafkaSourceConfig") -> str:  # REQ-147, REQ-250
    """Generate Trino Kafka connector properties file content.

    Returns the content for a kafka.properties file to be placed
    in Trino's catalog directory.
    """
    from provisa.core.auth_models import (
        KafkaAuthSaslPlain,
        KafkaAuthSaslScram256,
        KafkaAuthSaslScram512,
    )

    lines = [
        "connector.name=kafka",
        f"kafka.nodes={source.bootstrap_servers}",
        "kafka.hide-internal-columns=false",
    ]

    # REQ-250: Confluent is optional. Use the schema registry only when one is
    # configured; otherwise use FILE table descriptions generated from the topic's
    # manual columns (or a sampled layout) — no Confluent dependency.
    if source.schema_registry_url:
        lines.append("kafka.table-description-supplier=CONFLUENT")
        lines.append(f"kafka.confluent-schema-registry-url={source.schema_registry_url}")
    else:
        lines.append("kafka.table-description-supplier=FILE")
        lines.append("kafka.table-description-dir=/etc/trino/kafka")
        # Table names (sanitized) match the table-description tableName; the
        # description maps each back to its raw topicName.
        table_names = [t.table_name or t.topic for t in source.topics]
        if table_names:
            lines.append("kafka.table-names=" + ",".join(table_names))

    if isinstance(source.auth, (KafkaAuthSaslPlain, KafkaAuthSaslScram256, KafkaAuthSaslScram512)):
        lines.append("kafka.config.resources=/etc/trino/kafka-client.properties")

    return "\n".join(lines)
