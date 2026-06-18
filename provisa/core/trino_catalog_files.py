# Copyright (c) 2026 Kenneth Stott
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

from provisa.core.models import Source
from provisa.core.secrets import resolve_secrets

# Connector types whose catalog properties + table-defs come from the mapping DSL.
_MAPPING_DSL_TYPES = frozenset({"redis", "elasticsearch", "prometheus"})


def is_mapping_dsl_source(source: Source) -> bool:
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
                RedisColumn(name=c["name"], data_type=c.get("data_type", "VARCHAR"), field=c.get("field"))
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
                ESColumn(name=c["name"], data_type=c.get("data_type", "VARCHAR"), path=c.get("path"))
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
    url = m.get("url") or (f"http://{source.host}:{source.port}" if source.host else "http://localhost:9090")
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


def catalog_properties_for(source: Source, resolved_password: str) -> dict[str, str] | None:
    """Return connector catalog properties for a mapping-DSL source, else None."""
    stype = source.type.value
    if stype == "redis":
        from provisa.redis.source import generate_catalog_properties

        return generate_catalog_properties(_redis_config(source, resolved_password))
    if stype == "elasticsearch":
        from provisa.elasticsearch.source import generate_catalog_properties

        return generate_catalog_properties(_es_config(source, resolved_password))
    if stype == "prometheus":
        from provisa.prometheus.source import generate_catalog_properties

        return generate_catalog_properties(_prometheus_config(source))
    return None


# --- table-description file writing (REQ-250/251) ---


def _table_definitions(source: Source, resolved_password: str) -> tuple[str, list[dict]] | None:
    """Return (connector_subdir, table-definition dicts) for a mapping-DSL source."""
    stype = source.type.value
    if stype == "redis":
        from provisa.redis.source import generate_table_definitions

        return "redis", generate_table_definitions(_redis_config(source, resolved_password))
    if stype == "elasticsearch":
        from provisa.elasticsearch.source import generate_table_definitions

        return "elasticsearch", generate_table_definitions(_es_config(source, resolved_password))
    if stype == "prometheus":
        from provisa.prometheus.source import generate_table_definitions

        return "prometheus", generate_table_definitions(_prometheus_config(source))
    return None


def write_table_definitions(
    source: Source, resolved_password: str, etc_dir: Path | None = None
) -> list[Path]:
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


def write_kafka_catalog_files(kafka_source: dict, etc_dir: Path | None = None) -> list[Path]:
    """Generate the Kafka catalog ``.properties`` (+ client props) from config.

    ``kafka_source`` is a raw ``kafka_sources[]`` config entry. Returns paths written.
    """
    from pydantic import TypeAdapter

    from provisa.core.auth_models import KafkaAuth
    from provisa.kafka.source import (
        KafkaSourceConfig,
        generate_kafka_client_properties,
        generate_trino_kafka_properties,
    )

    auth_raw = kafka_source.get("auth")
    auth = TypeAdapter(KafkaAuth).validate_python(auth_raw) if auth_raw else None
    cfg = KafkaSourceConfig(
        id=kafka_source["id"],
        bootstrap_servers=resolve_secrets(kafka_source.get("bootstrap_servers", "localhost:9092")),
        schema_registry_url=kafka_source.get("schema_registry_url"),
        topics=[],
        auth=auth,
    )
    base = etc_dir or trino_etc_dir()
    install_dir = base / "catalog-install"
    install_dir.mkdir(parents=True, exist_ok=True)
    written: list[Path] = []

    props_path = install_dir / f"{cfg.id.replace('-', '_')}.properties"
    props_path.write_text(generate_trino_kafka_properties(cfg), encoding="utf-8")
    written.append(props_path)

    client_props = generate_kafka_client_properties(cfg)
    if client_props is not None:
        client_path = base / "kafka-client.properties"
        client_path.write_text(client_props, encoding="utf-8")
        written.append(client_path)

    return written
