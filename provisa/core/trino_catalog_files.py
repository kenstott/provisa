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

    from provisa.prometheus.source import endpoint_url

    m = source.mapping
    # One derivation of the server URL, shared with the native reader (REQ-1689 endpoint_url):
    # mapping.url wins; a host that is already a URL (the Sources form stores it that way —
    # demo/sources/prometheus/fragment.yaml) is taken as-is, never re-prefixed (REQ-1730: that
    # double prefix surfaced as "Error reading metrics"); a bare host is http://host:port. A bare
    # host passed straight through became prometheus.uri=<host>, which Trino rejects ("URI does not
    # have a scheme"). No URL at all is a misconfigured source and raises — no localhost default.
    url = endpoint_url(source.host, source.port, m)
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

    Only redis reads table-description files (``redis.table-description-dir``,
    :func:`with_catalog_dirs`). Trino's Elasticsearch connector has no such property: it reads each
    index's columns from the index mapping, so none is written for it. Prometheus auto-discovers
    metrics as tables and needs none either.
    """
    if source.type.value == "redis":
        from provisa.redis.source import generate_table_definitions

        return "redis", generate_table_definitions(_redis_config(source, resolved_password))
    return None


def write_table_definitions(
    source: Source, resolved_password: str, catalog: str, etc_dir: Path | None = None
) -> list[Path]:  # REQ-250, REQ-251, REQ-1992
    """Write per-table JSON table-description files; return the paths written.

    Under the source's own catalog (org-qualified: ``org_<id>__...``), so two orgs' tables of
    one name never share a file, and a catalog reads its own tables and no other's."""
    result = _table_definitions(source, resolved_password)
    if result is None:
        return []
    subdir, definitions = result
    target_dir = (etc_dir or trino_etc_dir()) / subdir / catalog
    target_dir.mkdir(parents=True, exist_ok=True)
    written: list[Path] = []
    for d in definitions:
        name = d.get("tableName") or d.get("name") or "table"
        path = target_dir / f"{name}.json"
        path.write_text(json.dumps(d, indent=2), encoding="utf-8")
        written.append(path)
    return written


def with_catalog_dirs(source: Source, props: dict[str, str], catalog: str) -> dict[str, str]:
    """``props`` reading the table descriptions of ``catalog`` alone (REQ-1992): the directory
    :func:`write_table_definitions` writes them to."""
    if source.type.value == "redis":
        from provisa.redis.source import TABLE_DESCRIPTION_DIR

        return {**props, "redis.table-description-dir": f"{TABLE_DESCRIPTION_DIR}/{catalog}"}
    return props


# --- Kafka catalog file generation (REQ-250) ---

#: Where the engine container reads a Kafka catalog's table descriptions and client properties,
#: each under its own catalog's name (REQ-1992).
_KAFKA_DESCRIPTIONS = "/etc/trino/kafka"
_KAFKA_CLIENT = "/etc/trino/kafka-client"


def _named_brokers(kafka_source: dict) -> str:
    """The brokers a Kafka source names — never a default one (REQ-812)."""
    if not kafka_source.get("bootstrap_servers"):
        raise ValueError(f"Kafka source {kafka_source.get('id')!r} names no bootstrap_servers")
    return kafka_source["bootstrap_servers"]


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
                    # REQ-1426: the declared type is the design; defaulting it to VARCHAR here
                    # would write a table description that disagrees with the registered column.
                    data_type=c["data_type"],
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
        bootstrap_servers=resolve_secrets(_named_brokers(kafka_source)),
        schema_registry_url=kafka_source.get("schema_registry_url"),
        topics=topics,
        auth=auth,
    )


def kafka_catalog_props(kafka_source: dict, catalog: str) -> dict[str, str]:  # REQ-147
    """Return the Trino kafka connector properties (minus ``connector.name``) for a
    ``kafka_sources[]`` entry, for a dynamic ``CREATE CATALOG ... USING kafka``.

    Reuses ``generate_trino_kafka_properties`` (the static-file content) as the
    single source of truth, parsed into a dict.
    """

    props: dict[str, str] = {}
    for line in generate_trino_kafka_properties(
        _kafka_source_config(kafka_source), catalog
    ).splitlines():
        line = line.strip()
        if not line or "=" not in line:
            continue
        key, value = line.split("=", 1)
        if key.strip() == "connector.name":
            continue  # supplied by USING kafka
        props[key.strip()] = value.strip()
    return props


# The user the Trino image runs as (trinodb/trino), the one reader of a Kafka client file.
TRINO_UID = 1000


def write_kafka_client_file(path: Path, text: str) -> None:  # REQ-147, REQ-2073
    """Write a Kafka catalog's client properties, which can hold a SASL password, for Trino alone.

    Trino's Kafka connector reads its client settings only from a file (``kafka.config.resources``),
    so this one file of a catalog is written. It is mode 0600 in a 0700 directory. Where the app runs
    as root (the app image) the file and directory are given to Trino's user; otherwise the app
    runs as that user itself (the development image, uid 1000) or beside a Trino of its own user,
    and they are already its. In the containerized stack the directory is a tmpfs shared with the
    engine (docker-compose.app.yml), so the file never reaches a disk.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    os.chmod(path.parent, 0o700)
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    try:
        # O_CREAT's mode applies to a new file only; one left by an earlier write is narrowed too.
        os.fchmod(fd, 0o600)
        # Only root can give a file away; os.geteuid exists on POSIX alone (a native Windows
        # process runs no Trino container to give it to).
        if os.name == "posix" and os.geteuid() == 0:
            os.fchown(fd, TRINO_UID, TRINO_UID)
            os.chown(path.parent, TRINO_UID, TRINO_UID)
        os.write(fd, text.encode("utf-8"))
    finally:
        os.close(fd)


def write_kafka_catalog_files(
    kafka_source: dict, catalog: str, etc_dir: Path | None = None, trino_conn=None
) -> list[Path]:  # REQ-147, REQ-250, REQ-1992
    """Generate the Kafka catalog ``.properties`` (+ client props) from config.

    ``kafka_source`` is a raw ``kafka_sources[]`` config entry; ``catalog`` its org-qualified
    catalog. Every file is named by the catalog, so two orgs' sources of one id never share one
    (the client properties can hold a password). Returns paths written. When ``trino_conn`` is given, also registers the catalog dynamically
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

    props_path = install_dir / f"{catalog}.properties"
    props_path.write_text(generate_trino_kafka_properties(cfg, catalog), encoding="utf-8")
    written.append(props_path)

    # No registry → FILE supplier: write per-topic table-description JSON from the
    # manual/sampled columns so Trino can read the topics without Confluent.
    if not cfg.schema_registry_url:
        kafka_dir = base / "kafka" / catalog
        kafka_dir.mkdir(parents=True, exist_ok=True)
        for d in generate_kafka_table_definitions(cfg):
            path = kafka_dir / f"{d['tableName']}.json"
            path.write_text(json.dumps(d, indent=2), encoding="utf-8")
            written.append(path)

    client_props = generate_kafka_client_properties(cfg)
    if client_props is not None:
        client_path = base / "kafka-client" / f"{catalog}.properties"
        write_kafka_client_file(client_path, client_props)
        written.append(client_path)

    if trino_conn is not None:
        from provisa.core.catalog import create_kafka_catalog

        create_kafka_catalog(trino_conn, kafka_source, catalog)

    return written


def generate_trino_kafka_properties(
    source: "KafkaSourceConfig", catalog: str
) -> str:  # REQ-147, REQ-250, REQ-1992
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
        lines.append(f"kafka.table-description-dir={_KAFKA_DESCRIPTIONS}/{catalog}")
        # Table names (sanitized) match the table-description tableName; the
        # description maps each back to its raw topicName.
        table_names = [t.table_name or t.topic for t in source.topics]
        if table_names:
            lines.append("kafka.table-names=" + ",".join(table_names))

    if isinstance(source.auth, (KafkaAuthSaslPlain, KafkaAuthSaslScram256, KafkaAuthSaslScram512)):
        lines.append(f"kafka.config.resources={_KAFKA_CLIENT}/{catalog}.properties")

    return "\n".join(lines)
