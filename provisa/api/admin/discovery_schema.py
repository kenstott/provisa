# Copyright (c) 2026 Kenneth Stott
# Canary: 8f2a1b3c-d4e5-6789-abcd-ef0123456789
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Admin router for NoSQL/non-relational schema discovery (Phase AI8).

POST /admin/discover/{source_id} — introspects the source via its adapter's
discover_schema() and returns candidate columns.
"""

# Requirements: REQ-017, REQ-252

from __future__ import annotations

# complexity-gate: allow-ble=1 reason="the Elasticsearch mapping fetch catches the driver/transport error only to re-raise it as an HTTP 502 with the index name — it translates, never swallows; the exception always propagates to the caller"

import logging
from typing import TYPE_CHECKING, Protocol, cast

from fastapi import APIRouter
from pydantic import BaseModel
from sqlalchemy import select

from provisa.api.errors import ApiError

from provisa.core.schema_org import sources
from provisa.source_adapters.registry import get_adapter

if TYPE_CHECKING:
    from provisa.core.database import Connection


class _SampleableDriver(Protocol):
    def sample_documents(self, collection: str, limit: int) -> list[dict]: ...


log = logging.getLogger(__name__)

router = APIRouter(prefix="/admin/schema-discovery", tags=["schema-discovery"])

# Source types that do not support schema discovery
_NO_DISCOVER = {"redis"}


class DiscoveredUniqueConstraint(BaseModel):  # REQ-1093
    name: str
    columns: list[str]


class UniqueConstraintsResponse(BaseModel):  # REQ-1093
    source_id: str
    unique_constraints: list[DiscoveredUniqueConstraint]


@router.get("/unique-constraints/{source_id}", response_model=UniqueConstraintsResponse)
async def get_unique_constraints(
    source_id: str, schema: str, table: str
) -> UniqueConstraintsResponse:  # REQ-1093
    """Introspect declared UNIQUE constraints for one (schema, table) on an RDB source.

    Seeds the register/edit "Uniques" panel. Returns an empty list when the source
    exposes none or does not support constraint introspection — uniqueness is never
    inferred from data.
    """
    from provisa.api.app import state
    from provisa.discovery.fk_introspect import introspect_unique_constraints

    if state.tenant_db is None:
        raise ApiError(503, "discovery.database_not_connected", "Database not connected")
    async with state.tenant_db.acquire() as conn:
        conn = cast("Connection", conn)
        result = await conn.execute_core(select(sources.c.type).where(sources.c.id == source_id))
        fetched = result.fetchone()
    if fetched is None:
        raise ApiError(
            404,
            "discovery.source_not_found",
            f"Source '{source_id}' not found",
            source_id=source_id,
        )
    source_type = fetched._mapping["type"]
    raw = await introspect_unique_constraints(
        state.source_pools, source_type, source_id, schema, table
    )
    return UniqueConstraintsResponse(
        source_id=source_id,
        unique_constraints=[
            DiscoveredUniqueConstraint(name=u["name"], columns=u["columns"]) for u in raw
        ],
    )


@router.get("/ir-types", response_model=list[str])
async def list_ir_types() -> list[str]:
    """The canonical IR data-type vocabulary (REQ-846) — the type names the UI offers when a steward
    assigns a column's type during schema discovery, so an assigned type is engine-independent (the
    landing write face maps IR → the store's physical type). Sorted for a stable dropdown order."""
    from provisa.core.ir_types import IR_TYPES

    return sorted(IR_TYPES)


def _get_source_pool():
    """Return the current source_pools from app state."""
    from provisa.api.app import state

    return state.source_pools


class DiscoveredColumn(BaseModel):
    name: str
    type: str
    nullable: bool = True
    description: str = ""
    source_path: str = ""


class DiscoverResponse(BaseModel):
    source_id: str
    source_type: str
    columns: list[DiscoveredColumn]
    unique_constraints: list[DiscoveredUniqueConstraint] = []  # REQ-1093


class DiscoverRequest(BaseModel):
    """Optional hints for discovery — e.g. collection name, index, keyspace."""

    collection: str | None = None
    index: str | None = None
    keyspace: str | None = None
    table: str | None = None
    schema_name: str | None = None  # REQ-1093: schema for UNIQUE-constraint introspection
    metric: str | None = None
    sample_limit: int = 100
    # REQ-1767: kafka discovery hints. schema_registry_url overrides the source's own
    # kafka_sources.schema_registry_url (REQ-147) when set — kafka_topics/kafka_sources currently
    # have no writer anywhere in this codebase (verified), so a dynamically-registered kafka
    # source never has one stored; the hint lets discovery work today without that separate,
    # larger gap being closed first.
    topic: str | None = None
    value_format: str | None = None
    schema_registry_url: str | None = None
    # REQ-150: kafka SchemaSource.SAMPLE mode. Overrides the source's own host:port as the
    # broker address to sample from when set; sample_limit (above, already generic) doubles as
    # the max-records-to-consume bound for this mode.
    bootstrap_servers: str | None = None


@router.post("/discover/{source_id}", response_model=DiscoverResponse)
async def discover_source_schema(
    source_id: str, body: DiscoverRequest | None = None
):  # REQ-017, REQ-252
    """Look up source, call adapter.discover_schema(), return columns."""
    from provisa.api.app import state

    if state.tenant_db is None:
        raise ApiError(503, "discovery.database_not_connected", "Database not connected")

    # Fetch source record from DB
    async with state.tenant_db.acquire() as conn:
        conn = cast("Connection", conn)
        result = await conn.execute_core(select(sources).where(sources.c.id == source_id))
        fetched = result.fetchone()
        row = dict(fetched._mapping) if fetched is not None else None

    if row is None:
        raise ApiError(
            404,
            "discovery.source_not_found",
            f"Source '{source_id}' not found",
            source_id=source_id,
        )

    source_type = row["type"]

    if source_type in _NO_DISCOVER:
        raise ApiError(
            400,
            "discovery.schema_discovery_unsupported",
            f"Source type '{source_type}' does not support schema discovery. "
            "Define columns manually.",
            source_type=source_type,
        )

    try:
        adapter = get_adapter(source_type)
    except KeyError:
        raise ApiError(
            400,
            "discovery.no_adapter_for_source_type",
            f"No adapter registered for source type '{source_type}'",
            source_type=source_type,
        )

    if not hasattr(adapter, "discover_schema"):
        raise ApiError(
            400,
            "discovery.adapter_missing_discover_schema",
            f"Adapter for '{source_type}' does not implement discover_schema",
            source_type=source_type,
        )

    hints = body or DiscoverRequest()

    # Build adapter-specific discovery args from source record + hints
    raw_columns = await _call_discover(adapter, source_type, row, hints)

    columns = [
        DiscoveredColumn(
            name=col.get("name", ""),
            type=col.get("type", "VARCHAR"),
            nullable=col.get("nullable", True),
            description=col.get("description", ""),
            source_path=col.get("sourcePath", col.get("source_path", "")),
        )
        for col in raw_columns
    ]

    # REQ-1093: seed declared UNIQUE constraints for the register/edit "Uniques" panel.
    # Only for RDB sources with a known table; introspection reads the live source constraints.
    unique_constraints: list[DiscoveredUniqueConstraint] = []
    if hints.table and hints.schema_name:
        from provisa.discovery.fk_introspect import introspect_unique_constraints

        raw_uniques = await introspect_unique_constraints(
            state.source_pools, source_type, source_id, hints.schema_name, hints.table
        )
        unique_constraints = [
            DiscoveredUniqueConstraint(name=u["name"], columns=u["columns"]) for u in raw_uniques
        ]

    return DiscoverResponse(
        source_id=source_id,
        source_type=source_type,
        columns=columns,
        unique_constraints=unique_constraints,
    )


async def _call_discover(
    adapter, source_type: str, row, hints: DiscoverRequest
) -> list[dict]:  # REQ-017, REQ-252
    """Dispatch to the correct adapter.discover_schema() signature.

    async (REQ-1767): kafka's branch below is the first adapter here whose real fetch is a
    network call through an async client (httpx via schema_registry.py's SchemaRegistryClient) —
    every other branch stays synchronous internally, only the function signature changed."""
    if source_type == "mongodb":
        # MongoDB discover_schema requires sample documents from a live connection.
        # Check source_pools for an active connection; raise 503 if none exists.
        source_pools = _get_source_pool()
        source_id = row["id"]
        if not source_pools.has(source_id):
            raise ApiError(
                503,
                "discovery.no_live_connection",
                (
                    f"No live connection for source '{source_id}'. "
                    "MongoDB schema discovery requires an active connection in the source pool. "
                    "Verify the source is connected and the server is reachable."
                ),
                source_id=source_id,
            )
        driver = cast(_SampleableDriver, source_pools.get(source_id))
        collection = hints.collection or "default"
        sample_docs = driver.sample_documents(
            collection=collection,
            limit=hints.sample_limit,
        )
        return adapter.discover_schema(sample_docs, collection)

    if source_type == "elasticsearch":
        # REQ-252: fetch the live index mapping via GET /<index>/_mapping. No index hint or a
        # transport error raises — discovery must never silently produce empty columns.
        index = hints.index
        if not index:
            raise ApiError(
                400,
                "discovery.elasticsearch_index_hint_required",
                "Elasticsearch discovery requires an 'index' hint.",
            )
        try:
            properties = adapter.fetch_index_mapping(row["host"], int(row["port"]), index)
        except Exception as e:
            raise ApiError(
                502,
                "discovery.elasticsearch_mapping_failed",
                f"Failed to read Elasticsearch mapping for index {index!r}: {e}",
                index=index,
                error=str(e),
            )
        return adapter.discover_schema(properties)

    if source_type == "cassandra":
        # REQ-1676: the cluster's schema metadata over CQL (the ``cassandra`` extra). A keyspace
        # and table hint name what to describe; a transport error raises — never empty columns.
        from provisa.cassandra.fetch import CassandraConnection, table_metadata

        keyspace, table = hints.keyspace, hints.table
        if not keyspace or not table:
            raise ApiError(
                400,
                "discovery.cassandra_hints_required",
                "Cassandra discovery requires 'keyspace' and 'table' hints.",
            )
        try:
            meta = table_metadata(
                CassandraConnection.build(
                    row["host"], int(row["port"]), username=row.get("username")
                ),
                keyspace,
                table,
            )
        except Exception as e:
            raise ApiError(
                502,
                "discovery.cassandra_metadata_failed",
                f"Failed to read Cassandra metadata for {keyspace}.{table}: {e}",
                keyspace=keyspace,
                table=table,
                error=str(e),
            )
        return adapter.discover_schema(meta)

    if source_type == "prometheus":
        # REQ-1689: the metric's live labels and metadata type over the HTTP API. No metric hint
        # or a transport error raises — discovery must never silently produce empty columns.
        from provisa.prometheus.fetch import PrometheusConnection, metric_columns
        from provisa.prometheus.source import endpoint_url

        metric = hints.metric
        if not metric:
            raise ApiError(
                400,
                "discovery.prometheus_metric_hint_required",
                "Prometheus discovery requires a 'metric' hint.",
            )
        try:
            mapping = row.get("mapping") or {}
            conn = PrometheusConnection.build(
                endpoint_url(row.get("host"), row.get("port"), mapping)
            )
            return metric_columns(conn, metric)
        except Exception as e:
            raise ApiError(
                502,
                "discovery.prometheus_metadata_failed",
                f"Failed to read Prometheus metadata for metric {metric!r}: {e}",
                metric=metric,
                error=str(e),
            )

    if source_type == "pinot":
        # REQ-1730: a table hint (required) plus a live query against the broker — see
        # provisa.pinot.fetch's own module doc for the controller-vs-broker address split.
        from provisa.pinot.fetch import PinotConnection, table_columns

        table = hints.table
        if not table:
            raise ApiError(
                400,
                "discovery.pinot_table_hint_required",
                "Pinot discovery requires a 'table' hint.",
            )
        try:
            conn = PinotConnection.build(
                row.get("host"),
                row.get("port"),
                (row.get("federation_hints") or {}).get("pinot_broker_url"),
            )
            columns = table_columns(conn, table)
        except Exception as e:
            raise ApiError(
                502,
                "discovery.pinot_metadata_failed",
                f"Failed to read Pinot schema for table {table!r}: {e}",
                table=table,
                error=str(e),
            )
        return adapter.discover_schema(columns)

    if source_type == "druid":
        # REQ-1730: a table hint (required) plus a live INFORMATION_SCHEMA.COLUMNS query — see
        # provisa.druid.fetch's own module doc.
        from provisa.druid.fetch import DruidConnection, table_columns

        table = hints.table
        if not table:
            raise ApiError(
                400,
                "discovery.druid_table_hint_required",
                "Druid discovery requires a 'table' hint.",
            )
        try:
            conn = DruidConnection.build(row.get("host"), row.get("port"))
            columns = table_columns(conn, table)
        except Exception as e:
            raise ApiError(
                502,
                "discovery.druid_metadata_failed",
                f"Failed to read Druid schema for table {table!r}: {e}",
                table=table,
                error=str(e),
            )
        return adapter.discover_schema(columns)

    if source_type == "hive_s3":
        # REQ-1730: schema + table hints (required) plus a live DESCRIBE over read_parquet — see
        # provisa.hive.fetch's own module doc.
        from provisa.hive.fetch import HiveS3Connection, table_columns

        schema, table = hints.schema_name, hints.table
        if not schema or not table:
            raise ApiError(
                400,
                "discovery.hive_s3_hints_required",
                "hive_s3 discovery requires 'schema_name' and 'table' hints.",
            )
        try:
            mapping = row.get("mapping") or {}
            conn = HiveS3Connection.build(row.get("database"), mapping)
            columns = table_columns(conn, schema, table)
        except Exception as e:
            raise ApiError(
                502,
                "discovery.hive_s3_metadata_failed",
                f"Failed to read hive_s3 schema for {schema}.{table}: {e}",
                schema=schema,
                table=table,
                error=str(e),
            )
        return adapter.discover_schema(columns)

    if source_type == "kafka":
        # REQ-1767: wires provisa.kafka.schema_registry's SchemaRegistryClient (REQ-116/147/150,
        # previously built but with zero callers anywhere) into the same discover/edit/register
        # flow mongodb/elasticsearch/cassandra/prometheus already use — a topic hint (required)
        # and value_format/schema_registry_url hints (both optional). Does NOT write to
        # kafka_topics/kafka_sources (REQ-147's catalog, which nothing in this codebase writes to
        # at all today — a separate, larger gap, not closed by this task): the discovered columns
        # flow straight into SchemaDiscovery.tsx's own edit-then-registerTable step, the same as
        # every other adapter here.
        from provisa.kafka.schema_registry import discover_topic_columns
        from provisa.kafka.source import ValueFormat

        topic = hints.topic
        if not topic:
            raise ApiError(
                400,
                "discovery.kafka_topic_hint_required",
                "Kafka discovery requires a 'topic' hint.",
            )
        # REQ-1730: source.database is the persistent Schema Registry URL field on the kafka
        # Sources form (SourceFormFieldsExtended.tsx's isKafka block) — the same one
        # TrinoKafkaConnector.details() reads to build its CONFLUENT catalog properties. Falling
        # back to it here means the one field an operator fills in covers both engines' discovery,
        # instead of requiring federation_hints.schema_registry_url (no persistent UI writer) or a
        # transient per-Discover-call hint to be set again for DuckDB alone.
        registry_url = (
            hints.schema_registry_url
            or (row.get("federation_hints") or {}).get("schema_registry_url")
            or row.get("database")
        )
        if registry_url:
            value_format = (
                ValueFormat(hints.value_format) if hints.value_format else ValueFormat.JSON
            )
            try:
                columns = await discover_topic_columns(registry_url, topic, value_format)
            except Exception as e:
                raise ApiError(
                    502,
                    "discovery.kafka_schema_registry_failed",
                    f"Failed to discover schema for topic {topic!r} from Schema Registry: {e}",
                    topic=topic,
                    error=str(e),
                )
            return adapter.discover_schema(columns)

        # REQ-150: no Schema Registry URL (hint or source-stored) — SchemaSource.SAMPLE instead
        # of SchemaSource.REGISTRY. Same architectural shape as mongodb's sample_documents branch
        # above: consume real messages off the live topic and infer column types from their
        # actual shape, rather than querying a registry. bootstrap_servers comes from a hint
        # override, else the source's own host:port (provisa/events/push_wiring.py's own
        # `f"{src.host}:{src.port}" if src.port else src.host` convention for this source type).
        from provisa.kafka.source import infer_columns_from_records, sample_topic_records

        bootstrap_servers = hints.bootstrap_servers or (
            f"{row['host']}:{row['port']}" if row.get("port") else row.get("host")
        )
        if not bootstrap_servers:
            raise ApiError(
                400,
                "discovery.kafka_bootstrap_servers_required",
                "Kafka sample-mode discovery requires a reachable broker — pass a "
                "bootstrap_servers hint or set host/port on the source.",
            )
        try:
            records = await sample_topic_records(
                bootstrap_servers, topic, max_records=hints.sample_limit
            )
        except Exception as e:
            raise ApiError(
                502,
                "discovery.kafka_sample_failed",
                f"Failed to sample topic {topic!r} for schema inference: {e}",
                topic=topic,
                error=str(e),
            )
        columns = infer_columns_from_records(records)
        return adapter.discover_schema(columns)

    # Fallback: try calling with no args
    try:
        return adapter.discover_schema()
    except TypeError:
        return []
