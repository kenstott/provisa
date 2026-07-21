# Copyright (c) 2026 Kenneth Stott
# Canary: 2ae8ef6d-2550-4cb3-bd42-e938c6f76e26
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Pure row-mapper and input-mapper helpers extracted from schema.py."""

# complexity-gate: allow-ble=1 reason="grandfathered bare-except in _parse_mapping_json relocated from schema.py; JSON decode errors are intentionally swallowed here"

from __future__ import annotations

from provisa.api.admin.types import (
    DomainType,
    RelationshipType,
    RLSRuleType,
    RoleRateLimitType,
    RoleType,
    SourceCdcConfigType,
    SourceInput,
    SourceType,
)
from provisa.api.admin.db_queries import derive_graphql_alias as _derive_graphql_alias_fn
from provisa.cypher.label_map import _to_rel_type as _to_cypher_rel_type


def _parse_mapping_json(mapping_json: str | None) -> dict:
    if not mapping_json:
        return {}
    import json as _json

    try:
        return _json.loads(mapping_json)
    except Exception:
        return {}


def _cdc_from_row(row):  # REQ-824
    """Deserialize the sources.cdc JSONB column into a SourceCdcConfigType."""
    import json as _json

    raw = row.get("cdc")
    if not raw:
        return None
    data = _json.loads(raw) if isinstance(raw, str) else raw
    return SourceCdcConfigType(
        bootstrap_servers=data["bootstrap_servers"],
        topic_prefix=data["topic_prefix"],
        schema_registry_url=data.get("schema_registry_url"),
        consumer_group_id=data.get("consumer_group_id", "provisa-debezium"),
    )


def _cdc_model_from_input(input: SourceInput):  # REQ-824
    """Map SourceCdcConfigInput → core SourceCdcConfig model, or None when absent."""
    from provisa.core.models import SourceCdcConfig

    if input.cdc is None:
        return None
    return SourceCdcConfig(
        bootstrap_servers=input.cdc.bootstrap_servers,
        topic_prefix=input.cdc.topic_prefix,
        schema_registry_url=input.cdc.schema_registry_url,
        consumer_group_id=input.cdc.consumer_group_id,
    )


def _source_from_row(row) -> SourceType:
    import json as _json

    raw_mapping = row.get("mapping") or {}
    mapping_json = _json.dumps(raw_mapping) if isinstance(raw_mapping, dict) else str(raw_mapping)
    return SourceType(
        id=row["id"],
        type=row["type"],
        host=row["host"],
        port=row["port"],
        database=row["database"],
        username=row["username"],
        dialect=row["dialect"],
        cache_enabled=row.get("cache_enabled", True),
        cache_ttl=row.get("cache_ttl"),
        prefer_materialized=bool(row.get("prefer_materialized", False)),
        load_protected=bool(row.get("load_protected", False)),  # REQ-1141
        off_peak_window=row.get("off_peak_window"),  # REQ-1141
        off_peak_tz=row.get("off_peak_tz") or "UTC",  # REQ-1141
        gql_naming_convention=row.get("gql_naming_convention"),
        path=row.get("path"),
        allowed_domains=list(row.get("allowed_domains") or []),
        description=row.get("description") or "",
        mapping_json=mapping_json,
        change_signal=row.get("change_signal") or "ttl",  # REQ-929
        cdc=_cdc_from_row(row),
    )


def _domain_from_row(row) -> DomainType:
    return DomainType(
        id=row["id"], description=row["description"], graphql_alias=row["graphql_alias"]
    )


def _role_from_row(row) -> RoleType:
    # REQ-1174: surface the per-role rate + query-complexity limits (JSON column) to the admin API.
    rl = row.get("rate_limit")
    rate_limit = None
    if isinstance(rl, dict):
        rate_limit = RoleRateLimitType(
            requests_per_second=rl.get("requests_per_second"),
            max_query_depth=rl.get("max_query_depth"),
            max_query_nodes=rl.get("max_query_nodes"),
            max_query_time_ms=rl.get("max_query_time_ms"),
        )
    return RoleType(
        id=row["id"],
        capabilities=list(row["capabilities"]),
        domain_access=list(row["domain_access"]),
        rate_limit=rate_limit,
    )


def _derive_graphql_alias(  # pyright: ignore[reportUnusedParameter]
    target_table_name: str, cardinality: str, _alias: str | None, convention: str = "apollo_graphql"
) -> str | None:
    return _derive_graphql_alias_fn(target_table_name, cardinality, convention)


def _rel_from_row(row, convention: str = "apollo_graphql") -> RelationshipType:
    cardinality = row["cardinality"]
    target_table_name = row.get("target_table_name") or ""
    source_column = row.get("source_column") or ""
    alias = row.get("alias")
    persisted_graphql_alias = row.get("graphql_alias") or None
    graphql_alias = persisted_graphql_alias or _derive_graphql_alias(
        target_table_name, cardinality, alias, convention
    )
    computed_cypher_alias = (
        None
        if alias
        else _to_cypher_rel_type(graphql_alias or target_table_name or "", cardinality)
    )
    return RelationshipType(
        id=row["id"],
        source_table_id=row["source_table_id"],
        target_table_id=row.get("target_table_id"),
        source_table_name=row.get("source_table_name", ""),
        source_domain_id=row.get("source_domain_id") or "",
        target_table_name=target_table_name,
        source_column=source_column,
        target_column=row.get("target_column"),
        cardinality=cardinality,
        materialize=row.get("materialize", False),
        refresh_interval=row.get("refresh_interval", 300),
        target_function_name=row.get("target_function_name"),
        function_arg=row.get("function_arg"),
        alias=alias,
        graphql_alias=graphql_alias,
        computed_cypher_alias=computed_cypher_alias,
        disable_cypher=row.get("disable_cypher", False),
    )


def _rls_from_row(row) -> RLSRuleType:
    return RLSRuleType(
        id=row["id"],
        table_id=row["table_id"],
        domain_id=row["domain_id"],
        role_id=row["role_id"],
        filter_expr=row["filter_expr"],
    )


def _live_strategy_from_raw(raw: dict) -> str:  # REQ-813
    """Read live.strategy, mapping the legacy delivery=poll|cdc field for back-compat."""
    if raw.get("strategy"):
        return raw["strategy"]
    # Legacy rows: delivery=poll → poll; delivery=cdc → native (PG/Mongo push).
    return "poll" if raw.get("delivery", "poll") == "poll" else "native"


def _live_type_from_row(raw):  # REQ-565, REQ-813
    """Build a LiveDeliveryConfigType from a persisted JSONB dict (None when unset)."""
    from provisa.api.admin.types import (
        LiveDeliveryConfigType,
        LiveKafkaParamsType,
        LiveOutputConfigType,
    )

    if not raw:
        return None
    kafka_raw = raw.get("kafka")
    return LiveDeliveryConfigType(
        strategy=_live_strategy_from_raw(raw),
        watermark_column=raw.get("watermark_column"),
        poll_interval=int(raw.get("poll_interval", 10)),
        kafka=(
            LiveKafkaParamsType(
                topic=kafka_raw["topic"],
                format=kafka_raw.get("format", "json"),
                key_column=kafka_raw.get("key_column"),
            )
            if kafka_raw
            else None
        ),
        query_id=raw.get("query_id"),
        outputs=[
            LiveOutputConfigType(
                type=o["type"],
                topic=o.get("topic"),
                key_column=o.get("key_column"),
                bootstrap_servers=o.get("bootstrap_servers"),
            )
            for o in raw.get("outputs", [])
        ],
    )
