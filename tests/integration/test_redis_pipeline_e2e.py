# Copyright (c) 2026 Kenneth Stott
# Canary: 6b0a7f52-3c14-4a9d-8e77-5d2c0f9a41be
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Redis: registration through to a served semantic query.

Redis is the only source in this bucket whose table identity IS its key layout. The Trino Redis
connector has no per-table key pattern setting: with ``redis.key-prefix-schema-table=true`` it scans
one prefix derived from the table identity and nothing else — and for the ``default`` schema that
prefix carries no schema segment at all, so the seeded keys here are ``pipeline_widgets:<n>`` by
necessity, not by convention. ``provisa.redis.source.required_key_pattern`` is the single definition
both this test and the catalog-property generator read.

The columns are all VARCHAR because a Redis hash stores only strings; the connector's ``hash``
value decoder maps each declared field straight out of the hash, and a numeric declared type would
be asserting a conversion the store never performed.

Seeding runs against the host-published ``REDIS_PORT`` while the catalog reaches the broker at the
compose service name ``redis:6379`` — the same split every module in this bucket uses.
"""

from __future__ import annotations

import os

import pytest
import redis

from tests.integration.connector_source_harness import (
    assert_registration_and_query,
    connector_client,  # noqa: F401 — imported for pytest fixture discovery
)

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope="session")]

_SOURCE_ID = "redis_pipeline"
_DOMAIN = "redis_e2e"
_SCHEMA = "default"
_TABLE = "pipeline_widgets"
_KEY_COLUMN = "widget_key"
_COLUMNS = ["sku", "color"]
_WIDGETS = [
    ("W-001", "amber"),
    ("W-002", "cobalt"),
    ("W-003", "jade"),
    ("W-004", "rose"),
]


def _seed_redis() -> None:
    """Write one hash per widget at the only key prefix the connector will scan."""
    from provisa.redis.source import required_key_pattern

    prefix = required_key_pattern(_TABLE, _SCHEMA).removesuffix("*")
    client = redis.Redis(
        host="localhost", port=int(os.environ["REDIS_PORT"]), decode_responses=True
    )
    try:
        for stale in client.scan_iter(match=f"{prefix}*"):
            client.delete(stale)
        for index, (sku, color) in enumerate(_WIDGETS, start=1):
            client.hset(f"{prefix}{index}", mapping={"sku": sku, "color": color})
    finally:
        client.close()


# No requires_* marker: redis is a core stack service (tests/conftest.py _CORE_SERVICES), up for
# every session that provisions the isolated stack.
async def test_redis_registers_and_serves_semantic_query(connector_client):  # noqa: F811
    _seed_redis()
    await assert_registration_and_query(
        connector_client,
        source_id=_SOURCE_ID,
        source_type="redis",
        host="redis",
        port=6379,
        domain_id=_DOMAIN,
        schema_name=_SCHEMA,
        table_name=_TABLE,
        columns=_COLUMNS,
        order_by="sku",
        mapping={
            "tables": [
                {
                    "name": _TABLE,
                    # required_key_pattern is the contract, not a preference: any other pattern is
                    # rejected by generate_catalog_properties because the connector cannot scan it.
                    "key_pattern": f"{_TABLE}:*",
                    "key_column": _KEY_COLUMN,
                    "value_type": "hash",
                    "columns": [
                        {"name": "sku", "data_type": "VARCHAR", "field": "sku"},
                        {"name": "color", "data_type": "VARCHAR", "field": "color"},
                    ],
                }
            ]
        },
        expected_rows=[{"sku": sku, "color": color} for sku, color in _WIDGETS],
    )
