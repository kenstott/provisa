# Copyright (c) 2026 Kenneth Stott
# Canary: 7d21c9b4-6e85-4a37-9f10-2b5c83d7e604
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""True E2E for Elasticsearch as a connector-source: registration through to a served query.

See ``connector_source_harness`` for why reachability and this are different claims.

The source is created with an empty ``tables`` mapping on purpose. Elasticsearch is one of the
mapping-DSL types (``_TrinoMappingDslConnector``, trino_connectors.py:505): a non-empty ``tables``
list would make ``write_table_definitions`` emit table-description files under
``trino_etc_dir()/elasticsearch`` on the *host*, and the Trino container mounts only
``./trino/catalog`` and ``./trino/kafka`` (docker-compose.core.yml:77-103) — so those files would
never reach the coordinator. With no definitions the connector's own index discovery is what
supplies the table, which is both the working configuration and the one worth testing.

The index therefore has to carry an explicit mapping: index discovery reads the ES mapping to build
the Trino columns, and that mapping is what ``registerTable`` ultimately introspects.
"""

from __future__ import annotations

import os

import httpx
import pytest

from tests.integration.connector_source_harness import (
    assert_registration_and_query,
    connector_client,  # noqa: F401 — imported for pytest fixture discovery
)

pytestmark = [pytest.mark.e2e, pytest.mark.asyncio(loop_scope="session")]

# Hyphen-free id, empty database — keeps the recorded catalog name and the physical one identical.
_SOURCE_ID = "elasticsearch_pipeline"
_DOMAIN = "es_e2e"

# The ES connector's default schema (elasticsearch.default-schema-name, es source.py:135); the
# index name is the table.
_SCHEMA = "default"
_INDEX = "provisa_pipeline_widgets"

_WIDGETS = [(1, "alpha"), (2, "beta"), (3, "gamma")]

_ES_BASE = f"http://localhost:{os.environ.get('ELASTICSEARCH_PORT', '9200')}"


def _seed_elasticsearch() -> None:
    """Create the index with an explicit mapping, bulk-load the widgets, and refresh.

    The refresh is not optional: ES makes writes searchable on its own schedule, and the
    coordinator would otherwise read an empty index. ``refresh=wait_for`` on the bulk call blocks
    until the documents are visible, which is what lets the harness run with no settle window.
    """
    with httpx.Client(timeout=60) as client:
        client.delete(f"{_ES_BASE}/{_INDEX}")  # a stale index from a prior run would keep its docs
        resp = client.put(
            f"{_ES_BASE}/{_INDEX}",
            json={
                "mappings": {
                    "properties": {
                        "id": {"type": "long"},
                        "name": {"type": "keyword"},
                    }
                }
            },
        )
        assert resp.status_code == 200, f"create index: HTTP {resp.status_code}: {resp.text}"

        lines = []
        for wid, name in _WIDGETS:
            lines.append('{"index":{}}')
            lines.append(f'{{"id":{wid},"name":"{name}"}}')
        resp = client.post(
            f"{_ES_BASE}/{_INDEX}/_bulk",
            params={"refresh": "wait_for"},
            content="\n".join(lines) + "\n",
            headers={"Content-Type": "application/x-ndjson"},
        )
        assert resp.status_code == 200, f"bulk: HTTP {resp.status_code}: {resp.text}"
        assert not resp.json()["errors"], f"bulk reported item errors: {resp.text}"


@pytest.mark.requires_elasticsearch
async def test_elasticsearch_registers_and_serves_semantic_query(connector_client):  # noqa: F811
    """createSource → registerTable (types introspected) → rebuildSchemas → semantic SELECT."""
    _seed_elasticsearch()

    await assert_registration_and_query(
        connector_client,
        source_id=_SOURCE_ID,
        source_type="elasticsearch",
        host="elasticsearch",
        port=9200,
        mapping={"tables": []},
        domain_id=_DOMAIN,
        schema_name=_SCHEMA,
        table_name=_INDEX,
        alias="widgets",
        columns=["id", "name"],
        order_by="id",
        expected_rows=[{"id": wid, "name": name} for wid, name in _WIDGETS],
    )
