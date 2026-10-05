# Copyright (c) 2026 Kenneth Stott
# Canary: 8ec2ddb2-5549-442f-8942-af26efc07808
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Live SSE delivery is each org's, governed as its subscriber (REQ-286, REQ-1266).

Two orgs built by the real ``build_org_runtime`` against a live Postgres register a live table of
the SAME id, ``live-pg.marks`` -- physically ``lv_a.marks`` for one org and ``lv_b.marks`` for the
other. Each org's runtime holds its own live engine. A subscription opened through the real
``/data/subscribe`` handler is polled through the one governed pipeline as the subscriber:

* a subscriber in org B receives org B's rows and never org A's;
* in org A, ``restricted`` (a row rule ``region = 'us'`` and a masked ``note``) receives only its
  rows with the column masked, while ``reader`` receives every row in the clear.
"""

# Requirements: REQ-286, REQ-336, REQ-1266

from __future__ import annotations

import asyncio
import json
import os
from types import SimpleNamespace
from typing import Any, cast

import pytest

from provisa.core.request_context import reset_current_org, set_current_org

_CONFIG = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "fixtures", "governed_live_config.yaml")
)

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

# org id -> the physical schema its live-pg.marks is, and the rows it holds
_ORGS = {
    "lva": ("lv_a", [(1, "us", "a-secret", 1), (2, "eu", "a-other", 2)]),
    "lvb": ("lv_b", [(1, "us", "b-secret", 1)]),
}
_QUERY_ID = "live-pg.marks"


def _table(schema: str) -> dict:
    def _col(name: str, data_type: str, **extra: Any) -> dict:
        return {
            "name": name,
            "data_type": data_type,
            "visible_to": ["reader", "restricted"],
            **extra,
        }

    return {
        "source_id": "live-pg",
        "domain_id": "lv",
        "schema": schema,
        "table": "marks",
        "live": {"strategy": "poll", "watermark_column": "ts", "poll_interval": 3600},
        "columns": [
            _col("id", "integer", is_primary_key=True),
            _col("region", "varchar"),
            _col(
                "note",
                "varchar",
                mask_type="constant",
                mask_value="***",
                unmasked_to=["reader"],
            ),
            _col("ts", "integer"),
        ],
    }


async def _drop(state) -> None:
    async with state.tenant_db.acquire() as conn:
        for org, (schema, _rows) in _ORGS.items():
            await conn.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
            await conn.execute(f"DROP SCHEMA IF EXISTS org_{org} CASCADE")
            await conn.execute(f"DROP SCHEMA IF EXISTS org_{org}_mv_cache CASCADE")


@pytest.fixture(scope="module")
async def two_orgs():
    """Two orgs, each with its own live-pg.marks and rows, built and served by the running app."""
    from _pytest.monkeypatch import MonkeyPatch

    from provisa.api.app import _rebuild_schemas, build_org_runtime, create_app, state
    from provisa.core.config_loader import load_config, parse_config_dict

    mp = MonkeyPatch()
    mp.setenv("PROVISA_CONFIG", _CONFIG)
    mp.setenv("PG_HOST", os.environ.get("PG_HOST", "localhost"))
    mp.setenv("PG_PORT", os.environ.get("PG_PORT", "5432"))
    mp.setenv("PG_PASSWORD", os.environ.get("PG_PASSWORD", "provisa"))
    try:
        app = create_app()
        async with app.router.lifespan_context(app):
            assert state.tenant_db is not None
            await _drop(state)
            async with state.tenant_db.acquire() as conn:
                for schema, rows in _ORGS.values():
                    await conn.execute(f"CREATE SCHEMA {schema}")
                    await conn.execute(
                        f"CREATE TABLE {schema}.marks (id integer PRIMARY KEY, region varchar, "
                        "note varchar, ts integer)"
                    )
                    for row in rows:
                        await conn.execute(
                            f"INSERT INTO {schema}.marks VALUES ($1, $2, $3, $4)", *row
                        )
            for org, (schema, _rows) in _ORGS.items():
                rt = await build_org_runtime(org, include_demo=True)
                assert rt.live_engine is not None, "the org's prod runtime holds its live engine"
                token = set_current_org(org)
                try:
                    assert rt.model_db is not None
                    async with rt.model_db.acquire() as conn:
                        await load_config(
                            parse_config_dict(
                                {
                                    "sources": [],
                                    "domains": [],
                                    "roles": [],
                                    "tables": [_table(schema)],
                                    "rls_rules": [
                                        {
                                            "table_id": "marks",
                                            "role_id": "restricted",
                                            "filter": "region = 'us'",
                                        }
                                    ],
                                }
                            ),
                            conn,
                            state.federation_engine,
                            catalog_names=rt.source_catalogs,
                            origin="admin",
                        )
                    await _rebuild_schemas()  # reconciles the org's engine from its model
                finally:
                    reset_current_org(token)
            try:
                yield state
            finally:
                await _drop(state)
    finally:
        mp.undo()


async def _first_rows(org: str, role: str) -> list[dict]:
    """Subscribe as ``role`` in ``org`` through the real handler, fire the subscriber's poll with
    nothing bound (as the scheduler fires it) and return the rows the stream delivers."""
    from provisa.api.app import state
    from provisa.api.data.subscribe import subscribe

    request = cast("Any", SimpleNamespace(state=SimpleNamespace(role=role)))
    token = set_current_org(org)
    try:
        response = await subscribe("marks", request, None, _QUERY_ID)
        engine = state.live_engine
        assert engine is not None
        [(qid, output_type)] = [g for g in engine._groups if g[1].startswith("sse:")]
    finally:
        reset_current_org(token)
    stream = cast("Any", response.body_iterator)
    try:
        await engine._poll(qid, output_type)
        rows = []
        while True:
            chunk = await asyncio.wait_for(stream.__anext__(), timeout=30)
            if chunk.startswith("data: "):
                rows.append(json.loads(chunk.removeprefix("data: ").strip()))
            if len(rows) == len(_ORGS[org][1]) or role == "restricted":
                break
    finally:
        await stream.aclose()
    return sorted(rows, key=lambda r: r["id"])


async def test_a_subscriber_receives_only_its_own_orgs_rows(two_orgs):
    """Both orgs' live tables have one id. Org B's subscriber is served by org B's engine and
    governed as org B's reader: it receives org B's row, never org A's."""
    rows = await _first_rows("lvb", "reader")
    assert rows == [{"id": 1, "region": "us", "note": "b-secret", "ts": 1}]


async def test_a_restricted_role_receives_only_its_rows_with_masked_columns(two_orgs):
    restricted = await _first_rows("lva", "restricted")
    assert restricted == [{"id": 1, "region": "us", "note": "***", "ts": 1}]
    reader = await _first_rows("lva", "reader")
    assert reader == [
        {"id": 1, "region": "us", "note": "a-secret", "ts": 1},
        {"id": 2, "region": "eu", "note": "a-other", "ts": 2},
    ]
