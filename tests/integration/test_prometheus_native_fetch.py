# Copyright (c) 2026 Kenneth Stott
# Canary: 11a765de-fa03-4604-92fb-dca34d7bfa2f
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Prometheus read natively against the stack's live server (REQ-1689): what Register Table lists
and types on a native engine, and the rows the landing loader produces — no Trino anywhere. The
server's own ``up`` metric is the data: nothing can be seeded into a pull-only server."""

from __future__ import annotations

import os
import time
from types import SimpleNamespace

import httpx
import pytest

from provisa.events.source_loader import make_prometheus_loader
from provisa.prometheus import fetch as pf

pytestmark = [
    pytest.mark.integration,
    pytest.mark.requires_prometheus,
    pytest.mark.asyncio(loop_scope="session"),
]


def _base() -> str:
    return f"http://localhost:{os.environ['PROMETHEUS_PORT']}"


@pytest.fixture(autouse=True)
def _scraped():
    """Wait for the first scrape so ``up`` has a sample (observability/prometheus.yml, 15 s)."""
    deadline = time.monotonic() + 120
    with httpx.Client(timeout=10) as c:
        while True:
            r = c.get(f"{_base()}/api/v1/query", params={"query": "up"})
            if r.status_code == 200 and r.json()["data"]["result"]:
                return
            assert time.monotonic() < deadline, "prometheus never produced an 'up' sample"
            time.sleep(3)


async def test_metrics_list_and_up_is_typed_from_its_labels():
    conn = pf.PrometheusConnection.build(_base())
    assert "up" in pf.list_metrics(conn)
    cols = {c["name"]: c["type"] for c in pf.metric_columns(conn, "up")}
    assert cols["timestamp"] == "TIMESTAMP" and cols["value"] == "DOUBLE"
    assert cols["job"] == "VARCHAR" and cols["instance"] == "VARCHAR"


async def test_loader_lands_up_samples_with_their_labels():
    source = SimpleNamespace(id="p", host=_base(), port=0, password="", mapping={})
    table = SimpleNamespace(
        table_name="up",
        columns=[
            SimpleNamespace(name=n, native_filter_type=None) for n in ("timestamp", "value", "job")
        ],
    )
    rows = await make_prometheus_loader()(source, table)
    assert rows and all(r["job"] and isinstance(r["value"], float) for r in rows)
