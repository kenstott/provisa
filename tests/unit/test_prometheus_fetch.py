# Copyright (c) 2026 Kenneth Stott
# Canary: 2d54c3e4-6666-4549-997f-afd9f93fad09
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Prometheus over its HTTP API, engine-independently (REQ-1689): URL resolution, metric listing,
column typing from labels and metadata, the range read, the loader, and the engine-gated wiring."""

from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace

import httpx
import pytest
import respx

from provisa.prometheus import fetch as pf
from provisa.prometheus.source import endpoint_url

_BASE = "http://prom.test:9090"


def _ok(data):
    return httpx.Response(200, json={"status": "success", "data": data})


def _conn() -> pf.PrometheusConnection:
    return pf.PrometheusConnection.build(_BASE)


class TestEndpointUrl:
    def test_mapping_url_wins_then_url_host_then_host_port(self):
        assert endpoint_url("h", 1, {"url": "http://m:9090"}) == "http://m:9090"
        assert endpoint_url("http://u:9090", 0, {}) == "http://u:9090"
        assert endpoint_url("h", 9091, None) == "http://h:9091"
        with pytest.raises(ValueError, match="names no URL"):
            endpoint_url("", None, {})


def test_range_seconds():
    assert pf.range_seconds("15m") == 900 and pf.range_seconds("2d") == 172800
    with pytest.raises(ValueError, match="use <n>s"):
        pf.range_seconds("1 hour")


@respx.mock
def test_list_metrics_and_typed_columns():
    respx.get(f"{_BASE}/api/v1/label/__name__/values").mock(
        return_value=_ok(["up", "http_requests_total"])
    )
    assert pf.list_metrics(_conn()) == ["http_requests_total", "up"]
    respx.get(f"{_BASE}/api/v1/series").mock(
        return_value=_ok([{"__name__": "up", "job": "prometheus", "instance": "localhost:9090"}])
    )
    respx.get(f"{_BASE}/api/v1/metadata").mock(return_value=_ok({"up": [{"type": "gauge"}]}))
    cols = pf.metric_columns(_conn(), "up")
    assert [(c["name"], c["type"]) for c in cols] == [
        ("timestamp", "TIMESTAMP"),
        ("value", "DOUBLE"),
        ("instance", "VARCHAR"),
        ("job", "VARCHAR"),
    ]


@respx.mock
def test_api_error_status_is_refused():
    respx.get(f"{_BASE}/api/v1/label/__name__/values").mock(
        return_value=httpx.Response(
            200, json={"status": "error", "errorType": "bad_data", "error": "x"}
        )
    )
    with pytest.raises(ValueError, match="Prometheus API error"):
        pf.list_metrics(_conn())


def test_table_metric_defaults_and_mapping_dsl():
    assert pf.table_metric({}, "up") == ("up", "value", "1h")
    mapping = {
        "tables": [
            {
                "name": "requests",
                "metric": "http_requests_total",
                "value_column": "count",
                "default_range": "30m",
            }
        ]
    }
    assert pf.table_metric(mapping, "requests") == ("http_requests_total", "count", "30m")


@respx.mock
def test_fetch_rows_projects_timestamp_value_and_labels():
    seen: dict = {}

    def _capture(request: httpx.Request) -> httpx.Response:
        seen.update(dict(request.url.params))
        return _ok(
            {
                "resultType": "matrix",
                "result": [
                    {
                        "metric": {"__name__": "up", "job": "prometheus"},
                        "values": [[1700000000, "1"], [1700000015, "1"]],
                    },
                    {"metric": {"__name__": "up", "job": "node"}, "values": [[1700000000, "0"]]},
                ],
            }
        )

    respx.get(f"{_BASE}/api/v1/query_range").mock(side_effect=_capture)
    rows = pf.fetch_rows(_conn(), {}, "up", ["timestamp", "value", "job", "instance"])
    assert seen["query"] == "up" and float(seen["end"]) - float(seen["start"]) == 3600
    assert rows[0] == {
        "timestamp": datetime.fromtimestamp(1700000000, tz=UTC),
        "value": 1.0,
        "job": "prometheus",
        "instance": None,
    }
    assert [(r["job"], r["value"]) for r in rows] == [
        ("prometheus", 1.0),
        ("prometheus", 1.0),
        ("node", 0.0),
    ]


@pytest.mark.asyncio
@respx.mock
async def test_loader_reads_the_registered_columns():
    from provisa.events.source_loader import make_prometheus_loader

    respx.get(f"{_BASE}/api/v1/query_range").mock(
        return_value=_ok(
            {"result": [{"metric": {"job": "prometheus"}, "values": [[1700000000, "1"]]}]}
        )
    )
    source = SimpleNamespace(id="p", host=_BASE, port=0, password="", mapping={})
    table = SimpleNamespace(
        table_name="up",
        columns=[SimpleNamespace(name=n, native_filter_type=None) for n in ("value", "job")],
    )
    assert await make_prometheus_loader()(source, table) == [{"value": 1.0, "job": "prometheus"}]


def test_loader_is_wired_only_when_the_engine_does_not_read_prometheus_live():
    from provisa.events.app_wiring import build_adapter_loaders
    from provisa.federation.connector import Mechanism

    state = SimpleNamespace(config=SimpleNamespace(sources=[]))
    land = SimpleNamespace(
        engine=SimpleNamespace(
            connectors={
                "prometheus": SimpleNamespace(mechanism=Mechanism.FETCH, reads_in_place=False)
            }
        )
    )
    assert "prometheus" in build_adapter_loaders(state, land)
    live = SimpleNamespace(
        engine=SimpleNamespace(
            connectors={
                "prometheus": SimpleNamespace(mechanism=Mechanism.ATTACH_R, reads_in_place=True)
            }
        )
    )
    assert "prometheus" not in build_adapter_loaders(state, live)
