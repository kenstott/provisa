# Copyright (c) 2026 Kenneth Stott
# Canary: b9b8116c-9946-4668-b167-a83332ad9e68
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Elasticsearch read over HTTP, engine-independently (REQ-1672): index listing, mapping-driven
column paths, the scroll read, the adapter loader, and the engine-gated wiring."""

from __future__ import annotations

from types import SimpleNamespace

import httpx
import pytest
import respx

from provisa.elasticsearch import fetch as es

_BASE = "http://es.test:9200"
_MAPPING = {
    "tickets": {
        "mappings": {
            "properties": {
                "ticket_id": {"type": "keyword"},
                "priority": {"type": "integer"},
                "reporter": {"properties": {"name": {"type": "text"}}},
            }
        }
    }
}


def _conn() -> es.ESConnection:
    return es.ESConnection.build("es.test", 9200)


class TestConnection:
    def test_host_and_port_become_the_base_url(self):
        assert es.ESConnection.build("h", 9200).base_url == "http://h:9200"
        assert es.ESConnection.build("h", 9243, tls=True).base_url == "https://h:9243"

    def test_a_url_host_is_taken_verbatim(self):
        assert es.ESConnection.build("https://cloud.example/es/", 9200).base_url == (
            "https://cloud.example/es"
        )

    def test_basic_auth_only_when_both_halves_present(self):
        assert es.ESConnection.build("h", 1, username="u", password="p").auth == ("u", "p")
        assert es.ESConnection.build("h", 1, username="u").auth is None


@respx.mock
def test_list_indices_skips_system_indices():
    respx.get(f"{_BASE}/_cat/indices").mock(
        return_value=httpx.Response(
            200, json=[{"index": "tickets"}, {"index": ".kibana"}, {"index": "adopters"}]
        )
    )
    assert es.list_indices(_conn()) == ["adopters", "tickets"]


@respx.mock
def test_index_columns_flatten_the_mapping_with_source_paths():
    respx.get(f"{_BASE}/tickets/_mapping").mock(return_value=httpx.Response(200, json=_MAPPING))
    cols = es.index_columns(_conn(), "tickets")
    assert [(c["name"], c["type"], c["sourcePath"]) for c in cols] == [
        ("priority", "INTEGER", "priority"),
        ("reporter_name", "VARCHAR", "reporter.name"),
        ("ticket_id", "VARCHAR", "ticket_id"),
    ]


@respx.mock
def test_table_index_and_columns_resolves_nested_paths_and_refuses_unknown_columns():
    respx.get(f"{_BASE}/tickets/_mapping").mock(return_value=httpx.Response(200, json=_MAPPING))
    index, cols = es.table_index_and_columns(_conn(), {}, "tickets", ["ticket_id", "reporter_name"])
    assert index == "tickets"
    assert cols == [("ticket_id", "ticket_id"), ("reporter_name", "reporter.name")]
    with pytest.raises(ValueError, match="no field for registered column"):
        es.table_index_and_columns(_conn(), {}, "tickets", ["ticket_id", "nope"])


@respx.mock
def test_table_index_and_columns_honours_the_mapping_dsl():
    respx.get(f"{_BASE}/app-logs-2026/_mapping").mock(
        return_value=httpx.Response(
            200,
            json={"app-logs-2026": {"mappings": {"properties": {"level": {"type": "keyword"}}}}},
        )
    )
    mapping = {
        "tables": [
            {
                "name": "logs",
                "index": "app-logs-2026",
                "columns": [{"name": "severity", "path": "level"}],
            }
        ]
    }
    index, cols = es.table_index_and_columns(_conn(), mapping, "logs", ["severity"])
    assert (index, cols) == ("app-logs-2026", [("severity", "level")])


@respx.mock
def test_fetch_rows_scrolls_every_page_and_releases_the_scroll():
    respx.post(f"{_BASE}/tickets/_search").mock(
        return_value=httpx.Response(
            200,
            json={
                "_scroll_id": "s1",
                "hits": {"hits": [{"_source": {"ticket_id": "T-1", "reporter": {"name": "Ann"}}}]},
            },
        )
    )
    scroll = respx.post(f"{_BASE}/_search/scroll").mock(
        side_effect=[
            httpx.Response(
                200,
                json={"_scroll_id": "s1", "hits": {"hits": [{"_source": {"ticket_id": "T-2"}}]}},
            ),
            httpx.Response(200, json={"_scroll_id": "s1", "hits": {"hits": []}}),
        ]
    )
    released = respx.delete(f"{_BASE}/_search/scroll").mock(return_value=httpx.Response(200))
    rows = es.fetch_rows(
        _conn(), "tickets", [("ticket_id", "ticket_id"), ("reporter_name", "reporter.name")]
    )
    assert rows == [
        {"ticket_id": "T-1", "reporter_name": "Ann"},
        {"ticket_id": "T-2", "reporter_name": None},
    ]
    assert scroll.call_count == 2
    assert released.called


@pytest.mark.asyncio
@respx.mock
async def test_loader_reads_the_registered_columns_of_the_index():
    from provisa.events.source_loader import make_elasticsearch_loader

    respx.get(f"{_BASE}/tickets/_mapping").mock(return_value=httpx.Response(200, json=_MAPPING))
    respx.post(f"{_BASE}/tickets/_search").mock(
        return_value=httpx.Response(
            200, json={"hits": {"hits": [{"_source": {"ticket_id": "T-1", "priority": 2}}]}}
        )
    )
    source = SimpleNamespace(
        id="es", host="es.test", port=9200, username="", password="", mapping={}
    )
    table = SimpleNamespace(
        table_name="tickets",
        columns=[
            SimpleNamespace(name="ticket_id", native_filter_type=None),
            SimpleNamespace(name="priority", native_filter_type=None),
            SimpleNamespace(name="_nf_q", native_filter_type="query_param"),
        ],
    )
    rows = await make_elasticsearch_loader()(source, table)
    assert rows == [{"ticket_id": "T-1", "priority": 2}]


def test_loader_is_wired_only_when_the_engine_has_no_elasticsearch_connector():
    from provisa.events.app_wiring import build_adapter_loaders

    from provisa.federation.connector import Mechanism

    state = SimpleNamespace(config=SimpleNamespace(sources=[]))
    # A native engine's completed reach lists the type as a land-into-store entry (FETCH): not live.
    land = SimpleNamespace(
        engine=SimpleNamespace(
            connectors={
                "elasticsearch": SimpleNamespace(mechanism=Mechanism.FETCH, reads_in_place=False)
            }
        )
    )
    assert "elasticsearch" in build_adapter_loaders(state, land)
    assert "elasticsearch" in build_adapter_loaders(
        state, SimpleNamespace(engine=SimpleNamespace(connectors={}))
    )
    trino = SimpleNamespace(
        engine=SimpleNamespace(
            connectors={
                "elasticsearch": SimpleNamespace(mechanism=Mechanism.ATTACH_R, reads_in_place=True)
            }
        )
    )
    assert "elasticsearch" not in build_adapter_loaders(state, trino)


@pytest.mark.asyncio
async def test_native_tables_lists_live_indices_under_default():
    from provisa.api.admin import introspect

    class _Conn:
        async def execute_core(self, stmt):
            row = SimpleNamespace(
                _mapping={
                    "id": "es",
                    "host": "es.test",
                    "port": 9200,
                    "username": None,
                    "mapping": {},
                }
            )
            return SimpleNamespace(fetchone=lambda: row)

    with respx.mock:
        respx.get(f"{_BASE}/_cat/indices").mock(
            return_value=httpx.Response(200, json=[{"index": "tickets"}, {"index": "adopters"}])
        )
        tables = await introspect._native_tables_elasticsearch(
            "es", "default", _Conn(), SimpleNamespace()
        )  # type: ignore[arg-type]
        assert [t.name for t in tables or []] == ["adopters", "tickets"]
        assert (
            await introspect._native_tables_elasticsearch("es", "other", _Conn(), SimpleNamespace())
            == []
        )  # type: ignore[arg-type]
