# Copyright (c) 2026 Kenneth Stott
# Canary: 5e0c1f6a-7b3d-4a52-9c14-2f8d6e1ab7c3
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The HTTP query surfaces on the admin System health table and in the startup banner.

A surface the app mounts under ``/data`` has a row, or says here why it has none: a new surface
fails ``test_every_mounted_data_path_has_a_row_or_a_reason`` until it is given one.
"""

from __future__ import annotations

import logging
from types import SimpleNamespace

import pytest

from provisa.api.admin import system_health
from provisa.api.admin.system_health import HTTP_SURFACES, collect_system_health

#: Mounted under /data, with no row on the table: what serves the surfaces, not a surface.
_NO_ROW = {
    # Schema and metadata reads the clients of the surfaces make.
    "/data/catalog",
    "/data/domains",
    "/data/engine",
    "/data/graph-counts",
    "/data/graph-schema",
    "/data/introspection",
    "/data/grpc-group-by-columns",
    "/data/jsonapi-group-by-columns",
    "/data/openapi",
    "/data/proto",
    "/data/schema-version",
    "/data/sdl",
    "/data/sources",
    # gRPC has its listener's row; these relay to it.
    "/data/grpc",
    "/data/grpc-command",
    "/data/grpc-commands",
    # Operations rather than a query language.
    "/data/compile",
    "/data/impute-relationships",
    "/data/ingest",
    "/data/neo4j-export",
    "/data/nl-to-sql",
    "/data/query",
    "/data/redirect",
    "/data/redirect-file",
    "/data/sql",
    "/data/subscribe",
    "/data/touch",
}


def _route(path: str) -> SimpleNamespace:
    return SimpleNamespace(path=path)


@pytest.fixture
def served_app(monkeypatch):
    monkeypatch.setenv("PROVISA_CONFIG", "config/provisa.yaml")
    from provisa.api.app import create_app

    return create_app()


def test_every_mounted_data_path_has_a_row_or_a_reason(served_app):
    mounted = {
        "/".join(route.path.split("/")[:3])
        for route in served_app.routes
        if getattr(route, "path", "").startswith("/data/")
    }
    assert mounted - _NO_ROW == set(HTTP_SURFACES.values())


def test_every_surface_is_serving_on_the_real_app(served_app):
    assert system_health.serving_http_surfaces(served_app) == set(HTTP_SURFACES)


@pytest.fixture
def health_state(monkeypatch):
    """An app state with nothing on it: only the protocol rows are read here."""
    import provisa.api.app as app_module

    state = SimpleNamespace(
        federation_engine=SimpleNamespace(cluster_diagnostics=lambda: (True, 1, 1)),
        model_db=None,
        response_cache_store=None,
        _grpc_server=None,
        _flight_server=None,
    )
    monkeypatch.setattr(app_module, "state", state)
    monkeypatch.setattr(system_health, "_port", lambda _key: None)


async def test_each_surface_has_a_row_with_its_path_and_whether_it_serves(health_state):
    app = SimpleNamespace(routes=[_route("/data/sparql"), _route("/data/rest/orders")])

    health = await collect_system_health(app)

    rows = {p.name: p for p in health.protocols}
    assert {name: rows[name].path for name in HTTP_SURFACES} == HTTP_SURFACES
    assert {name: rows[name].status for name in HTTP_SURFACES} == {
        "SPARQL": "running",
        "GraphQL": "disabled",
        "Cypher over HTTP": "disabled",
        "REST": "running",
        "JSON:API": "disabled",
    }
    assert all(rows[name].port is None for name in HTTP_SURFACES)


async def test_a_path_that_only_starts_like_a_surface_is_not_that_surface(health_state):
    app = SimpleNamespace(routes=[_route("/data/jsonapi-group-by-columns")])

    health = await collect_system_health(app)

    assert next(p for p in health.protocols if p.name == "JSON:API").status == "disabled"


async def test_a_listener_row_has_no_path(health_state):
    health = await collect_system_health(SimpleNamespace(routes=[]))

    assert next(p for p in health.protocols if p.name == "pgwire").path is None


def test_the_banner_names_each_surface_being_served(caplog):
    from provisa.api.app_startup import _log_http_surfaces

    log = logging.getLogger("test_banner")
    app = SimpleNamespace(routes=[_route(path) for path in HTTP_SURFACES.values()])
    with caplog.at_level(logging.INFO, logger="test_banner"):
        _log_http_surfaces(app, log)

    for name, path in HTTP_SURFACES.items():
        assert f"{name} endpoint listening at {path} on the HTTP port" in caplog.messages


def test_the_banner_says_a_surface_that_is_not_served_is_not(caplog):
    from provisa.api.app_startup import _log_http_surfaces

    log = logging.getLogger("test_banner")
    app = SimpleNamespace(routes=[_route("/data/graphql")])
    with caplog.at_level(logging.INFO, logger="test_banner"):
        _log_http_surfaces(app, log)

    assert "GraphQL endpoint listening at /data/graphql on the HTTP port" in caplog.messages
    assert "SPARQL endpoint not served at /data/sparql" in caplog.messages
    assert not any("SPARQL endpoint listening" in m for m in caplog.messages)
