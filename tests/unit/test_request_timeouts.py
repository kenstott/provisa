# Copyright (c) 2026 Kenneth Stott
# Canary: 8a2e6f41-0c95-4d37-b1e8-5f9d3a7c2b60
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The request timeout is an operator setting per transport (REQ-1905).

`limits.request_timeout` is the default; `limits.request_timeouts` holds an optional value per
transport. Flight ships at 3600 s (large data transfers) and pgwire at 300 s (BI workloads); every
other transport ships on the 60 s default. One resolver answers for every place that binds a
request deadline."""

# Requirements: REQ-1905

from __future__ import annotations

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from provisa.core import deployment_settings, limits, settings_registry
from provisa.core.database import Database, create_engine_from_url
from provisa.core.limits import REQUEST_TRANSPORTS, request_timeout_for, request_timeout_setting
from provisa.core.schema_admin import deployment_settings as settings_table
from provisa.core.schema_admin import metadata


@pytest.fixture(autouse=True)
def _a_fresh_deployment(monkeypatch):
    """Nothing stored, nothing in the environment, an empty config."""
    monkeypatch.delenv("PROVISA_REQUEST_TIMEOUT", raising=False)
    monkeypatch.setattr(deployment_settings, "_db", None)
    monkeypatch.setattr(deployment_settings, "_held", None)
    monkeypatch.setattr(settings_registry, "_config", {})


@pytest.fixture
def control_plane(tmp_path):
    engine = create_engine_from_url(f"sqlite+pysqlite:///{tmp_path / 'cp.db'}")
    with engine.begin() as conn:
        metadata.create_all(conn, tables=[settings_table])
    db = Database(engine, name="platform")
    deployment_settings.bind(db)
    yield db
    engine.dispose()


def test_the_ten_transports():
    assert REQUEST_TRANSPORTS == (
        "graphql",
        "rest",
        "jsonapi",
        "sql_http",
        "cypher_http",
        "pgwire",
        "flight",
        "bolt",
        "grpc",
        "mcp",
    )


def test_a_fresh_deployment_ships_flight_at_3600_pgwire_at_300_and_the_rest_at_60():
    assert request_timeout_for("flight") == 3600.0
    assert request_timeout_for("pgwire") == 300.0
    for transport in (
        "graphql",
        "rest",
        "jsonapi",
        "sql_http",
        "cypher_http",
        "bolt",
        "grpc",
        "mcp",
    ):
        assert request_timeout_for(transport) == 60.0


def test_the_setting_named_is_the_one_in_force():
    assert request_timeout_setting("flight") == "limits.request_timeouts.flight"
    assert request_timeout_setting("graphql") == "limits.request_timeout"


def test_a_transports_own_value_in_the_config_is_honoured_and_the_others_keep_the_default():
    settings_registry.bind_config(
        {"server": {"limits": {"request_timeout": 45, "request_timeouts": {"graphql": 5}}}}
    )
    assert request_timeout_for("graphql") == 5.0
    assert request_timeout_for("rest") == 45.0
    assert request_timeout_for("flight") == 3600.0  # the shipped value is not the default's


def test_the_environment_variable_sets_the_default_only(monkeypatch):
    monkeypatch.setenv("PROVISA_REQUEST_TIMEOUT", "20")
    assert request_timeout_for("graphql") == 20.0
    assert request_timeout_for("pgwire") == 300.0


def test_an_unknown_transport_in_the_config_is_a_config_error_naming_it():
    settings_registry.bind_config({"server": {"limits": {"request_timeouts": {"smtp": 5}}}})
    with pytest.raises(settings_registry.SettingInvalid) as raised:
        request_timeout_for("graphql")
    assert raised.value.key == "limits.request_timeouts.smtp"


def test_asking_for_a_transport_that_does_not_exist_is_refused():
    with pytest.raises(ValueError, match="smtp"):
        request_timeout_for("smtp")


def test_a_stored_value_wins_and_clearing_it_returns_the_transport_to_what_shipped(control_plane):
    settings_registry.store(
        control_plane,
        {"limits.request_timeouts": {"flight": 7200, "graphql": 10}},
        updated_by="op",
    )
    assert request_timeout_for("flight") == 7200.0
    assert request_timeout_for("graphql") == 10.0
    settings_registry.store(
        control_plane, {"limits.request_timeouts": {"graphql": None}}, updated_by="op"
    )
    assert request_timeout_for("graphql") == 60.0
    assert request_timeout_for("flight") == 7200.0  # a partial store keeps the other keys


# --- the settings API ---------------------------------------------------------------------------


@pytest.fixture
def client(control_plane, tmp_path, monkeypatch):
    import yaml

    from provisa.api.admin.settings_router import router
    from provisa.api.app import state

    cfg = tmp_path / "provisa.yaml"
    cfg.write_text(yaml.safe_dump({"sources": [], "domains": [], "tables": [], "roles": []}))
    monkeypatch.setenv("PROVISA_CONFIG", str(cfg))
    monkeypatch.setattr(state, "admin_db", control_plane)
    app = FastAPI()
    app.include_router(router)
    return TestClient(app)


def test_get_reports_the_default_and_all_ten_transports(client):
    body = client.get("/admin/settings").json()["limits"]
    assert body["request_timeout"] == 60.0
    assert set(body["request_timeouts"]) == set(REQUEST_TRANSPORTS)
    assert body["request_timeouts"]["flight"] == 3600.0
    assert body["request_timeouts"]["pgwire"] == 300.0
    assert body["request_timeouts"]["graphql"] is None  # uses the default


def test_put_changes_only_the_keys_present_and_null_clears(client):
    r = client.put(
        "/admin/settings",
        json={
            "limits": {"request_timeout": 90, "request_timeouts": {"flight": 7200, "pgwire": None}}
        },
    )
    assert r.status_code == 200, r.text
    assert set(r.json()["updated"]) == {"limits.request_timeout", "limits.request_timeouts"}
    limits_ = client.get("/admin/settings").json()["limits"]
    assert limits_["request_timeout"] == 90.0
    assert limits_["request_timeouts"]["flight"] == 7200.0
    # null clears pgwire's stored value: it is back on what shipped.
    assert limits_["request_timeouts"]["pgwire"] == 300.0
    assert request_timeout_for("graphql") == 90.0


@pytest.mark.parametrize(
    ("limits_body", "field"),
    [
        ({"request_timeout": 0}, "limits.request_timeout"),
        ({"request_timeout": "soon"}, "limits.request_timeout"),
        ({"request_timeouts": {"flight": -1}}, "limits.request_timeouts.flight"),
        ({"request_timeouts": {"flight": "long"}}, "limits.request_timeouts.flight"),
        ({"request_timeouts": {"smtp": 5}}, "limits.request_timeouts.smtp"),
    ],
)
def test_a_bad_value_is_a_400_naming_the_field(client, limits_body, field):
    from provisa.api.errors import ApiError

    app = client.app
    captured: list[ApiError] = []

    @app.exception_handler(ApiError)
    async def _capture(_request, exc):  # the app's own handler renders code + params
        from fastapi.responses import JSONResponse

        captured.append(exc)
        return JSONResponse({"code": exc.code, "params": exc.params}, status_code=exc.status_code)

    r = client.put("/admin/settings", json={"limits": limits_body})
    assert r.status_code == 400
    assert r.json()["code"] == "settings.invalid_value"
    assert r.json()["params"]["field"] == field
    # Nothing was stored.
    assert client.get("/admin/settings").json()["limits"]["request_timeout"] == 60.0


def test_the_resolver_is_the_registrys(monkeypatch):
    """No resolution or storage of its own: two registry reads and an explicit is-None choice."""
    import inspect

    src = inspect.getsource(limits.request_timeout_for)
    assert 'settings_registry.value("limits.request_timeouts")' in src
    assert 'settings_registry.value("limits.request_timeout")' in src
    assert "is None" in src


# --- which transport a request is on, and every binder asks the one resolver --------------------


def test_an_http_route_is_its_own_transport_and_other_paths_have_none():
    from provisa.core.limits import http_transport_for_path

    assert http_transport_for_path("/data/graphql") == "graphql"
    assert http_transport_for_path("/data/sql") == "sql_http"
    assert http_transport_for_path("/data/cypher") == "cypher_http"
    assert http_transport_for_path("/data/rest/sales/orders") == "rest"
    assert http_transport_for_path("/data/jsonapi/sales/orders") == "jsonapi"
    assert http_transport_for_path("/admin/settings") is None
    assert http_transport_for_path("/data/restricted") is None  # a prefix, not a substring


def test_a_statement_is_timed_by_its_route_when_one_is_bound_else_by_its_surface():
    from provisa.core.limits import bound_request_transport, statement_timeout

    settings_registry.bind_config(
        {"server": {"limits": {"request_timeouts": {"rest": 7, "bolt": 9, "mcp": 11}}}}
    )
    assert statement_timeout("bolt") == (9.0, "bolt", "limits.request_timeouts.bolt")
    assert statement_timeout("mcp") == (11.0, "mcp", "limits.request_timeouts.mcp")
    assert statement_timeout("grpc") == (60.0, "grpc", "limits.request_timeout")
    assert statement_timeout("pgwire") == (300.0, "pgwire", "limits.request_timeouts.pgwire")
    assert statement_timeout("airport") == (3600.0, "flight", "limits.request_timeouts.flight")
    with bound_request_transport("rest"):
        assert statement_timeout("http") == (7.0, "rest", "limits.request_timeouts.rest")
    # An HTTP request on a route that is not one of the data transports (an admin route running a
    # query) has no transport of its own: the default.
    assert statement_timeout("http") == (60.0, "http", "limits.request_timeout")


def test_every_place_that_binds_a_request_deadline_asks_the_resolver():
    """file -> the text that must be there (the transport it resolves) and the hard-coded or
    private reading that must be gone."""
    from pathlib import Path

    root = Path(__file__).parents[2] / "provisa"
    expected = {
        "api/data/endpoint.py": ['request_timeout_for("graphql")'],
        "api/rest/cypher_router.py": ['request_timeout_for("cypher_http")'],
        "api/rest/graph_tools_router.py": ['request_timeout_for("cypher_http")'],
        "api/flight/server.py": ['request_timeout_for("flight")'],
        "pgwire/server.py": ['request_timeout_for("pgwire")'],
        "pgwire/copy_handler.py": ['request_timeout_for("pgwire")'],
        "pgwire/_pipeline.py": ["statement_timeout(plan.audit.surface)"],
        "core/request_thread.py": ['settings_registry.value("limits.request_timeout")'],
        "api/app.py": ["http_transport_for_path("],
    }
    for rel, needles in expected.items():
        src = (root / rel).read_text()
        for needle in needles:
            assert needle in src, f"{rel}: {needle}"
    gone = {
        "api/data/endpoint_helpers.py": "def _request_timeout",
        "api/data/endpoint.py": "_request_timeout()",
        "api/rest/cypher_router.py": "_request_timeout()",
        "api/rest/graph_tools_router.py": "_request_timeout()",
        "pgwire/_pipeline.py": "_request_timeout()",
        "core/request_thread.py": 'os.environ.get("PROVISA_REQUEST_TIMEOUT"',
        "api/flight/server.py": 'server_limits["request_timeout"]',
    }
    for rel, needle in gone.items():
        assert needle not in (root / rel).read_text(), f"{rel} still has {needle}"
    assert "timeout=120" not in (root / "pgwire/copy_handler.py").read_text()
