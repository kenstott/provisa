# Copyright (c) 2026 Kenneth Stott
# Canary: 3f0b7c2e-5a41-4d8e-9c6a-1e2d7b9f0a64
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A read of the control plane's own tables (provisa-admin) is judged by the gates like any other
one-source read (REQ-863), against the control plane's dialect, and goes to the admin terminal
when it passes them (REQ-1411, REQ-1919). The source state the router reads holds that dialect
from the same loader that fills every other source's: a missing dialect supports nothing, and
landed the meta reads of the SQL explorer with "cannot run a subquery read as a value"."""

# Requirements: REQ-863, REQ-1411, REQ-1919

from types import SimpleNamespace

import pytest

from provisa.core.models import ProvisaConfig
from provisa.transpiler.router import Route, decide_route

# The ontology terms query the SQL explorer sends, as governed: one meta table read twice, a
# correlated subquery read as a value, JSON aggregation of JSON objects.
_TERMS = (
    "SELECT t0.id, t0.name, COALESCE((SELECT JSON_AGG(JSON_BUILD_OBJECT('id', t1.id, "
    "'name', t1.name)) FROM meta.ontology_terms AS t1 WHERE t1.parent_id = t0.id), '[]') "
    "AS children FROM meta.ontology_terms AS t0"
)
_PLAIN = "SELECT t0.id, t0.name FROM meta.ontology_terms AS t0"


class _NativeEngine:
    """The bound EngineRuntime of the default demo: DuckDB, landing no ops signals."""

    name = "duckdb"
    landed_signals: frozenset[str] = frozenset()


@pytest.fixture()
def demo_state(monkeypatch):
    """The source state a demo boot builds: an embedded PostgreSQL control plane, DuckDB engine,
    and no source declared in the configuration."""
    from provisa.api.app import state as app_state
    from provisa.api.app_loaders import _populate_source_catalog_names
    from provisa.core.request_context import reset_current_org, set_current_org

    token = set_current_org("default")
    monkeypatch.setattr(app_state, "org_id", "default", raising=False)
    for name in (
        "source_catalogs",
        "source_types",
        "source_dialects",
        "source_cache",
        "source_federation_hints",
    ):
        monkeypatch.setattr(app_state, name, {}, raising=False)
    monkeypatch.setattr(app_state, "federation_engine", _NativeEngine(), raising=False)
    monkeypatch.setattr(app_state, "model_db", SimpleNamespace(dialect="postgresql"), raising=False)
    _populate_source_catalog_names(ProvisaConfig(sources=[], domains=[], tables=[], roles=[]))
    yield app_state
    monkeypatch.undo()
    reset_current_org(token)


def _route(state, statement: str):
    return decide_route(
        {"provisa-admin"},
        state.source_types,
        state.source_dialects,
        statement=statement,
        operator_floor={},
    )


def test_the_control_plane_source_is_typed_and_dialected_by_the_loader(demo_state):
    assert demo_state.source_types["provisa-admin"] == "postgresql"
    assert demo_state.source_dialects["provisa-admin"] == "postgres"


def test_a_meta_read_with_a_subquery_read_as_a_value_goes_to_the_control_plane(demo_state):
    decision = _route(demo_state, _TERMS)
    assert (decision.route, decision.source_id, decision.dialect) == (
        Route.DIRECT,
        "provisa-admin",
        "postgres",
    )
    assert decision.reason == "single source with a direct access path (postgresql)"


def test_a_plain_meta_read_goes_to_the_control_plane(demo_state):
    decision = _route(demo_state, _PLAIN)
    assert (decision.route, decision.source_id, decision.dialect) == (
        Route.DIRECT,
        "provisa-admin",
        "postgres",
    )
    assert decision.reason == "single source with a direct access path (postgresql)"
