# Copyright (c) 2026 Kenneth Stott
# Canary: 5f5ea307-3022-45ba-ba17-362aaf04e7e3
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Fixtures for tests/unit only (tests/conftest.py holds the suite-wide ones).

Unit tests run as work for the deployment's own org (REQ-1266).

The product binds an org at every entrypoint -- the boot, each request, each protocol session,
each background job -- and refuses a per-org read with none bound. A unit test calls below those
entrypoints, so it stands in for one: each test runs with the deployment org bound, as the boot
binds it.

That binding is the test harness's, not the product's, so it must never hide an entrypoint that
forgets to bind. A test marked ``@pytest.mark.unbound`` runs with nothing bound: the entrypoint
binders are proven under it (each binds by itself), and so are the refusals.
"""

from __future__ import annotations


# The request-deadline clock a test moves itself (tests/deadline_clock.py).
from tests.deadline_clock import deadline_clock  # noqa: F401


import sys  # noqa: E402

import pytest  # noqa: E402


@pytest.fixture(scope="session")
def embedded_postgres():
    """A Postgres of this session's own (tests/embedded_pg.py): the pgserver wheel's server on a
    leased 127.0.0.1 port. A unit test that needs Postgres uses this, never whatever answers on the
    machine's 5432 — other jobs start and stop that one, and a parallel worker shares it."""
    from tests.embedded_pg import start_local_postgres

    pg = start_local_postgres()
    yield pg
    pg.stop()


# The org a fresh AppState serves (AppState._org_id) -- used when the app module is not loaded.
_DEPLOYMENT_ORG = "default"


def pytest_configure(config: pytest.Config) -> None:
    config.addinivalue_line(
        "markers",
        "unbound: run with no org bound -- for the entrypoints that bind one themselves and for "
        "the refusal of work bound to none (REQ-1266)",
    )


@pytest.fixture(autouse=True)
def _no_otel_log_pipeline_outlives_its_test():
    """A test that builds an app without running its lifespan installs the app's OTLP log
    pipeline (setup_otel at create_app) and never reaches the lifespan's shutdown_otel: its root
    handler and exporter thread would ship every later test's log records over HTTP. It is shut
    down here, as the lifespan would."""
    yield
    otel = sys.modules.get("provisa.api.otel_setup")
    if otel is not None and otel._log_provider is not None:
        otel.shutdown_otel()


@pytest.fixture(autouse=True)
def _no_unit_test_dials_a_pgwire_server(monkeypatch: pytest.MonkeyPatch):
    """A replica prepares its server's catalog by connecting to the server's port. A unit test's
    stand-in server listens nowhere, and its port (the default 5433) may be held by a real
    adapter on this machine -- a test once sent its catalog query to one. So the real
    preparation is refused in this lane: a test that reaches it gives its replica a
    ``prepare_catalog``. The real function stays reachable as ``_prepare_catalog.real`` for the
    tests of the function itself."""
    replica = sys.modules.get("provisa.federation.pgwire_replica")
    if replica is None:
        import provisa.federation.pgwire_replica as replica

    real = getattr(replica._prepare_catalog, "real", replica._prepare_catalog)

    def _refused(ports):
        raise AssertionError(
            f"a unit test let a replica dial a real pgwire port ({ports}): give the "
            "ConnectorReplica a prepare_catalog"
        )

    _refused.real = real  # type: ignore[attr-defined]
    monkeypatch.setattr(replica, "_prepare_catalog", _refused)
    yield


@pytest.fixture(autouse=True)
def _deployment_org_bound(request: pytest.FixtureRequest):
    if request.node.get_closest_marker("unbound") is not None:
        yield
        return
    from provisa.core.request_context import reset_current_org, set_current_org

    app = sys.modules.get("provisa.api.app")
    org_id = app.state.org_id if app is not None else _DEPLOYMENT_ORG
    token = set_current_org(org_id)
    yield
    if "monkeypatch" in request.fixturenames:
        # A routed AppState attribute monkeypatch replaced is put back while the org is bound.
        request.getfixturevalue("monkeypatch").undo()
    reset_current_org(token)
