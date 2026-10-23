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

    # wal_level logical: a PostgreSQL source's change feed is read from a logical replication
    # slot (REQ-929, test_pg_logical_change_feed.py); a cluster of its own per test would be one
    # more server per worker.
    pg = start_local_postgres(settings={"wal_level": "logical"})
    yield pg
    pg.stop()


@pytest.fixture(scope="session")
def control_plane_template(embedded_postgres):
    """The worker's control-plane template database (tests/pg_control_plane.py), built once."""
    from tests.pg_control_plane import build_template

    return build_template(embedded_postgres)


@pytest.fixture
def control_plane(embedded_postgres, control_plane_template):
    """A control plane of this test's own (REQ-2014: PostgreSQL only): a database cloned from the
    worker's template -- the platform registry and org ``default`` laid out -- dropped afterwards.
    ``.platform`` and ``.org(org_id)`` are its handles, ``.url`` its SQLAlchemy URL."""
    from tests.pg_control_plane import clone

    plane = clone(embedded_postgres, control_plane_template)
    yield plane
    plane.close()


# The org a fresh AppState serves (AppState._org_id) -- used when the app module is not loaded.
_DEPLOYMENT_ORG = "default"


def pytest_configure(config: pytest.Config) -> None:
    config.addinivalue_line(
        "markers",
        "unbound: run with no org bound -- for the entrypoints that bind one themselves and for "
        "the refusal of work bound to none (REQ-1266)",
    )
    config.addinivalue_line(
        "markers",
        "real_role_probe: record a role-probing event through security.role_probe.record itself, "
        "not the unit stand-in (REQ-2076)",
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
def _no_runner_freezes_the_test_process(monkeypatch: pytest.MonkeyPatch):
    """A replica runner's first idle pass freezes the heap once per process (REQ-1911). In a test
    worker that would change process-wide GC state for every later test, so the freeze counts as
    done here; the test of the freeze itself clears the flag and records the call instead."""
    from provisa.federation import replica_runner

    monkeypatch.setattr(replica_runner, "_boot_builds_frozen", True)


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

    def _refused(ports, password):
        raise AssertionError(
            f"a unit test let a replica dial a real pgwire port ({ports}): give the "
            "ConnectorReplica a prepare_catalog"
        )

    _refused.real = real  # type: ignore[attr-defined]
    monkeypatch.setattr(replica, "_prepare_catalog", _refused)
    yield


@pytest.fixture(autouse=True)
def _deployment_org_bound(request: pytest.FixtureRequest, monkeypatch):
    # ``monkeypatch`` is set up before this fixture and so torn down after it: what it replaced is
    # put back below while the org is still bound.
    if request.node.get_closest_marker("unbound") is not None:
        yield
        return
    from provisa.core.request_context import reset_current_org, set_current_org

    app = sys.modules.get("provisa.api.app")
    org_id = app.state.org_id if app is not None else _DEPLOYMENT_ORG
    token = set_current_org(org_id)
    # REQ-1992: the deployment org's folder, as its runtime's build makes it ready on a node.
    from provisa.core import org_files

    org_files.folder(org_id).mkdir(parents=True, exist_ok=True)
    yield
    # A routed AppState attribute monkeypatch replaced is put back while the org is bound.
    monkeypatch.undo()
    reset_current_org(token)


@pytest.fixture(autouse=True)
def _role_probe_events(request):
    """REQ-2076: a role named that the caller does not hold is recorded through the admin plane
    (security.role_probe.record), which a unit test's state does not have. Here the events are
    collected instead, for a test to read; tests/unit/test_role_probe.py tests the recording
    itself (``@pytest.mark.real_role_probe``)."""
    events: list = []
    # Its own patcher: a test that undoes its ``monkeypatch`` (a wire fixture wanting the real
    # streams) must not bring the admin-plane recording back with it.
    patcher = pytest.MonkeyPatch()
    if request.node.get_closest_marker("real_role_probe") is None:
        from provisa.security import role_probe

        async def _collect(probe):
            events.append(probe)
            return role_probe.suspension(probe)

        patcher.setattr(role_probe, "record", _collect)
    yield events
    patcher.undo()
