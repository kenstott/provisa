# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Probe for calcite govdata-ops #1416 on the pinned engine (engine-v0.128.10): through the
govdata source's own pgwire server and Trino's ``postgresql`` connector, does ``DESCRIBE`` of each
table below return columns?

The fixtures are the ones of ``test_govdata_trino_source_e2e.py`` (cold start, reachability,
catalog from the product's ``TrinoGovdataConnector``). The test fails only on an error; its
assertions are the column counts the run saw. It prints each count and writes, to
``results/calcite-1416-probe/`` (the lane's artifact), the counts and the pgwire server's log
lines around the first ``DESCRIBE``.

Credentials: ``ASKAMERICA_API_KEY`` (a secret of the warehouse lane).
"""

from __future__ import annotations

import contextlib
import os
import subprocess
import sys
from pathlib import Path

import pytest

from tests.conftest import as_deployment_org, deployment_org_on_this_node

pytestmark = [pytest.mark.integration, pytest.mark.requires_warehouse]

_SOURCE_ID = "askamerica-1416-probe"
_CATALOG = "askamerica_1416_probe"
_SCHEMAS = "econ,weather,fec,ref"
_COLD_START_SECONDS = 600


def _cold_start(source, pr) -> None:
    """Start the source's server and wait for its first catalog query, failing with the end of
    the server's own log when the wait runs out: the report otherwise says only "STARTING" (warehouse
    run 38886673581: the catalog was not answered in 600 s).

    The adapter's catalog is prepared from its first use by the engine on (REQ-540, amended
    2026-10-28): a registered table is that use, so the source is marked in use as a registration
    marks it before the wait -- otherwise no preparation is sent and the wait answers STARTING
    at once."""
    pr.use_catalog_of(source)
    try:
        pr.ensure_endpoint(source, timeout=_COLD_START_SECONDS)
    except pr.SourceStillStartingError as exc:
        server = pr._endpoint_replica(source)._server
        tail = server.log_tail() if server is not None else "(no server was started)"
        raise AssertionError(f"{exc}\nthe server's log, its end:\n{tail}") from exc


_TRINO_HOST = os.environ.get("TRINO_HOST", "localhost")
_TRINO_PORT = int(os.environ.get("TRINO_PORT", "8080"))


def _required(name: str, why: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"{name} is not set: {why}")
    return value


def _source():
    from provisa.core.models import Source, SourceType

    return Source(
        id=_SOURCE_ID,
        type=SourceType.govdata,
        username=_required("ASKAMERICA_API_KEY", "the AskAmerica source needs a real key"),
        database=_SCHEMAS,
    )


def _container_reach() -> dict[str, str]:
    """Where the adapter listens and the name the Trino container reaches it by (as the Reachability section of
    test_govdata_trino_source_e2e.py describes)."""
    from provisa.federation import pgwire_replica as pr

    if sys.platform == "darwin":
        return {pr.ENGINE_HOST_ENV: "host.docker.internal"}
    gateway = subprocess.run(
        [
            "docker",
            "network",
            "inspect",
            "bridge",
            "--format",
            "{{(index .IPAM.Config 0).Gateway}}",
        ],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    return {pr.BIND_HOST_ENV: gateway, pr.ENGINE_HOST_ENV: gateway}


@pytest.fixture(scope="module")
def served(tmp_path_factory):
    """The source's server, bound where the Trino container reaches it."""
    from provisa.federation import pgwire_replica as pr

    data_dir = tmp_path_factory.mktemp("askamerica-1416-state")
    reached = _container_reach()
    previous = {
        name: os.environ.get(name)
        for name in ("PROVISA_DATA_DIR", pr.BIND_HOST_ENV, pr.ENGINE_HOST_ENV)
    }
    os.environ["PROVISA_DATA_DIR"] = str(data_dir)
    os.environ.update(reached)
    source = _source()
    try:
        with deployment_org_on_this_node():
            _cold_start(source, pr)
        yield source
    finally:
        with as_deployment_org():
            pr.stop_endpoint(source.id)
        for name, value in previous.items():
            if value is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = value


def _adapter_log(source) -> str:
    """The end of the adapter's own log. A govdata server runs from the one installed bundle, so
    its log is beside that bundle, not under this module's state directory (run 44967727447
    found none there): the server says where."""
    from provisa.federation import pgwire_replica as pr

    with as_deployment_org():
        server = pr._endpoint_replica(source)._server
    if server is None:
        return "(no server was started)"
    return server.log_tail()


@contextlib.contextmanager
def _step(what: str, sql: str, source):
    """Name the statement a failure happened on and attach the end of the adapter's log."""
    try:
        yield
    except Exception as exc:  # noqa: BLE001 - re-raised with the statement and the adapter's log
        raise AssertionError(
            f"{what} failed on:\n  {sql}\n{type(exc).__name__}: {exc}\n"
            f"--- adapter log (end) ---\n{_adapter_log(source)}"
        ) from exc


@pytest.fixture(scope="module")
def catalog(served):
    """A Trino ``postgresql`` catalog over the adapter, built from the product's own connector."""
    import trino.dbapi

    from provisa.federation.trino_connectors import TrinoGovdataConnector

    source = served
    # A module fixture runs before the per-test org binding: the endpoint is the deployment
    # org's, as in served.
    with as_deployment_org():
        props = TrinoGovdataConnector().details(source)
    with_clause = ", ".join(f"\"{k}\" = '{v}'" for k, v in props.items())
    create = f"CREATE CATALOG {_CATALOG} USING postgresql WITH ({with_clause})"
    # A client of the test's own, as the other catalog tests connect: the resource groups admit
    # user "provisa" only with Provisa's own source (provisa/<org>, or Flight's trino-jdbc).
    conn = trino.dbapi.connect(host=_TRINO_HOST, port=_TRINO_PORT, user="itest")
    cur = conn.cursor()
    cur.execute(f"DROP CATALOG IF EXISTS {_CATALOG}")
    cur.fetchall()
    with _step("creating the catalog", create, source):
        cur.execute(create)
        cur.fetchall()
    try:
        yield cur, source
    finally:
        cur.execute(f"DROP CATALOG IF EXISTS {_CATALOG}")
        cur.fetchall()
        conn.close()


def _run(cur, what: str, sql: str, source) -> list:
    with _step(what, sql, source):
        cur.execute(sql)
        return cur.fetchall()


_PROBED = (
    ("econ", "inflation_metrics"),
    ("weather", "nws_stations"),
    ("fec", "candidates"),
    ("ref", "countries"),
)
_RESULTS = Path("results") / "calcite-1416-probe"
_LOG_CONTEXT_LINES = 40


def _server_log_lines(source) -> list[str]:
    from provisa.federation import pgwire_replica as pr

    with as_deployment_org():
        server = pr._endpoint_replica(source)._server
    assert server is not None, "the source's server was not started"
    return (server._bundle_dir / pr.SERVER_LOG_NAME).read_text(errors="replace").splitlines()


def test_trino_describe_of_each_probed_table(catalog):
    cur, source = catalog
    _RESULTS.mkdir(parents=True, exist_ok=True)

    before = len(_server_log_lines(source))
    counts: dict[str, int] = {}
    first_window: list[str] = []
    for schema, table in _PROBED:
        columns = _run(cur, "describing the table", f"DESCRIBE {_CATALOG}.{schema}.{table}", source)
        counts[f"{schema}.{table}"] = len(columns)
        print(f"DESCRIBE {schema}.{table}: {len(columns)} columns {[c[0] for c in columns]}")
        if (schema, table) == _PROBED[0]:
            lines = _server_log_lines(source)
            start = max(0, before - _LOG_CONTEXT_LINES)
            first_window = lines[start:]

    (_RESULTS / "counts.txt").write_text(
        "".join(f"{name}: {n} columns\n" for name, n in counts.items())
    )
    (_RESULTS / "pgwire-log-around-first-describe.txt").write_text(
        f"(server log lines from {max(0, before - _LOG_CONTEXT_LINES) + 1}; "
        f"the first DESCRIBE was sent after line {before})\n" + "\n".join(first_window) + "\n"
    )
    (_RESULTS / "pgwire-server.log").write_text("\n".join(_server_log_lines(source)) + "\n")
    print("--- pgwire server log around the first DESCRIBE ---")
    print("\n".join(first_window))

    # The test fails only on an error (each DESCRIBE above raises through _step); this is what it saw.
    assert set(counts) == {f"{s}.{t}" for s, t in _PROBED}, counts
