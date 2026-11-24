# Copyright (c) 2026 Kenneth Stott
# Canary: fd8f3483-6570-45ff-a568-f2023397d1af
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Probe for calcite govdata-ops #1416 on engine-v0.128.11 (read from its GitHub release in this
probe only; the pins are untouched): through the govdata source's own pgwire server and Trino's
``postgresql`` connector, does ``DESCRIBE`` of each table below return columns, and do ``SHOW
TABLES ... LIKE`` and ``SELECT * ... LIMIT 1`` of it work?

Logging for the run: Trino at DEBUG for ``io.trino.plugin.jdbc`` and ``io.trino.plugin.postgresql``
(a ``log.properties`` copied into this run's Trino container, which is then restarted), with each
failed query's full failure (stack trace included) read from Trino's query API; the pgwire server
with ``PGWIRE_CALCITE_DEBUG_LOG`` set, so its ``pgwire_calcite`` and ``buenavista`` loggers write
every statement, Parse and Bind (with the bound values) at DEBUG; and the server's exit status.

The fixtures are the ones of ``test_govdata_trino_source_e2e.py`` (cold start, reachability,
catalog from the product's ``TrinoGovdataConnector``). The test fails only on an error; its
assertions are the column counts the run saw. It prints each count and writes, to
``results/warehouse/calcite-1416-probe/`` (inside the warehouse lane's own directory of the
artifact: a directory directly under ``results/`` is read as a lane, run 45469384210), the counts
and the pgwire server's log lines around the first ``DESCRIBE``.

Credentials: ``ASKAMERICA_API_KEY`` (a secret of the warehouse lane).
"""

from __future__ import annotations

import contextlib
import json
import os
import subprocess
import sys
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

import pytest

from tests.conftest import (
    _ITEST_COMPOSE_ARGS,
    _REPO_ROOT,
    _itest_compose_env,
    as_deployment_org,
    deployment_org_on_this_node,
)

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


_PROBE_RELEASE = "engine-v0.128.11"


@pytest.fixture(scope="module")
def probe_release():
    """The govdata connector at engine-v0.128.11 for this module only: its entry in the one table
    of per-connector releases (``CONNECTOR_RELEASE``), and the schema record the bundle's model is
    held to read as that release's (the schemas recorded must still be the ones it serves, or the
    start is refused by name as before). Both are restored at the end."""
    from provisa.govdata import subjects
    from provisa.runtime_deps import pgwire_bundles

    previous = pgwire_bundles.CONNECTOR_RELEASE.get("govdata")
    recorded = subjects.bundle_schemas
    pgwire_bundles.CONNECTOR_RELEASE["govdata"] = _PROBE_RELEASE
    subjects.bundle_schemas = lambda: (_PROBE_RELEASE, recorded()[1])
    try:
        yield _PROBE_RELEASE
    finally:
        subjects.bundle_schemas = recorded
        if previous is None:
            del pgwire_bundles.CONNECTOR_RELEASE["govdata"]
        else:
            pgwire_bundles.CONNECTOR_RELEASE["govdata"] = previous


@pytest.fixture(scope="module")
def staged_bundle(probe_release):
    """The engine-v0.128.11 pgwire-govdata bundle, staged from its GitHub release (verified
    against the release's ``.sha256``), in this process so the release above applies (REQ-294: a
    running Provisa never fetches a bundle; the warehouse lane stages none)."""
    from provisa.runtime_deps.pgwire_bundles import default_cache_root, download_release_asset
    from provisa.runtime_deps.stage_pgwire_bundles import _excepted, stage

    root = default_cache_root()
    stage(root, downloader=download_release_asset, excepted=_excepted(["govdata"]))
    _RESULTS.mkdir(parents=True, exist_ok=True)
    launcher = root / probe_release / "pgwire-govdata" / "bin" / "pgwire-govdata"
    _write("bundle.txt", f"release: {probe_release}\nlauncher: {launcher}\n")
    assert launcher.is_file(), launcher


_DEBUG_LOG = Path.home() / "pgwire_calcite_debug.log"


@pytest.fixture(scope="module")
def served(tmp_path_factory, staged_bundle):
    """The source's server, bound where the Trino container reaches it."""
    from provisa.federation import pgwire_replica as pr

    data_dir = tmp_path_factory.mktemp("askamerica-1416-state")
    reached = _container_reach()
    previous = {
        name: os.environ.get(name)
        for name in (
            "PROVISA_DATA_DIR",
            pr.BIND_HOST_ENV,
            pr.ENGINE_HOST_ENV,
            "PGWIRE_CALCITE_DEBUG_LOG",
        )
    }
    os.environ["PROVISA_DATA_DIR"] = str(data_dir)
    os.environ.update(reached)
    # The server's pgwire_calcite and buenavista loggers at DEBUG, to ~/pgwire_calcite_debug.log
    # (the bundle's server.py): every statement, Parse and Bind with its values.
    os.environ["PGWIRE_CALCITE_DEBUG_LOG"] = "1"
    _DEBUG_LOG.unlink(missing_ok=True)
    source = _source()
    proc = None
    try:
        with deployment_org_on_this_node():
            _cold_start(source, pr)
        with as_deployment_org():
            proc = pr._endpoint_replica(source)._server._proc
        yield source
    finally:
        _RESULTS.mkdir(parents=True, exist_ok=True)
        before = None if proc is None else proc.exit_code()
        with as_deployment_org():
            pr.stop_endpoint(source.id)
        after = None if proc is None else proc.exit_code()
        _write(
            "pgwire-exit-status.txt",
            f"exit code before the stop (None: still running): {before}\n"
            f"exit code after the stop (SIGTERM to its process group): {after}\n",
        )
        if _DEBUG_LOG.is_file():
            _write("pgwire-debug.log", _DEBUG_LOG.read_text(errors="replace"))
        else:
            _write("pgwire-debug.log", f"(no {_DEBUG_LOG} was written)\n")
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


_TRINO_LOG_PROPERTIES = "io.trino.plugin.jdbc=DEBUG\nio.trino.plugin.postgresql=DEBUG\n"
_TRINO_READY_SECONDS = 300


def _trino_container() -> str:
    ids = subprocess.run(
        ["docker", "compose", *_ITEST_COMPOSE_ARGS, "ps", "-q", "trino"],
        cwd=_REPO_ROOT,
        env=_itest_compose_env(),
        check=True,
        capture_output=True,
        text=True,
    ).stdout.split()
    assert len(ids) == 1, f"one trino container expected, found {ids}"
    return ids[0]


def _await_trino() -> None:
    import trino.dbapi

    deadline = time.monotonic() + _TRINO_READY_SECONDS
    while True:
        try:
            conn = trino.dbapi.connect(host=_TRINO_HOST, port=_TRINO_PORT, user="itest")
            cur = conn.cursor()
            cur.execute("SELECT 1")
            cur.fetchall()
            conn.close()
            return
        except Exception:  # noqa: BLE001 - starting up; the deadline below ends the wait
            if time.monotonic() >= deadline:
                raise
            time.sleep(3)


@pytest.fixture(scope="module")
def trino_debug():
    """This run's Trino with ``log.properties`` at DEBUG for the JDBC and PostgreSQL plugins:
    copied into the container (compose mounts none) and the container restarted, as a log level
    is read at start. Yields the container id and the time Trino was ready."""
    container = _trino_container()
    props = _RESULTS / "trino-log.properties"
    _RESULTS.mkdir(parents=True, exist_ok=True)
    props.write_text(_TRINO_LOG_PROPERTIES)
    subprocess.run(
        ["docker", "cp", str(props), f"{container}:/etc/trino/log.properties"], check=True
    )
    restarted = datetime.now(timezone.utc)
    subprocess.run(["docker", "restart", container], check=True, capture_output=True)
    _await_trino()
    yield container, restarted
    logs = subprocess.run(
        ["docker", "logs", "--timestamps", "--since", restarted.isoformat(), container],
        capture_output=True,
        text=True,
    )
    _write("trino-server.log", logs.stdout + logs.stderr)


@pytest.fixture(scope="module")
def catalog(served, trino_debug):
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
        yield cur, source, trino_debug
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
_RESULTS = Path("results") / "warehouse" / "calcite-1416-probe"
_LOG_CONTEXT_LINES = 40


def _server_log_lines(source) -> list[str]:
    from provisa.federation import pgwire_replica as pr

    with as_deployment_org():
        server = pr._endpoint_replica(source)._server
    assert server is not None, "the source's server was not started"
    return (server._bundle_dir / pr.SERVER_LOG_NAME).read_text(errors="replace").splitlines()


def _query_failure(query_id: str) -> dict:
    """The failed query's failure as Trino's query API holds it: type, message, error code,
    stack trace, and each cause in turn."""
    req = urllib.request.Request(
        f"http://{_TRINO_HOST}:{_TRINO_PORT}/v1/query/{query_id}",
        headers={"X-Trino-User": "itest"},
    )
    with urllib.request.urlopen(req, timeout=30) as resp:  # noqa: S310 - this run's own Trino
        info = json.loads(resp.read())
    return {
        "queryId": query_id,
        "query": info.get("query"),
        "state": info.get("state"),
        "errorCode": info.get("errorCode"),
        "failureInfo": info.get("failureInfo"),
    }


def _failure_text(failure: dict | None, depth: int = 0) -> str:
    if not failure:
        return ""
    pad = "  " * depth
    lines = [f"{pad}{failure.get('type')}: {failure.get('message')}"]
    lines += [f"{pad}    at {frame}" for frame in failure.get("stack", [])]
    for suppressed in failure.get("suppressed", []):
        lines.append(f"{pad}  Suppressed:")
        lines.append(_failure_text(suppressed, depth + 1))
    if failure.get("cause"):
        lines.append(f"{pad}Caused by:")
        lines.append(_failure_text(failure["cause"], depth + 1))
    return "\n".join(lines)


def _attempt(cur, sql: str) -> dict:
    """Run ``sql`` and say what happened: its rows, or its error and Trino's whole failure."""
    import trino.exceptions

    started = datetime.now(timezone.utc).isoformat()
    try:
        cur.execute(sql)
        rows = cur.fetchall()
        return {"sql": sql, "started": started, "ok": True, "rows": rows}
    except trino.exceptions.TrinoQueryError as exc:
        failure = _query_failure(exc.query_id)
        return {
            "sql": sql,
            "started": started,
            "ok": False,
            "error_name": exc.error_name,
            "message": exc.message,
            "failure": failure,
            "stack": _failure_text(failure["failureInfo"]),
        }


def _write(name: str, text: str) -> None:
    (_RESULTS / name).write_text(text)


def _summary(result: dict) -> str:
    if result["ok"]:
        return f"ok, {len(result['rows'])} rows: {result['rows']}"
    return f"FAILED {result['error_name']}: {result['message']}"


def test_trino_describe_of_each_probed_table(catalog):
    cur, source, (container, _restarted) = catalog
    _RESULTS.mkdir(parents=True, exist_ok=True)

    before = len(_server_log_lines(source))
    start = max(0, before - _LOG_CONTEXT_LINES)
    debug_before = (
        len(_DEBUG_LOG.read_text(errors="replace").splitlines()) if _DEBUG_LOG.is_file() else 0
    )
    results: dict[str, dict[str, dict]] = {}
    report: list[str] = []
    first_window_written = False
    for schema, table in _PROBED:
        name = f"{schema}.{table}"
        pattern = "inflation%" if (schema, table) == _PROBED[0] else table
        attempts = {
            "describe": _attempt(cur, f"DESCRIBE {_CATALOG}.{schema}.{table}"),
            "show_tables": _attempt(cur, f"SHOW TABLES FROM {_CATALOG}.{schema} LIKE '{pattern}'"),
            "select_limit_1": _attempt(cur, f"SELECT * FROM {_CATALOG}.{schema}.{table} LIMIT 1"),
        }
        results[name] = attempts
        for kind, result in attempts.items():
            line = f"{name} {kind}: {result['sql']}\n  -> {_summary(result)}"
            print(line)
            report.append(line)
            if not result["ok"]:
                safe = name.replace(".", "_")
                _write(f"trino-failure-{safe}-{kind}.txt", result["stack"] + "\n")
                _write(
                    f"trino-failure-{safe}-{kind}.json",
                    json.dumps(result["failure"], indent=2, default=str),
                )
        _write("results.txt", "\n".join(report) + "\n")
        if not first_window_written and not attempts["describe"]["ok"]:
            first_window_written = True
            time.sleep(2)  # the server's log handlers flush on their own schedule
            window = _server_log_lines(source)[start:]
            _write(
                "pgwire-log-around-first-describe.txt",
                f"(server log lines from {start + 1}; the first DESCRIBE was sent after line "
                f"{before})\n" + "\n".join(window) + "\n",
            )
            debug = (
                _DEBUG_LOG.read_text(errors="replace").splitlines() if _DEBUG_LOG.is_file() else []
            )
            _write(
                "pgwire-debug-around-first-failing-describe.txt",
                f"(debug log lines from {debug_before + 1}; the first DESCRIBE was sent after "
                f"line {debug_before}; window ends after {name})\n"
                + "\n".join(debug[debug_before:])
                + "\n",
            )
            since = attempts["describe"]["started"]
            logs = subprocess.run(
                ["docker", "logs", "--timestamps", "--since", since, container],
                capture_output=True,
                text=True,
            )
            _write(
                "trino-log-around-first-failing-describe.txt",
                f"(Trino's log from {since}, the first failing DESCRIBE's start)\n"
                + logs.stdout
                + logs.stderr,
            )
    _write(
        "results.json",
        json.dumps(
            {
                name: {
                    kind: {k: v for k, v in r.items() if k not in ("failure", "stack")}
                    for kind, r in attempts.items()
                }
                for name, attempts in results.items()
            },
            indent=2,
            default=str,
        ),
    )
    _write("pgwire-server.log", "\n".join(_server_log_lines(source)) + "\n")

    # The test fails only on an error; these are the outcomes it saw, one set per probed table.
    assert set(results) == {f"{s}.{t}" for s, t in _PROBED}, results
