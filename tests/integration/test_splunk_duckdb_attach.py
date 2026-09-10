# Copyright (c) 2026 Kenneth Stott
# Canary: 9d31c7a4-06be-4f52-b8d1-2f7a5c9e4183
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1694: DuckDB attaches a LIVE Splunk through the connector's bundled Calcite pgwire server.

tests/integration/test_duckdb_attach_calcite_pgwire.py proves the mechanism with the ``pgwire-file``
bundle standing in for a SaaS API, so it needs no credentials. This module runs the same mechanism
against the real thing: the ``pgwire-splunk`` bundle, a real Splunk container, a real API token, and
a real ``ATTACH ... (TYPE postgres, READ_ONLY)`` — no Trino anywhere.

Two operand keys are only exercised here. The container serves its management port with a
self-signed certificate, so the Calcite adapter cannot connect at all unless
``mapping.disable_ssl_validation`` reaches the operand as ``disableSslValidation`` (REQ-724); and
Splunk ships its own sample Data Models, so ``mapping.datamodel_filter`` reaching
``datamodelFilter`` is what makes discovery return the seeded model alone. Both were missing from
``_splunk_operand`` before REQ-1694 — a unit test pins the operand, this pins the behaviour.

The Splunk this drives is the DEMO UNIT (``demo/sources/splunk``), provisioned through
``demo/sources/provision.py`` on freshly reserved ports under its own compose project — the same
entry point the demo start and the source-to-query e2e use, so one compose file and one seeder
(index, events, the Data Model whose root object must inherit from ``BaseEvent``, the widened ACL,
the minted API token) serve all three.

NOT the itest stack's own ``splunk`` service, deliberately. That service mounts a NAMED volume
(``splunk_test_data:/opt/splunk/var``), and the heavy-service teardown's ``docker compose rm -fsv``
drops only ANONYMOUS volumes — so every run inherits the previous run's Splunk state. Driven that
way, this module's unfiltered read returned zero rows reproducibly while the identical Source
against a clean container of the same image returned all seven, cold or warm. A test that carries
state between runs cannot say which of the two answers is the product's.
"""

from __future__ import annotations

import contextlib
import importlib
import os
import socket
import subprocess
import sys
import time
from pathlib import Path
from types import ModuleType

import duckdb
import httpx
import pytest

from provisa.core.models import Source, SourceType
from provisa.federation import pgwire_replica as pr
from provisa.federation.connector_duckdb import DuckDBSplunkConnector

pytestmark = [pytest.mark.integration]

_REPO_ROOT = Path(__file__).resolve().parents[2]
_PROVISION = _REPO_ROOT / "demo" / "sources" / "provision.py"
_PROJECT_PREFIX = "provisa-itest-splunkattach"


def _reserve_ports(n: int) -> list[int]:
    """n DISTINCT free TCP ports, held together so the kernel hands out a different one each."""
    socks = [socket.socket() for _ in range(n)]
    try:
        for s in socks:
            s.bind(("127.0.0.1", 0))
        return [s.getsockname()[1] for s in socks]
    finally:
        for s in socks:
            s.close()


def _prime_module(mgmt_port: int, hec_port: int) -> ModuleType:
    """The demo unit's seeder, bound to this run's ports.

    Its ports are module-level constants read from the environment, so the environment is set
    before the import — the demo unit stays a plain script with no test-only parameter.
    """
    os.environ["PROVISA_DEMO_SPLUNK_PORT"] = str(mgmt_port)
    os.environ["PROVISA_DEMO_SPLUNK_HEC_PORT"] = str(hec_port)
    spec = importlib.util.spec_from_file_location(
        "provisa_demo_splunk_prime", _REPO_ROOT / "demo" / "sources" / "splunk" / "prime.py"
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _wait_for_model_rows(prime: ModuleType, mgmt_base: str, expected: int) -> None:
    """Block until the seeded Data Model actually returns its rows to a Splunk search.

    Splunk indexes events and builds a model's summaries asynchronously: for a window after the
    REST create, ``| datamodel <model>`` returns the model with zero rows. The Calcite adapter
    caches a discovered model for ``datamodelCacheTtl`` (60 minutes by default), so a pgwire server
    started inside that window can serve an empty table for the rest of the run. Waiting here — on
    Splunk's own search API, before the server is ever started — is what makes the read below
    deterministic rather than a race against indexing.
    """
    model = prime.MODEL
    search = f"| datamodel {model} {model} search | stats count AS n"
    deadline = time.monotonic() + 600
    seen: object = "never ran"
    with httpx.Client(verify=False, timeout=120) as client:  # noqa: S501 - self-signed test cert
        while time.monotonic() < deadline:
            r = client.post(
                f"{mgmt_base}/services/search/jobs",
                params={"output_mode": "json"},
                data={"search": search, "exec_mode": "oneshot"},
                auth=(prime.USER, prime.PASSWORD),
            )
            if r.status_code == 200:
                results = r.json().get("results", [])
                seen = results[0]["n"] if results else "no results row"
                if results and int(results[0]["n"]) == expected:
                    return
            else:
                seen = f"HTTP {r.status_code} {r.text}"
            time.sleep(10)
    raise AssertionError(f"data model {model} never returned {expected} rows (last: {seen})")


@pytest.fixture(scope="module")
def prime() -> ModuleType:
    """Provision a CLEAN Splunk through the demo unit and seed it, then remove it afterwards."""
    mgmt_port, hec_port = _reserve_ports(2)
    env = {
        **os.environ,
        "PROVISA_DEMO_SPLUNK_PORT": str(mgmt_port),
        "PROVISA_DEMO_SPLUNK_HEC_PORT": str(hec_port),
    }
    args = [
        f"--env=PROVISA_DEMO_SPLUNK_PORT={mgmt_port}",
        f"--env=PROVISA_DEMO_SPLUNK_HEC_PORT={hec_port}",
    ]
    # `up` starts the container, waits for its healthcheck and runs prime.py (the seeder).
    subprocess.run(  # noqa: S603 - args are code-built, not user input
        [sys.executable, str(_PROVISION), "up", "--prefix", _PROJECT_PREFIX, *args, "splunk"],
        check=True,
        cwd=_REPO_ROOT,
        env=env,
    )
    try:
        module = _prime_module(mgmt_port, hec_port)
        _wait_for_model_rows(module, f"https://localhost:{mgmt_port}", len(module.ALERTS))
        module.MGMT_PORT_FOR_TEST = mgmt_port  # the port the Source addresses
        yield module
    finally:
        with contextlib.suppress(subprocess.CalledProcessError):
            subprocess.run(  # noqa: S603 - args are code-built, not user input
                [
                    sys.executable,
                    str(_PROVISION),
                    "down",
                    "--prefix",
                    _PROJECT_PREFIX,
                    *args,
                    "splunk",
                ],
                check=True,
                cwd=_REPO_ROOT,
                env=env,
            )


@pytest.fixture(scope="module")
def splunk_source(prime: ModuleType) -> Source:
    """The splunk Source that addresses the seeded container, exactly as the Sources form stores
    one: host/port, the container's own admin account in ``username``/``password``, and SSL
    validation disabled for its self-signed certificate.

    Username/password rather than an API token because Splunk generates a token's value: it cannot
    be known before the container starts, while this account is fixed by the unit's compose.yml
    (REQ-1694). What the form persists is the same either way -- a password reference on the
    control-plane row (REQ-1695)."""
    return Source(
        id="splunk-duckdb-itest",
        type=SourceType.splunk,
        host="localhost",
        port=prime.MGMT_PORT_FOR_TEST,
        username=prime.USER,
        password=prime.PASSWORD,
        mapping={
            "use_token": False,
            "disable_ssl_validation": True,
            "datamodel_filter": prime.MODEL,
        },
    )


@pytest.fixture(scope="module")
def attached(splunk_source: Source):
    """The real bundle started for this source, and a DuckDB that has ATTACHed its endpoint."""
    details = DuckDBSplunkConnector().details(splunk_source)  # starts the server, waits to listen
    con = duckdb.connect()
    con.execute("INSTALL postgres")
    con.execute("LOAD postgres")
    con.execute(details["attach"])
    try:
        yield con, details
    finally:
        con.close()
        pr.stop_all_servers()


def _relation(details: dict, prime: ModuleType) -> str:
    return f'"{details["raw_alias"]}"."{details["remote_schema"]}"."{prime.MODEL}"'


def test_attach_details_name_the_live_endpoint_and_schema(splunk_source: Source, prime, attached):
    _con, details = attached
    assert details["raw_alias"] == "_src_splunk-duckdb-itest"
    assert details["remote_schema"] == "splunk_duckdb_itest"
    assert details["attach"].endswith('AS "_src_splunk-duckdb-itest" (TYPE postgres, READ_ONLY)')
    # The operand the server was configured from carries both REQ-1694 keys.
    operand = pr.build_model_json(splunk_source)["schemas"][0]["operand"]
    assert operand["disableSslValidation"] is True
    assert operand["datamodelFilter"] == prime.MODEL
    # mapping.use_token is false, so the credential reaches Calcite as a username/password pair
    # rather than a token — the same branch the Sources form's "Username / Password" mode drives.
    assert "token" not in operand
    assert operand["username"] == splunk_source.username
    assert operand["password"] == splunk_source.password


def test_duckdb_lists_the_seeded_data_model_as_a_table(prime, attached):
    """The Calcite adapter's discovery — one table per Splunk Data Model — reaches DuckDB's
    catalog through the attached endpoint. This is what Register Table lists."""
    con, details = attached
    rows = con.execute(
        "SELECT table_schema, table_name FROM information_schema.tables WHERE table_catalog = ?",
        [details["raw_alias"]],
    ).fetchall()
    assert (details["remote_schema"], prime.MODEL) in rows


def test_duckdb_reads_live_rows(prime, attached):
    """Reading the attached relation IS reading Splunk — nothing is landed. The rows are the seven
    the demo seeder sent over HEC, and the aggregate goes down to Calcite.

    No predicate is asserted here. Every WHERE DuckDB pushes into this bundle comes back empty,
    verified live against a correctly typed column (``alert_id`` is INTEGER in the attached
    catalog): a string comparison is rejected outright, because DuckDB's postgres scanner appends
    ``COLLATE "C"`` and Calcite's parser has no COLLATE ("Encountered \"COLLATE\""), and a numeric
    one is pushed as ``alert_id > '4'`` and matches nothing — even when written as
    ``CAST(alert_id AS INTEGER) > 4``. Unfiltered reads and ``count(*)`` are correct, so this
    asserts those. Predicate pushdown is a defect in the bundled connector, reported separately;
    asserting the empty result it currently returns would pin the bug as if it were the contract.
    """
    con, details = attached
    rel = _relation(details, prime)
    expected = [(a, t, n) for a, t, n in prime.ALERTS]
    # The model already returns its rows to Splunk itself (the prime fixture waited for that), so
    # this poll only absorbs the connector's own first-query warm-up, not Splunk's indexing.
    deadline = time.monotonic() + 300
    last: object = None
    while True:
        try:
            rows = con.execute(
                f"SELECT alert_id, alert_type, animal_name FROM {rel} ORDER BY alert_id"
            ).fetchall()
        except duckdb.Error as exc:  # the attached connector is not warm yet
            rows, last = [], exc
        if len(rows) == len(expected):
            break
        assert time.monotonic() < deadline, (
            f"expected {len(expected)} seeded rows, got {rows!r} (last error: {last!r})"
        )
        time.sleep(5)
    assert [(int(r[0]), r[1], r[2]) for r in rows] == expected

    assert con.execute(f"SELECT count(*) FROM {rel}").fetchone() == (len(expected),)
    # The connector's own typing reaches DuckDB's catalog: the model's declared number is an
    # INTEGER column, alongside Splunk's auto-merged core event fields.
    cols = dict(
        con.execute(
            "SELECT column_name, data_type FROM information_schema.columns "
            "WHERE table_catalog = ? AND table_name = ?",
            [details["raw_alias"], prime.MODEL],
        ).fetchall()
    )
    assert cols["alert_id"] == "INTEGER"
    assert cols["alert_type"] == "VARCHAR" and cols["animal_name"] == "VARCHAR"
    assert cols["time"] == "TIMESTAMP" and cols["index"] == "VARCHAR"


def test_attach_is_read_only(prime, attached):
    con, details = attached
    with pytest.raises(duckdb.Error):
        con.execute(f"INSERT INTO {_relation(details, prime)} VALUES (99, 'nope', 'nope')")
