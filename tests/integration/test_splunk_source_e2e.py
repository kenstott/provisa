# Copyright (c) 2026 Kenneth Stott
# Canary: b2380efb-e3ed-45bd-81a4-af8571899b22
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""E2E: Splunk as a connector-source, read through the Provisa federation engine (REQ-1097).

Same connector-source shape as tests/integration/test_cassandra_source_e2e.py (see that module's
docstring for the general "catalog seam" explanation) — Splunk has no direct driver in
provisa/executor/drivers/registry.py, only a Trino connector entry
(provisa/federation/trino_connectors.py:255 TrinoSplunkConnector), so it is reachable ONLY by
projecting it as a live Trino catalog (``connector.name = splunk``, a real custom Trino plugin at
trino/plugins/trino-splunk/ wrapping Apache Calcite's ``org.apache.calcite.adapter.splunk``
adapter) and querying through Trino.

TrinoSplunkConnector.details() builds the catalog .properties from ``url``, ``token`` OR
``user``+``password`` (per ``mapping["use_token"]``), optional ``app``/``datamodel-filter``, and
``case-insensitive-name-matching=true`` — this test drives the real ``create_catalog`` path so any
drift between those property names and the plugin's JDBC connection-string parser would surface as
a live catalog-creation or query failure, not a mocked assertion.

How the Calcite Splunk adapter exposes "tables" (empirically determined, not assumed)
----------------------------------------------------------------------------------------
TrinoSplunkConnector never sets ``customTables``, so ``SplunkSchemaFactory.create()`` only runs
its dynamic-discovery path: one Trino/Calcite table per Splunk **Data Model** (``GET
/services/data/models``), normalized to lowercase/underscores, with Splunk's core event fields
(``time``, ``host``, ``source``, ``sourcetype``, ``index``) auto-merged with the model's own
declared fields. There is no way to expose a bare index as a queryable table through this
connector — a Data Model is required. This was confirmed live: ``SHOW TABLES FROM
<catalog>.splunk`` lists Splunk's own shipped sample models (``internal_audit_logs``,
``internal_server``) plus whatever Data Model this test seeds.

A genuine product-adjacent gotcha, root-caused against the live stack (not guessed) and worked
around here in test setup — NOT a bug in ``provisa/``:
  - A hand-built Data Model JSON with ``"parentName": null`` (or omitted, or ``""``) makes the
    ``| datamodel <model> <object> search`` SPL command fail with a *misleading* error
    (``Key 'parentName' was missing from JSON document.`` for null/omitted; ``Could not load
    parent dataset ''`` for empty string) — even though the REST API happily stores and echoes
    that JSON back. Comparing against Splunk's own shipped models
    (``/opt/splunk/etc/apps/search/default/data/models/internal_audit_logs.json`` inside the
    container) showed every root event object uses ``"parentName": "BaseEvent"`` — Splunk's
    built-in root event dataset — never ``null``. Root objects must inherit from ``BaseEvent``
    (or another real base dataset), not leave the field null/empty. This test's seeded model uses
    ``"parentName": "BaseEvent"``.
  - A freshly REST-created Data Model defaults to ``"sharing": "app"`` with ``perms.read:
    ["admin"]`` (visible from ``.../data/models/<model>/acl``). Querying it through the
    Splunk-owned service account used by the JDBC layer can still fail with "Data model '<model>'
    is not accessible. You may lack permissions..." until its ACL is explicitly widened
    (``sharing=global``, ``perms.read=*``) via ``POST .../data/models/<model>/acl`` — done here in
    the seed helper.
  - The constraint's search key is ``"search"`` (matches the shipped models), not ``"query"``.

Splunk REST/HEC recipe (empirically verified against a live container, not assumed)
----------------------------------------------------------------------------------------
  1. Disable TLS on the global HTTP Event Collector settings (``POST
     .../data/inputs/http/http`` with ``enableSSL=0``) — simplifies event ingestion to plain
     ``http://`` on the HEC port; confirmed live (a ``https://`` POST to the HEC port failed with
     a low-level TLS connect error once this was set, `http://` succeeded).
  2. Create the test index (``POST /servicesNS/admin/search/data/indexes``).
  3. Create a HEC token scoped to that index (``POST
     .../splunk_httpinput/data/inputs/http``), read back the generated token value.
  4. POST 3 JSON events to ``http://splunk:8088/services/collector`` with
     ``Authorization: Splunk <token>``.
  5. Create the Data Model (``POST .../data/models`` with ``name`` + ``eai:data`` as a JSON
     string) — object's ``parentName: "BaseEvent"``, constraint key ``search`` (see above).
  6. Widen the Data Model's ACL to global/``*`` read (see above).

Splunk's own indexing + Data Model summary generation is eventually consistent — the seed helper
and the test's polling loop both retry generously to absorb that, on top of the ~2.5-3 minute
splunk/splunk:latest boot-to-healthy time already covered by the compose healthcheck's
``start_period``.
"""

from __future__ import annotations

import json
import os
import subprocess
import time

import httpx
import pytest
import trino.dbapi
import trino.exceptions

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope="session")]

_TRINO_HOST = os.environ.get("TRINO_HOST", "localhost")
_TRINO_PORT = int(os.environ.get("TRINO_PORT", "8080"))
_ITEST_PROJECT = os.environ.get("PROVISA_ITEST_PROJECT", "provisa-itest")

_SPLUNK_MGMT_PORT = int(os.environ.get("SPLUNK_MGMT_PORT", "8089"))
_SPLUNK_HEC_PORT = int(os.environ.get("SPLUNK_HEC_PORT", "8088"))
_SPLUNK_USER = "admin"
_SPLUNK_PASSWORD = "Provisa_2026!"

_INDEX = "provisa_itest"
_SOURCETYPE = "_json"
_HEC_TOKEN_NAME = "provisa_itest_token"
_MODEL = "provisa_itest_model"
_WIDGETS = [(1, "Widget A"), (2, "Widget B"), (3, "Widget C")]


@pytest.fixture(scope="module", autouse=True)
def _wait_for_trino():
    """Wait for Trino to finish initializing before running Trino tests."""
    deadline = time.monotonic() + 120
    while time.monotonic() < deadline:
        try:
            conn = trino.dbapi.connect(
                host=_TRINO_HOST, port=_TRINO_PORT, user="itest", catalog="system"
            )
            cur = conn.cursor()
            cur.execute("SELECT 1")
            cur.fetchall()
            conn.close()
            return
        except Exception:
            time.sleep(2)
    raise RuntimeError("Trino did not become ready within 120s")


def _trino_cursor():
    conn = trino.dbapi.connect(host=_TRINO_HOST, port=_TRINO_PORT, user="itest", catalog="system")
    cur = conn.cursor()
    cur.execute("SELECT 1")
    cur.fetchall()
    return conn, cur


def _drop(cur, name):
    try:
        cur.execute(f"DROP CATALOG {name}")
        cur.fetchall()
    except Exception:
        pass


def _splunk_container_id() -> str:
    """The running `splunk` service's container id, found by compose labels (no fixed name — the
    isolated stack never uses container_name, so it never collides with a parallel run)."""
    out = subprocess.run(
        [
            "docker",
            "ps",
            "-q",
            "--filter",
            f"label=com.docker.compose.project={_ITEST_PROJECT}",
            "--filter",
            "label=com.docker.compose.service=splunk",
        ],  # fmt: skip
        capture_output=True,
        text=True,
        check=True,
    ).stdout.strip()
    ids = out.splitlines()
    if not ids:
        raise RuntimeError(f"No running splunk container for project {_ITEST_PROJECT!r}")
    return ids[0]


def _mgmt_base() -> str:
    return f"https://localhost:{_SPLUNK_MGMT_PORT}"


def _wait_for_splunk_mgmt_api() -> None:
    """Splunk's own healthcheck (checkstate.sh) only reports splunkd process health, not that the
    REST management API is actually answering requests yet — poll it directly."""
    deadline = time.monotonic() + 180
    last_err: Exception | None = None
    with httpx.Client(verify=False, timeout=10) as client:  # noqa: S501 - self-signed test cert
        while time.monotonic() < deadline:
            try:
                r = client.get(
                    f"{_mgmt_base()}/services/server/info",
                    params={"output_mode": "json"},
                    auth=(_SPLUNK_USER, _SPLUNK_PASSWORD),
                )
                if r.status_code == 200:
                    return
            except Exception as e:
                last_err = e
            time.sleep(3)
    raise RuntimeError(f"Splunk management API never became ready: {last_err!r}")


def _seed_splunk() -> str:
    """Build the full Splunk-side fixture: HEC (plain HTTP), a test index, 3 seeded events, and a
    Data Model exposing them as a Calcite/Trino table (see module docstring for why a Data Model
    is required, and for the parentName/ACL gotchas root-caused against the live container).

    Returns the HEC token value used to seed events.
    """
    _wait_for_splunk_mgmt_api()

    with httpx.Client(verify=False, timeout=30) as client:  # noqa: S501 - self-signed test cert
        auth = (_SPLUNK_USER, _SPLUNK_PASSWORD)

        # 1. Disable TLS on the global HEC listener — plain http:// on the HEC port (verified live:
        # https:// to this port hard-fails the connection once enableSSL=0 is set).
        r = client.post(
            f"{_mgmt_base()}/servicesNS/nobody/splunk_httpinput/data/inputs/http/http",
            params={"output_mode": "json"},
            data={"enableSSL": "0", "disabled": "0"},
            auth=auth,
        )
        assert r.status_code in (200, 201), f"HEC global config failed: {r.status_code} {r.text}"

        # 2. Create the test index (retry: management API can 503 briefly after first responding).
        deadline = time.monotonic() + 60
        last_resp = None
        while time.monotonic() < deadline:
            r = client.post(
                f"{_mgmt_base()}/servicesNS/admin/search/data/indexes",
                params={"output_mode": "json"},
                data={"name": _INDEX, "datatype": "event"},
                auth=auth,
            )
            last_resp = r
            if r.status_code in (200, 201) or "already exists" in r.text:
                break
            time.sleep(3)
        else:
            raise RuntimeError(f"index creation never succeeded: {last_resp}")

        # 3. Create a HEC token scoped to that index.
        r = client.post(
            f"{_mgmt_base()}/servicesNS/nobody/splunk_httpinput/data/inputs/http",
            params={"output_mode": "json"},
            data={
                "name": _HEC_TOKEN_NAME,
                "index": _INDEX,
                "indexes": _INDEX,
                "sourcetype": _SOURCETYPE,
            },
            auth=auth,
        )
        assert r.status_code in (200, 201), f"HEC token creation failed: {r.status_code} {r.text}"
        r = client.get(
            f"{_mgmt_base()}/servicesNS/nobody/splunk_httpinput/data/inputs/http/{_HEC_TOKEN_NAME}",
            params={"output_mode": "json"},
            auth=auth,
        )
        r.raise_for_status()
        token = r.json()["entry"][0]["content"]["token"]

        # 4. Seed 3 JSON events over HEC (plain http, no TLS — see step 1).
        hec_url = f"http://localhost:{_SPLUNK_HEC_PORT}/services/collector"
        with httpx.Client(timeout=15) as hec_client:
            for wid, name in _WIDGETS:
                r = hec_client.post(
                    hec_url,
                    headers={"Authorization": f"Splunk {token}"},
                    json={
                        "index": _INDEX,
                        "sourcetype": _SOURCETYPE,
                        "event": {"widget_id": wid, "widget_name": name},
                    },
                )
                assert r.status_code == 200 and r.json().get("code") == 0, (
                    f"HEC event POST failed: {r.status_code} {r.text}"
                )

        # 5. Create the Data Model that exposes those events as a Calcite/Trino table. Root object
        # MUST inherit from Splunk's built-in "BaseEvent" dataset (see module docstring) — a null
        # or empty parentName makes the `datamodel` SPL command fail with a misleading error even
        # though the REST API accepts and echoes the JSON back unchanged.
        model_json = json.dumps(
            {
                "objects": [
                    {
                        "objectName": _MODEL,
                        "displayName": _MODEL,
                        "parentName": "BaseEvent",
                        "lineage": _MODEL,
                        "fields": [
                            {
                                "fieldName": "widget_id",
                                "type": "number",
                                "displayName": "widget_id",
                            },
                            {
                                "fieldName": "widget_name",
                                "type": "string",
                                "displayName": "widget_name",
                            },
                        ],
                        "calculations": [],
                        "constraints": [{"search": f"index={_INDEX} sourcetype={_SOURCETYPE}"}],
                    }
                ]
            }
        )
        # DELETE any leftover model from a prior failed run in this same container.
        client.delete(
            f"{_mgmt_base()}/servicesNS/nobody/search/data/models/{_MODEL}",
            params={"output_mode": "json"},
            auth=auth,
        )
        r = client.post(
            f"{_mgmt_base()}/servicesNS/nobody/search/data/models",
            params={"output_mode": "json"},
            data={"name": _MODEL, "eai:data": model_json},
            auth=auth,
        )
        assert r.status_code in (200, 201), f"data model creation failed: {r.status_code} {r.text}"

        # 6. Widen the Data Model's ACL — a freshly created model defaults to app-sharing with
        # read restricted to its creating user, which the JDBC-layer service account can fail to
        # satisfy ("Data model '<model>' is not accessible...") even with correct credentials.
        r = client.post(
            f"{_mgmt_base()}/servicesNS/nobody/search/data/models/{_MODEL}/acl",
            params={"output_mode": "json"},
            data={
                "sharing": "global",
                "owner": "nobody",
                "perms.read": "*",
                "perms.write": "admin",
            },
            auth=auth,
        )
        assert r.status_code in (200, 201), (
            f"data model ACL update failed: {r.status_code} {r.text}"
        )

    return token


@pytest.mark.requires_splunk
async def test_splunk_catalog_created_and_queryable():
    """Register a splunk Source, project it as a live Trino catalog, query it end-to-end.

    Drives the REAL registration path: provisa.core.catalog.create_catalog builds the catalog
    properties from TrinoSplunkConnector.details() and issues CREATE CATALOG against the live
    Trino coordinator — the same seam EngineRuntime.on_asset_create/reconcile/ensure_entry drive
    when a source is registered through the actual API (REQ-843). host="splunk" is the compose
    service name — Trino resolves it from inside its own container on the isolated stack's
    private network, NOT the host-published ephemeral ${SPLUNK_MGMT_PORT} (used only so this test
    process, running on the host, can reach the Splunk REST/HEC APIs to seed data).
    """
    pytest.importorskip("trino")
    from provisa.core.catalog import create_catalog
    from provisa.core.models import Source, SourceType

    _splunk_container_id()  # fail fast with a clear message if the service isn't up
    _seed_splunk()

    conn, cur = _trino_cursor()

    catalog = "splunk_itest"
    _drop(cur, catalog)
    src = Source(
        id="splunk-itest",
        type=SourceType.splunk,
        host="splunk",
        port=8089,
        username=_SPLUNK_USER,
        password=_SPLUNK_PASSWORD,
        mapping={"use_token": False, "disable_ssl_validation": True},
    )
    try:
        create_catalog(conn, src, "")

        # The catalog exposes Splunk's Data Models as Trino tables (see module docstring) —
        # confirmed live: no bare-index table exists through this connector, only Data Models.
        cur.execute(f"SHOW SCHEMAS FROM {catalog}")
        schemas = {r[0] for r in cur.fetchall()}
        assert "splunk" in schemas

        cur.execute(f"SHOW TABLES FROM {catalog}.splunk")
        tables = {r[0] for r in cur.fetchall()}
        assert _MODEL in tables

        # Querying <catalog>.splunk.<model> through Trino IS reading through the federation
        # engine — Trino's splunk connector (Calcite Data-Model discovery) reads live from
        # Splunk; nothing is landed. Splunk's own indexing + data-model summary generation is
        # eventually consistent, so retry generously.
        rows: list = []
        deadline = time.monotonic() + 90
        while time.monotonic() < deadline:
            try:
                cur.execute(
                    f"SELECT widget_id, widget_name FROM {catalog}.splunk.{_MODEL} ORDER BY widget_id"
                )
                rows = cur.fetchall()
            except trino.exceptions.TrinoExternalError:
                rows = []  # catalog/data-model freshly created; connector may not be warm yet
            if len(rows) == len(_WIDGETS):
                break
            time.sleep(3)

        assert sorted((r[0], r[1]) for r in rows) == _WIDGETS
    finally:
        _drop(cur, catalog)
        conn.close()
