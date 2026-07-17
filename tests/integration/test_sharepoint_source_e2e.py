# Copyright (c) 2026 Kenneth Stott
# Canary: 9b1e6c4d-3a72-4f58-8e0d-1c9a5b6e2f70
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""E2E: SharePoint as a connector-source, read through the Provisa federation engine (REQ-1097).

Same connector-bucket pattern as ``test_cassandra_source_e2e.py`` (read its module docstring for the
full catalog-seam explanation). SharePoint has NO direct driver in
``provisa/executor/drivers/registry.py``; it is reachable ONLY through the federation engine's Trino
``sharepoint`` catalog, a project-authored Calcite-based connector plugin (NOT a stock Trino
connector):

  1. A ``Source`` row (type=sharepoint) declares the SharePoint site + auth. Fields, per
     ``provisa/federation/trino_connectors.py:223`` ``TrinoSharepointConnector.details()``:
       - ``base_url`` (or ``host``) -> ``site-url``
       - ``username`` -> ``client-id``
       - ``password`` -> ``client-secret``
       - ``database`` -> ``tenant-id``
       - ``mapping.auth_type`` -> ``auth-type`` (defaults to ``CLIENT_CREDENTIALS``)
       - ``mapping.certificate_path`` / ``mapping.certificate_password`` -> certificate auth,
         alternative to a client secret.
  2. ``provisa.core.catalog.create_catalog`` looks up "sharepoint" in
     ``provisa.federation.trino_connectors.TRINO_CONNECTORS``, builds the catalog ``.properties``
     via the above, and issues ``CREATE CATALOG ... USING sharepoint WITH (...)`` against the live
     Trino coordinator.
  3. Querying ``<catalog>.<schema>`` (a SharePoint list) via ``trino.dbapi`` then reads live from
     the SharePoint site through Trino's sharepoint plugin — no data is landed in Provisa's own
     store.

Why this is credential-gated, not docker-gated
-------------------------------------------------
There is no SharePoint emulator/self-hosted target to self-provision — a real Microsoft 365 tenant
+ site is required. The trino-sharepoint plugin jar and a client cert (``sharepoint.pfx``, repo
root) ARE present and already mounted into the ``trino``/``trino-worker`` services by
``docker-compose.core.yml`` (lines ~85-88, ~119-122), and ``.env`` sets ``SP_SITE_URL`` +
``SP_CERT_PATH`` (see ``tests/steps/steps_sharepoint_connector.py`` for the same site,
``kenstott.sharepoint.com``, exercised as unit-level Source/catalog-properties checks). What is
MISSING from this environment is the Azure AD app registration identity needed to actually
authenticate: no tenant id and no client id/secret (or certificate password) are configured — the
cert file alone is not a runnable credential. This test is unconditionally skipped unless ALL of
``SHAREPOINT_SITE_URL``/``SHAREPOINT_TENANT_ID``/``SHAREPOINT_CLIENT_ID`` are set AND at least one
of ``SHAREPOINT_CLIENT_SECRET`` or (``SHAREPOINT_CERT_PATH`` + ``SHAREPOINT_CERT_PASSWORD``) is set
— so it SKIPS here. Not added to ``tests/conftest.py::_MARKER_SERVICES`` — there is no docker
service for the provisioner to bring up beyond the already-always-running core Trino, which reaches
out to the real Microsoft 365 endpoint over the network.
"""

from __future__ import annotations

import os
import time

import pytest
import trino.dbapi
import trino.exceptions

pytestmark = [pytest.mark.integration, pytest.mark.requires_sharepoint]

_TRINO_HOST = os.environ.get("TRINO_HOST", "localhost")
_TRINO_PORT = int(os.environ.get("TRINO_PORT", "8080"))

_REQUIRED = ("SHAREPOINT_SITE_URL", "SHAREPOINT_TENANT_ID", "SHAREPOINT_CLIENT_ID")
_HAVE_IDENTITY = all(os.environ.get(v) for v in _REQUIRED)
_HAVE_SECRET = bool(os.environ.get("SHAREPOINT_CLIENT_SECRET"))
_HAVE_CERT = bool(os.environ.get("SHAREPOINT_CERT_PATH")) and bool(
    os.environ.get("SHAREPOINT_CERT_PASSWORD")
)
_HAVE_CREDS = _HAVE_IDENTITY and (_HAVE_SECRET or _HAVE_CERT)
pytestmark.append(
    pytest.mark.skipif(
        not _HAVE_CREDS,
        reason=(
            "No live SharePoint site credentials configured (repo has sharepoint.pfx + "
            "SP_SITE_URL in .env, but no Azure AD tenant/client id or client secret/cert "
            "password — the cert alone is not a runnable credential); set "
            "SHAREPOINT_SITE_URL/SHAREPOINT_TENANT_ID/SHAREPOINT_CLIENT_ID plus "
            "SHAREPOINT_CLIENT_SECRET or SHAREPOINT_CERT_PATH+SHAREPOINT_CERT_PASSWORD to run "
            "against a real site"
        ),
    )
)

_LIST_NAME = os.environ.get("SHAREPOINT_TEST_LIST", "ProvisaWidgetsE2E")


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


def test_sharepoint_catalog_created_and_lists_visible():
    """Register a sharepoint Source, project it as a live Trino catalog, confirm the target list
    is enumerable end-to-end.

    Drives the REAL registration path: provisa.core.catalog.create_catalog builds the catalog
    properties from TrinoSharepointConnector.details() (site-url/auth-type/client-id/client-secret
    or certificate-path/certificate-password/tenant-id) and issues CREATE CATALOG against the live
    Trino coordinator. This does NOT seed data into SharePoint — creating/populating a SharePoint
    list requires Graph/REST calls out of scope for this test; ``SHAREPOINT_TEST_LIST`` must name
    an existing list on the target site. The read assertion is intentionally structural (the list
    shows up as a Trino schema) rather than asserting specific row content, since list contents are
    operator-managed on the live tenant, not test-owned.
    """
    pytest.importorskip("trino")
    from provisa.core.catalog import create_catalog
    from provisa.core.models import Source, SourceType

    conn, cur = _trino_cursor()

    catalog = "sharepoint_itest"
    _drop(cur, catalog)

    mapping: dict = {}
    if _HAVE_SECRET:
        mapping["auth_type"] = "CLIENT_CREDENTIALS"
    else:
        mapping["auth_type"] = "CLIENT_CREDENTIALS"
        mapping["certificate_path"] = os.environ["SHAREPOINT_CERT_PATH"]
        mapping["certificate_password"] = os.environ["SHAREPOINT_CERT_PASSWORD"]

    src = Source(
        id="sharepoint-itest",
        type=SourceType.sharepoint,
        base_url=os.environ["SHAREPOINT_SITE_URL"],
        username=os.environ["SHAREPOINT_CLIENT_ID"],
        password=os.environ.get("SHAREPOINT_CLIENT_SECRET", ""),
        database=os.environ["SHAREPOINT_TENANT_ID"],
        mapping=mapping,
    )
    try:
        create_catalog(conn, src, os.environ.get("SHAREPOINT_CLIENT_SECRET", ""))

        # Querying SHOW SCHEMAS through Trino IS reading through the federation engine — Trino's
        # sharepoint connector enumerates live SharePoint lists on the site; nothing is landed.
        schemas: set = set()
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            try:
                cur.execute(f"SHOW SCHEMAS FROM {catalog}")
                schemas = {r[0] for r in cur.fetchall()}
            except trino.exceptions.TrinoExternalError:
                schemas = set()  # catalog freshly created; connector may not be warm yet
            if schemas:
                break
            time.sleep(2)

        assert _LIST_NAME.lower() in {s.lower() for s in schemas}, (
            f"Expected list {_LIST_NAME!r} among SharePoint schemas, got {sorted(schemas)}"
        )
    finally:
        _drop(cur, catalog)
        conn.close()


if __name__ == "__main__":
    pytest.main([__file__, "-q"])
