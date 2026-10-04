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
  3. The connector exposes a single data schema, ``sharepoint``; each live SharePoint list is a
     TABLE under it (the calcite-sharepoint-list plugin: ``SharePointListSchema`` holds one
     ``SharePointListTable`` per list, keyed by ``SharePointNameConverter.toSqlName(displayName)``
     — lowercased, spaces/hyphens -> underscores). Querying ``<catalog>.sharepoint.<list>`` via
     ``trino.dbapi`` then reads live from the SharePoint site through the plugin — no data is
     landed in Provisa's own store.

Credentials
-----------
There is no SharePoint emulator to self-provision: a real Microsoft 365 tenant and site are
required, so this runs in the warehouse lane. It reads the SP_* variables the lane and ``.env``
set (the names ``tests/steps/steps_sharepoint_connector.py`` uses): SP_SITE_URL, SP_TENANT_ID,
SP_CLIENT_ID and SP_AUTH_TYPE, plus SP_CLIENT_SECRET for CLIENT_CREDENTIALS or the certificate for
CERTIFICATE. The connector runs inside the Trino container, where compose mounts the certificate
at ``/certs/sharepoint.pfx`` from ``./sharepoint.pfx`` (the lane writes it there); SP_CERT_PASSWORD
is optional, a password-less PFX has none.
"""

from __future__ import annotations

import os
import time

import pytest
import trino.dbapi
import trino.exceptions

pytestmark = [
    pytest.mark.integration,
    pytest.mark.requires_sharepoint,
    pytest.mark.requires_warehouse,
]

_TRINO_HOST = os.environ.get("TRINO_HOST", "localhost")
_TRINO_PORT = int(os.environ.get("TRINO_PORT", "8080"))

# The list the connector must expose as a table. Defaults to the built-in "Documents" library,
# which exists on every SharePoint site (toSqlName("Documents") == "documents") — a stable,
# always-present target that does not depend on operator-created test fixtures. Override with
# SHAREPOINT_TEST_LIST to assert a specific operator-managed list.
_LIST_NAME = os.environ.get("SHAREPOINT_TEST_LIST", "documents")
# REQ-1730: TrinoSharePointConnector.details passes the sql-normalized source id as the plugin's
# `schema` property, so the data schema is that id, not the plugin's old literal "sharepoint".
_SOURCE_ID = "sharepoint-itest"
_DATA_SCHEMA = _SOURCE_ID.replace("-", "_")


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
    an existing list on the target site (defaults to the built-in Documents library). The read
    assertion is intentionally structural (the list shows up as a Trino TABLE under the
    ``sharepoint`` schema — the connector's live list enumeration) rather than asserting specific
    row content, since list contents are operator-managed on the live tenant, not test-owned.
    """
    pytest.importorskip("trino")
    from provisa.core.catalog import create_catalog
    from provisa.core.models import Source, SourceType

    conn, cur = _trino_cursor()

    catalog = "sharepoint_itest"
    _drop(cur, catalog)

    auth_type = os.environ[
        "SP_AUTH_TYPE"
    ].upper()  # .env writes "certificate"; the connector takes CERTIFICATE
    mapping: dict = {"auth_type": auth_type}
    secret = ""
    if auth_type == "CERTIFICATE":
        # The in-container path compose mounts ./sharepoint.pfx at (docker-compose.core.yml).
        mapping["certificate_path"] = "/certs/sharepoint.pfx"
        mapping["certificate_password"] = os.environ.get(
            "SP_CERT_PASSWORD", ""
        )  # none on a password-less PFX
    else:
        secret = os.environ["SP_CLIENT_SECRET"]

    src = Source(
        id=_SOURCE_ID,
        type=SourceType.sharepoint,
        base_url=os.environ["SP_SITE_URL"],
        username=os.environ["SP_CLIENT_ID"],
        password=secret,
        database=os.environ["SP_TENANT_ID"],
        mapping=mapping,
    )
    try:
        create_catalog(conn, src, secret)

        # Querying SHOW TABLES through Trino IS reading through the federation engine — the
        # sharepoint connector enumerates live SharePoint lists as TABLES under its single
        # ``sharepoint`` schema (getAvailableLists -> Graph); nothing is landed. SHOW SCHEMAS
        # would NOT prove auth: the schema names (sharepoint/metadata/information_schema/
        # pg_catalog) are static and returned without any Graph call.
        tables: set = set()
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            try:
                cur.execute(f"SHOW TABLES FROM {catalog}.{_DATA_SCHEMA}")
                tables = {r[0] for r in cur.fetchall()}
            except trino.exceptions.TrinoExternalError:
                tables = set()  # catalog freshly created; connector may not be warm yet
            if tables:
                break
            time.sleep(2)

        assert _LIST_NAME.lower() in {t.lower() for t in tables}, (
            f"Expected list {_LIST_NAME!r} among SharePoint lists (tables under "
            f"{_DATA_SCHEMA!r}), got {sorted(tables)}"
        )
    finally:
        _drop(cur, catalog)
        conn.close()


if __name__ == "__main__":
    pytest.main([__file__, "-q"])
