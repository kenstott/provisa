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

pytestmark = [
    pytest.mark.integration,
    pytest.mark.requires_sharepoint,
    pytest.mark.requires_warehouse,
]

_TRINO_HOST = os.environ.get("TRINO_HOST", "localhost")
_TRINO_PORT = int(os.environ.get("TRINO_PORT", "8080"))

_REQUIRED = ("SHAREPOINT_SITE_URL", "SHAREPOINT_TENANT_ID", "SHAREPOINT_CLIENT_ID")


def _present(name: str) -> bool:
    """A credential var counts only when it is set AND non-empty -- an empty value is as good as
    unset, so the test is skipped by name rather than run against M365 with a half credential."""
    return bool(os.environ.get(name))


# What a full credential is missing, by name: the identity trio, plus a secret OR a cert+password.
_MISSING_IDENTITY = [v for v in _REQUIRED if not _present(v)]
_HAVE_SECRET = _present("SHAREPOINT_CLIENT_SECRET")
_HAVE_CERT = _present("SHAREPOINT_CERT_PATH") and _present("SHAREPOINT_CERT_PASSWORD")
_HAVE_CREDS = not _MISSING_IDENTITY and (_HAVE_SECRET or _HAVE_CERT)

if _HAVE_CREDS:
    _SKIP_REASON = ""
else:
    _missing: list[str] = list(_MISSING_IDENTITY)
    if not (_HAVE_SECRET or _HAVE_CERT):
        # Name exactly which auth material is absent (an empty cert password reads as absent).
        _cert_bits = [
            n for n in ("SHAREPOINT_CERT_PATH", "SHAREPOINT_CERT_PASSWORD") if not _present(n)
        ]
        _missing.append(
            "SHAREPOINT_CLIENT_SECRET or (" + " + ".join(_cert_bits or ["SHAREPOINT_CERT_*"]) + ")"
        )
    _SKIP_REASON = (
        "No live SharePoint credentials — missing (empty counts as unset): "
        + ", ".join(_missing)
        + ". Set the Azure AD app identity plus a client secret or cert+password to run against "
        "a real M365 site."
    )
pytestmark.append(pytest.mark.skipif(not _HAVE_CREDS, reason=_SKIP_REASON))

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

    mapping: dict = {}
    if _HAVE_SECRET:
        mapping["auth_type"] = "CLIENT_CREDENTIALS"
    else:
        # Certificate app-only auth. The connector runs INSIDE the Trino container, where compose
        # mounts the pfx at a fixed path (docker-compose.core.yml: ./sharepoint.pfx:/certs/sharepoint.pfx);
        # certificate-path must therefore be that in-container path, NOT the host path in
        # SHAREPOINT_CERT_PATH (which only gates the skip — proving a real cert exists to be mounted).
        # Matches the CERTIFICATE contract in trino/catalog-install/e2e_sharepoint.properties and the
        # steps_sharepoint_connector BDD assertion.
        mapping["auth_type"] = "CERTIFICATE"
        mapping["certificate_path"] = "/certs/sharepoint.pfx"
        mapping["certificate_password"] = os.environ["SHAREPOINT_CERT_PASSWORD"]

    src = Source(
        id=_SOURCE_ID,
        type=SourceType.sharepoint,
        base_url=os.environ["SHAREPOINT_SITE_URL"],
        username=os.environ["SHAREPOINT_CLIENT_ID"],
        password=os.environ.get("SHAREPOINT_CLIENT_SECRET", ""),
        database=os.environ["SHAREPOINT_TENANT_ID"],
        mapping=mapping,
    )
    try:
        create_catalog(conn, src, os.environ.get("SHAREPOINT_CLIENT_SECRET", ""))

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
