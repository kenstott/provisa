# Copyright (c) 2026 Kenneth Stott
# Canary: 3c7a1e58-2d94-4b06-9f31-8ad5c0e6b742
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1693: DuckDB reads a LIVE SharePoint site in place, through the bundled Calcite pgwire server.

The non-Trino lane for ``sharepoint``. ``test_sharepoint_source_e2e.py`` covers the Trino catalog
path; this covers everything else, where the source has no engine-native connector at all:

    DuckDB  --(postgres extension, ATTACH ... TYPE postgres)-->  pgwire-sharepoint bundle (JVM)
            --(calcite-sharepoint-list adapter, Microsoft Graph)-->  the real SharePoint site

``DuckDBSharepointConnector.details()`` (REQ-1690) starts that server once per source via
``pgwire_replica.ensure_endpoint`` and hands back the ``ATTACH`` DDL a live ``EngineRuntime``
issues. Nothing is stubbed: a real JVM, a real Microsoft Graph round trip against
``SP_SITE_URL``, and a real DuckDB attach. Every statement is a read — Calcite serves no DML,
and the attach is ``READ_ONLY``.

Certificate-auth contracts asserted here (all three were defects the live run exposed, see
``pgwire_replica._sharepoint_operand``): the operand must name ``authType=CERTIFICATE``, its
``certificatePath`` must be absolute, and ``certificatePassword`` must be present even when empty.

Credential gating
-----------------
There is no SharePoint emulator; a real Microsoft 365 tenant is required. ``tests/env_creds.py``
exports the ``SP_*`` block of ``.env`` and bridges it to the ``SHAREPOINT_*`` names this suite
reads (resolving the relative ``SP_CERT_PATH`` to an absolute path), so on a machine whose ``.env``
carries the Azure AD app registration the test RUNS. It skips only where those credentials do not
exist. No docker service is involved — the JVM bundle is a cached runtime_dep and the site is
reached over the network.
"""

from __future__ import annotations

import os
from pathlib import Path

import duckdb
import pytest

from provisa.core.models import Source, SourceType
from provisa.federation import pgwire_replica as pr
from provisa.federation.connector_duckdb import DuckDBSharepointConnector

pytestmark = [pytest.mark.integration]

_REQUIRED = (
    "SHAREPOINT_SITE_URL",
    "SHAREPOINT_TENANT_ID",
    "SHAREPOINT_CLIENT_ID",
    "SHAREPOINT_CERT_PATH",
    "SHAREPOINT_CERT_PASSWORD",
)
_MISSING = [name for name in _REQUIRED if not os.environ.get(name)]

pytestmark.append(
    pytest.mark.skipif(
        bool(_MISSING),
        reason=(
            "live SharePoint credentials absent: "
            + ", ".join(_MISSING)
            + " (set the SP_* block in .env; tests/env_creds.py bridges the names)"
        ),
    )
)

# The site's built-in document library. Every SharePoint site has one, so it is the only table this
# test may assume exists without writing to the tenant.
_LIST_NAME = "documents"


@pytest.fixture(scope="module")
def source() -> Source:
    cert_path = Path(os.environ["SHAREPOINT_CERT_PATH"]).resolve()
    assert cert_path.is_file(), f"SHAREPOINT_CERT_PATH does not exist: {cert_path}"
    return Source(
        id="sp-duckdb-live",
        type=SourceType.sharepoint,
        base_url=os.environ["SHAREPOINT_SITE_URL"],
        database=os.environ["SHAREPOINT_TENANT_ID"],
        username=os.environ["SHAREPOINT_CLIENT_ID"],
        mapping={
            "auth_type": "certificate",
            # ABSOLUTE: the pgwire server's cwd is its bundle directory in the runtime_deps cache.
            "certificate_path": str(cert_path),
            "certificate_password": os.environ["SHAREPOINT_CERT_PASSWORD"],
        },
    )


@pytest.fixture(scope="module")
def attached(source: Source):
    details = DuckDBSharepointConnector().details(source)  # boots the JVM, waits for its listener
    con = duckdb.connect()
    con.execute("INSTALL postgres")
    con.execute("LOAD postgres")
    con.execute(details["attach"])
    try:
        yield con, details
    finally:
        con.close()
        pr.stop_all_servers()


def test_operand_carries_the_certificate_auth_contract(source: Source):
    """The three keys the Calcite adapter requires for certificate auth, or it authenticates as
    CLIENT_CREDENTIALS and fails on the missing secret."""
    operand = pr.build_model_json(source)["schemas"][0]["operand"]
    assert operand["authType"] == "CERTIFICATE"
    assert Path(operand["certificatePath"]).is_absolute()
    assert operand["certificatePassword"] == os.environ["SHAREPOINT_CERT_PASSWORD"]
    assert operand["siteUrl"] == os.environ["SHAREPOINT_SITE_URL"]
    assert operand["tenantId"] == os.environ["SHAREPOINT_TENANT_ID"]
    assert operand["clientId"] == os.environ["SHAREPOINT_CLIENT_ID"]
    assert "clientSecret" not in operand


def test_relative_certificate_path_is_a_config_error(source: Source):
    bad = source.model_copy(
        update={"mapping": {**source.mapping, "certificate_path": "./sharepoint.pfx"}}
    )
    with pytest.raises(pr.MissingConnectorConfig, match="certificate_path must be an absolute"):
        pr.build_model_json(bad)


def test_absent_certificate_password_is_a_config_error(source: Source):
    mapping = {k: v for k, v in source.mapping.items() if k != "certificate_password"}
    bad = source.model_copy(update={"mapping": mapping})
    with pytest.raises(
        pr.MissingConnectorConfig, match="requires mapping.certificate_password as a string"
    ):
        pr.build_model_json(bad)


def test_duckdb_lists_the_document_library(attached):
    """The live Graph call: the attached catalog's schema holds one table per SharePoint list."""
    con, details = attached
    rows = con.execute(
        "SELECT table_name FROM information_schema.tables "
        "WHERE table_catalog = ? AND table_schema = ?",
        [details["raw_alias"], details["remote_schema"]],
    ).fetchall()
    tables = {name.lower() for (name,) in rows}
    assert _LIST_NAME in tables, (
        f"expected {_LIST_NAME!r} among the site's lists, got {sorted(tables)}"
    )


def test_duckdb_reads_the_document_library_columns_and_rows(attached):
    """The library may legitimately hold zero documents, so the row count is not asserted — the
    COLUMN SET is what proves the adapter described a real SharePoint list, and the SELECT proves
    the binary COPY scan carried it end to end."""
    con, details = attached
    rel = f'"{details["raw_alias"]}"."{details["remote_schema"]}"."{_LIST_NAME}"'
    columns = {
        name.lower()
        for (name,) in con.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_catalog = ? AND table_schema = ? AND lower(table_name) = ?",
            [details["raw_alias"], details["remote_schema"], _LIST_NAME],
        ).fetchall()
    }
    assert {"id", "title"} <= columns, (
        f"expected id/title among {_LIST_NAME} columns, got {sorted(columns)}"
    )

    count = con.execute(f"SELECT count(*) FROM {rel}").fetchone()
    assert count is not None
    assert count[0] >= 0

    projected = con.execute(f"SELECT id, title FROM {rel} LIMIT 5").fetchall()
    assert len(projected) <= 5
    assert all(len(row) == 2 for row in projected)


def test_the_live_attach_is_read_only(attached):
    """Read-only against the tenant is enforced at the attach, not by convention."""
    con, details = attached
    rel = f'"{details["raw_alias"]}"."{details["remote_schema"]}"."{_LIST_NAME}"'
    with pytest.raises(duckdb.Error):
        con.execute(f"DELETE FROM {rel}")
