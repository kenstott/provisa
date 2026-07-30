# Copyright (c) 2026 Kenneth Stott
# Canary: 5a91c73e-4b18-4d0a-9f22-6c3e8a71b5d4
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""The Provisa-owned Trino catalogs come from runtime values, not checked-in files (REQ-1332).

trino/catalog/*.properties is auto-loaded by a catalog.management=dynamic Trino, and a file-loaded
catalog cannot be dropped — so a static provisa_admin.properties made CREATE CATALOG a silent
no-op and pinned every deployment to the repo's dev connection values. The SaaS node's
provisa_admin therefore pointed at the bundled Postgres instead of Cloud SQL.
"""

from __future__ import annotations

from pathlib import Path

import pytest
import trino
from sqlalchemy import make_url

from provisa.core import trino_system_catalogs as tsc

_REPO = Path(__file__).resolve().parents[2]
_URL = make_url("postgresql://cloud_user:cloud_pw@10.1.2.3:6543/provisa_cloud")


class _Cursor:
    def __init__(self, log: list[str], drop_error: Exception | None):
        self._log = log
        self._drop_error = drop_error

    def execute(self, sql: str):
        self._log.append(sql)
        if self._drop_error is not None and sql.startswith("DROP CATALOG"):
            raise self._drop_error

    def fetchall(self):
        return []


class _Conn:
    def __init__(self, drop_error: Exception | None = None):
        self.executed: list[str] = []
        self._drop_error = drop_error

    def cursor(self):
        return _Cursor(self.executed, self._drop_error)


def test_no_system_catalog_is_shipped_as_a_mounted_properties_file():
    # docker-compose.core.yml mounts ./trino/catalog at /etc/trino/catalog; a file here shadows
    # the runtime registration. Staging copies live in trino/catalog-install/, which is not mounted.
    for name in tsc.SYSTEM_CATALOGS:
        assert not (_REPO / "trino" / "catalog" / f"{name}.properties").exists(), name


def test_control_plane_spec_uses_the_live_control_plane_not_the_dev_postgres():
    spec = tsc.control_plane_spec(_URL, "default")
    assert spec.connector == "postgresql"
    assert (
        spec.properties["connection-url"]
        == "jdbc:postgresql://10.1.2.3:6543/provisa_cloud?currentSchema=org_default"
    )
    assert spec.properties["connection-user"] == "cloud_user"
    assert spec.properties["connection-password"] == "cloud_pw"


def test_control_plane_spec_scopes_the_search_path_to_the_org():
    spec = tsc.control_plane_spec(_URL, "acme")
    assert spec.properties["connection-url"].endswith("?currentSchema=org_acme")


def test_a_non_postgres_control_plane_is_rejected_rather_than_defaulted():
    with pytest.raises(ValueError, match="Postgres control plane"):
        tsc.control_plane_spec(make_url("sqlite:///provisa.db"), "default")


def test_iceberg_specs_track_the_control_plane_and_object_store(monkeypatch):
    monkeypatch.setenv("PROVISA_OTEL_S3_ENDPOINT", "http://10.1.2.3:9000")
    monkeypatch.setenv("PROVISA_OTEL_BUCKET", "cloud-otel")
    monkeypatch.setenv("PROVISA_OTEL_S3_ACCESS_KEY", "ak")
    monkeypatch.setenv("PROVISA_OTEL_S3_SECRET_KEY", "sk")

    otel = tsc.otel_spec(_URL)
    assert otel.connector == "iceberg"
    assert (
        otel.properties["iceberg.jdbc-catalog.connection-url"]
        == "jdbc:postgresql://10.1.2.3:6543/provisa_cloud"
    )
    assert otel.properties["iceberg.jdbc-catalog.catalog-name"] == "otel"
    assert otel.properties["iceberg.jdbc-catalog.default-warehouse-dir"] == "s3://cloud-otel/warehouse"
    assert otel.properties["s3.endpoint"] == "http://10.1.2.3:9000"
    assert otel.properties["s3.aws-access-key"] == "ak"
    assert otel.properties["s3.aws-secret-key"] == "sk"

    results = tsc.results_spec(_URL)
    assert results.properties["iceberg.jdbc-catalog.catalog-name"] == "results"
    assert results.properties["iceberg.jdbc-catalog.default-warehouse-dir"].startswith(
        "s3://provisa-results/"
    )
    assert results.properties["s3.endpoint"] == "http://10.1.2.3:9000"


def test_spec_for_rejects_a_catalog_provisa_does_not_own():
    assert tsc.spec_for("otel", _URL, "default").name == "otel"
    with pytest.raises(ValueError, match="not a Provisa system catalog"):
        tsc.spec_for("sales_pg", _URL, "default")


def test_register_catalog_drops_before_creating():
    conn = _Conn()
    tsc.register_catalog(conn, tsc.control_plane_spec(_URL, "default"))
    assert conn.executed[0] == "DROP CATALOG IF EXISTS provisa_admin"
    create = conn.executed[1]
    assert create.startswith("CREATE CATALOG provisa_admin USING postgresql WITH (")
    assert "\"connection-url\" = 'jdbc:postgresql://10.1.2.3:6543/provisa_cloud?currentSchema=org_default'" in create


def test_a_catalog_that_cannot_be_dropped_is_reported_not_worked_around():
    conn = _Conn(
        drop_error=trino.exceptions.TrinoQueryError(
            {"errorName": "NOT_SUPPORTED", "message": "Catalog is not dynamic"}
        )
    )
    with pytest.raises(RuntimeError, match="shadows the runtime definition"):
        tsc.register_catalog(conn, tsc.otel_spec(_URL))
    # It must not fall through to CREATE CATALOG IF NOT EXISTS against the shadowing catalog.
    assert len(conn.executed) == 1
