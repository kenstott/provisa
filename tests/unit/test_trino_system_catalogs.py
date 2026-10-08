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

import subprocess
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
    host, port = "trino", 8080  # the coordinator's address: what its catalogs are recorded under

    def __init__(self, drop_error: Exception | None = None):
        self.executed: list[str] = []
        self._drop_error = drop_error

    def cursor(self):
        return _Cursor(self.executed, self._drop_error)


@pytest.fixture(autouse=True)
def _control_plane_address_from_the_url_only(monkeypatch):
    # tests/conftest.py exports PROVISA_ENGINE_CONTROL_PLANE_HOST/_PORT and
    # PROVISA_ENGINE_OTEL_S3_ENDPOINT so the containerized Trino in the integration lanes dials
    # `postgres:5432` / `minio:9000` instead of the host-side ports. These unit tests assert the
    # derivation FROM the passed URL/env, so the ambient overrides have to come off or every spec
    # here reports the compose address rather than the one under test.
    monkeypatch.delenv("PROVISA_ENGINE_CONTROL_PLANE_HOST", raising=False)
    monkeypatch.delenv("PROVISA_ENGINE_CONTROL_PLANE_PORT", raising=False)
    monkeypatch.delenv("PROVISA_ENGINE_OTEL_S3_ENDPOINT", raising=False)


def test_no_system_catalog_is_shipped_as_a_mounted_properties_file():
    # docker-compose.core.yml mounts ./trino/catalog at /etc/trino/catalog; a COMMITTED file here
    # shadows the runtime registration with the authoring machine's connection values. Trino's own
    # FileCatalogStore also writes into this directory whenever Provisa issues CREATE CATALOG, so
    # the guarantee is about what is TRACKED, not what is on disk — see
    # tests/unit/test_trino_catalog_dir_not_committed.py. Staging copies live in
    # trino/catalog-install/, which is not mounted.
    tracked = subprocess.run(
        ["git", "ls-files", "--", "trino/catalog/*.properties"],
        cwd=_REPO,
        capture_output=True,
        text=True,
        check=True,
    ).stdout.split()
    for name in tsc.SYSTEM_CATALOGS:
        assert f"trino/catalog/{name}.properties" not in tracked, name


def test_control_plane_spec_uses_the_live_control_plane_not_the_dev_postgres():
    spec = tsc.control_plane_spec(_URL)
    assert spec.connector == "postgresql"
    assert spec.properties["connection-url"] == "jdbc:postgresql://10.1.2.3:6543/provisa_cloud"
    assert spec.properties["connection-user"] == "cloud_user"
    assert spec.properties["connection-password"] == "cloud_pw"


def test_control_plane_spec_names_no_default_schema():
    """One ``provisa_admin`` serves every org and environment on a coordinator. With
    ``currentSchema=org_<id>`` in its URL the catalog was the last builder's: each build dropped
    and re-created it, and an unqualified name through it resolved in that builder's schema."""
    assert "currentSchema" not in tsc.control_plane_spec(_URL).properties["connection-url"]


def test_a_non_postgres_control_plane_is_rejected_rather_than_defaulted():
    with pytest.raises(ValueError, match="Postgres control plane"):
        tsc.control_plane_spec(make_url("sqlite:///provisa.db"))


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
    assert (
        otel.properties["iceberg.jdbc-catalog.default-warehouse-dir"] == "s3://cloud-otel/warehouse"
    )
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
    assert tsc.spec_for("otel", _URL).name == "otel"
    with pytest.raises(ValueError, match="not a Provisa system catalog"):
        tsc.spec_for("sales_pg", _URL)


def test_register_catalog_drops_before_creating():
    conn = _Conn()
    tsc.register_catalog(conn, tsc.control_plane_spec(_URL))
    assert conn.executed[0] == "DROP CATALOG IF EXISTS provisa_admin"
    create = conn.executed[1]
    assert create.startswith("CREATE CATALOG provisa_admin USING postgresql WITH (")
    assert "\"connection-url\" = 'jdbc:postgresql://10.1.2.3:6543/provisa_cloud'" in create


class _Record:
    """The spec hashes a coordinator's catalogs were created from, held in memory."""

    def __init__(self) -> None:
        self.hashes: dict[tuple[str, str], str] = {}

    def created_from(self, coordinator: str, name: str) -> str | None:
        return self.hashes.get((coordinator, name))

    def record(self, coordinator: str, name: str, spec_hash: str) -> None:
        self.hashes[(coordinator, name)] = spec_hash


@pytest.fixture
def _record():
    """The deployment's record of what each catalog was created from: one for the whole test, as
    the platform state store's is one for every process of the deployment
    (tests/unit/test_platform_state_catalogs.py covers the store itself)."""
    return _Record()


@pytest.fixture(autouse=True)
def _registrar(monkeypatch):
    """The deployment's catalog-registration lock, taken on the control plane: recorded, not taken
    (these tests reach no Postgres). Each test sees the lock's span in ``held``."""
    import contextlib

    held: list[str] = []

    @contextlib.contextmanager
    def _one(url, timeout=None):
        held.append("lock")
        yield
        held.append("unlock")

    monkeypatch.setattr(tsc, "one_registrar", _one)
    return held


def test_two_processes_register_the_catalogs_one_at_a_time(monkeypatch, _registrar, _record):
    """``register_catalog`` drops then creates; two processes of one deployment doing it at once
    interleave and one fails to boot (ALREADY_EXISTS). Every registration is inside the lock."""
    from provisa.core import catalog as catalog_module

    monkeypatch.setattr(catalog_module, "wait_until_ready", lambda conn, timeout=None: None)
    monkeypatch.setattr(tsc, "ensure_iceberg_catalog_tables", lambda url, timeout=None: None)
    monkeypatch.setattr(tsc, "register_catalog", lambda _c, spec: _registrar.append(spec.name))
    tsc.register_system_catalogs(_Conn(), _URL, _record)
    assert _registrar == ["lock", "provisa_admin", "otel", "results", "unlock"]
    _registrar.clear()
    tsc.ensure_system_catalogs(_LiveConn({"results"}), _URL, _record)
    assert _registrar == ["lock", "provisa_admin", "otel", "unlock"]


def test_registration_ensures_the_iceberg_metastore_before_creating_any_catalog(
    monkeypatch, _record
):
    # Trino's JDBC catalog factory never creates iceberg_tables; db/init.sql does, but only for the
    # BUNDLED Postgres via docker-entrypoint-initdb.d. On a managed control plane the tables were
    # absent, so CREATE CATALOG otel died with "Cannot check and eventually update SQL schema" and
    # took app startup down with it. The DDL must run against the URL Trino is handed, before the
    # first CREATE CATALOG.
    order: list[str] = []
    from provisa.core import catalog as catalog_module

    monkeypatch.setattr(catalog_module, "wait_until_ready", lambda conn, timeout=None: None)
    monkeypatch.setattr(
        tsc,
        "ensure_iceberg_catalog_tables",
        lambda url, timeout=None: order.append(f"ensure:{url.database}"),
    )
    monkeypatch.setattr(tsc, "register_catalog", lambda _c, spec: order.append(spec.name))

    tsc.register_system_catalogs(_Conn(), _URL, _record)
    assert order == ["ensure:provisa_cloud", "provisa_admin", "otel", "results"]


def test_the_iceberg_metastore_ddl_matches_db_init_sql():
    # One definition of these tables, or the bundled and managed control planes drift apart.
    init_sql = (_REPO / "db" / "init.sql").read_text()
    for table in ("iceberg_tables", "iceberg_namespace_properties"):
        assert f"CREATE TABLE IF NOT EXISTS {table}" in init_sql
        ddl = next(d for d in tsc._ICEBERG_CATALOG_DDL if f"EXISTS {table} " in d)
        for column in ("catalog_name", "PRIMARY KEY"):
            assert column in ddl


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


def test_a_coordinator_on_the_static_catalog_store_is_named_as_such():
    """A whole static store refuses every DROP CATALOG; the fix is catalog.management=dynamic, not
    a file to delete (the chart's Trino ran the static store and the message sent the operator
    looking for a provisa_admin.properties that did not exist)."""
    conn = _Conn(
        drop_error=trino.exceptions.TrinoQueryError(
            {
                "errorName": "NOT_SUPPORTED",
                "message": "DROP CATALOG is not supported by the static catalog store",
            }
        )
    )
    with pytest.raises(RuntimeError) as raised:
        tsc.register_catalog(conn, tsc.otel_spec(_URL))
    assert "catalog.management=dynamic" in str(raised.value)
    assert ".properties file" not in str(raised.value)
    assert len(conn.executed) == 1


class _ShowCatalogsCursor(_Cursor):
    def __init__(self, log: list[str], live: set[str]):
        super().__init__(log, None)
        self._live = live
        self._last = ""

    def execute(self, sql: str):
        self._last = sql
        super().execute(sql)

    def fetchall(self):
        return [[name] for name in sorted(self._live)] if self._last == "SHOW CATALOGS" else []


class _LiveConn(_Conn):
    def __init__(self, live: set[str]):
        super().__init__()
        self._live = live

    def cursor(self):
        return _ShowCatalogsCursor(self.executed, self._live)


@pytest.fixture
def _registered(monkeypatch):
    """Registration with the engine and the metastore stubbed: the names (re)created, in order."""
    from provisa.core import catalog as catalog_module

    monkeypatch.setattr(catalog_module, "wait_until_ready", lambda conn, timeout=None: None)
    monkeypatch.setattr(tsc, "ensure_iceberg_catalog_tables", lambda url, timeout=None: None)
    registered: list[str] = []
    monkeypatch.setattr(tsc, "register_catalog", lambda _c, spec: registered.append(spec.name))
    return registered


_ALL = ["provisa_admin", "otel", "results"]


def test_a_second_orgs_build_on_the_coordinator_issues_no_drop(_registered, _record):
    """REQ-1429, and the same for ``provisa_admin``: one coordinator serves every org, environment
    and worker, and each comes through registration when it boots or builds a runtime. Each used
    to drop and re-create ``provisa_admin`` for itself; a statement naming the catalog between
    the drop and the create failed CATALOG_NOT_FOUND (a view's refresh in the suite did), and on
    the SaaS node the same race left the coordinator with no ``otel`` at all. A catalog that is
    live and was created from the spec it should have is left alone."""
    tsc.register_system_catalogs(
        _Conn(), _URL, _record
    )  # the first server boots on an empty coordinator
    assert _registered == _ALL
    _registered.clear()

    live = _LiveConn(set(_ALL))
    tsc.ensure_system_catalogs(live, _URL, _record)  # another org's runtime build
    tsc.register_system_catalogs(live, _URL, _record)  # another worker's boot
    assert _registered == []
    assert not any(sql.startswith(("DROP CATALOG", "CREATE CATALOG")) for sql in live.executed)


def test_a_catalog_whose_spec_changed_is_created_again(_registered, _record):
    tsc.register_system_catalogs(_Conn(), _URL, _record)
    _registered.clear()
    moved = _URL.set(host="10.9.9.9")  # the control plane moved: every catalog's spec names it
    tsc.ensure_system_catalogs(_LiveConn(set(_ALL)), moved, _record)
    assert _registered == _ALL


def test_a_catalog_the_coordinator_lost_is_created_again(_registered, _record):
    """A restarted coordinator holds no dynamic catalog, whatever the record says."""
    tsc.register_system_catalogs(_Conn(), _URL, _record)
    _registered.clear()
    tsc.ensure_system_catalogs(_LiveConn({"results"}), _URL, _record)
    assert _registered == ["provisa_admin", "otel"]


def test_a_live_catalog_nothing_recorded_is_brought_under_the_record(_registered, _record):
    """Live, but not known to have been created from this spec: created from it, once."""
    live = _LiveConn(set(_ALL))
    tsc.ensure_system_catalogs(live, _URL, _record)
    assert _registered == _ALL
    _registered.clear()
    tsc.ensure_system_catalogs(live, _URL, _record)
    assert _registered == []
    assert {name for (_coordinator, name) in _record.hashes} == set(_ALL)


def test_each_coordinator_has_its_own_record(_registered, _record):
    """An org with an engine of its own is on another coordinator, whose catalogs are its own."""

    class _Other(_LiveConn):
        host = "trino-acme"

    tsc.register_system_catalogs(_Conn(), _URL, _record)
    _registered.clear()
    tsc.ensure_system_catalogs(_Other(set()), _URL, _record)
    assert _registered == _ALL


@pytest.mark.parametrize(
    ("sql", "unqualified"),
    [
        ('SELECT MAX("updated_at") FROM provisa_admin.public.registered_tables', []),
        ('SELECT * FROM "provisa_admin"."org_acme_mv_cache"."mv_orders"', []),
        ("SELECT * FROM provisa_admin.orders", ["provisa_admin.orders"]),
        (
            'SELECT o.id FROM sales.public.orders o JOIN "provisa_admin"."t" x ON x.id = o.id',
            ["provisa_admin.t"],
        ),
    ],
)
def test_a_statement_naming_provisa_admin_without_a_schema_is_found(sql, unqualified):
    assert tsc.unqualified_admin_references(sql) == unqualified
