# Copyright (c) 2026 Kenneth Stott
# Canary: 3d5b7c14-9a2e-4f83-b1c6-0e7d2a48f915
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Runtime-derived definitions for the three Provisa-owned Trino catalogs (REQ-1332).

``provisa_admin`` (the control-plane self-catalog), ``otel`` and ``results`` are not config
sources — nothing registers them through ``_upsert_sources``. They existed only as checked-in
``trino/catalog/*.properties`` files, and a ``catalog.management=dynamic`` Trino still auto-loads
that directory. A file-loaded catalog cannot be dropped, so ``CREATE CATALOG IF NOT EXISTS``
silently no-ops against one: every deployment inherited the repo's dev values (the bundled
``postgres:5432/provisa``, ``minioadmin``) regardless of how it was actually configured — on the
SaaS node ``provisa_admin`` pointed at the bundled Postgres instead of Cloud SQL.

These specs are the single source of truth instead: built from the live control-plane engine URL
and the object-store environment, registered dynamically at boot, and re-registered by the
reload-catalog admin endpoint. The ``.properties`` files stay under ``trino/catalog-install/``,
which is staging only and never mounted at ``/etc/trino/catalog``.
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass

import trino
from sqlalchemy.engine import URL

# The results catalog name is owned by the CTAS-redirect writer; import rather than restate it.
from provisa.executor.trino_write import RESULTS_CATALOG

log = logging.getLogger(__name__)

PROVISA_ADMIN_CATALOG = "provisa_admin"
OTEL_CATALOG = "otel"

SYSTEM_CATALOGS = (PROVISA_ADMIN_CATALOG, OTEL_CATALOG, RESULTS_CATALOG)


@dataclass(frozen=True)
class CatalogSpec:
    """One dynamic Trino catalog: its name, connector, and properties (minus ``connector.name``)."""

    name: str
    connector: str
    properties: dict[str, str]


def _pg_parts(url: URL) -> tuple[str, int, str, str, str]:
    """(host, port, database, user, password) from the control-plane URL.

    Trino reaches both the self-catalog and the Iceberg JDBC metastore over JDBC, so the control
    plane must be Postgres — a Trino deployment on a file-backed control plane has no
    ``provisa_admin`` to expose and no metastore to hold the ``otel``/``results`` tables.
    """
    if not url.get_backend_name().startswith("postgresql"):
        raise ValueError(
            f"Trino requires a Postgres control plane; control_plane.tenant_url is "
            f"{url.get_backend_name()!r}"
        )
    if not url.host or not url.database:
        raise ValueError("control_plane.tenant_url must specify a host and a database for Trino")
    # Port omitted in the URL means the Postgres default — the same reading ControlPlane.tenant_parts
    # applies (models.py), not a stand-in for a missing value.
    return url.host, url.port or 5432, url.database, url.username or "", url.password or ""


def engine_visible_address(host: str, port: int) -> tuple[str, int]:
    """The control-plane address as *Trino* reaches it, which is not always the app's.

    Every property below is a JDBC URL the coordinator dials, so the address has to be resolvable
    from Trino's network, not from the process that built the spec. In a deployment those coincide
    — app and coordinator are containers on one compose network and both say ``postgres:5432`` — so
    these variables are unset and this returns the URL's own address unchanged. They exist for the
    split case, where the app runs outside the stack and reaches Postgres on a host-published
    ephemeral port that means nothing inside the coordinator (the integration harness, which
    publishes every service on a reserved port). This is the same distinction a source row already
    makes: ``sources.host`` is the engine-visible host, which is why the fixture configs say
    ``postgres`` while the app's own URL says ``localhost``.
    """
    return (
        os.environ.get("PROVISA_ENGINE_CONTROL_PLANE_HOST", host),
        int(os.environ.get("PROVISA_ENGINE_CONTROL_PLANE_PORT", port)),
    )


def control_plane_spec(url: URL, org_id: str) -> CatalogSpec:
    """The ``provisa_admin`` catalog: the tenant control plane, scoped to this org's schema."""
    host, port, database, user, password = _pg_parts(url)
    host, port = engine_visible_address(host, port)
    return CatalogSpec(
        name=PROVISA_ADMIN_CATALOG,
        connector="postgresql",
        properties={
            "connection-url": f"jdbc:postgresql://{host}:{port}/{database}?currentSchema=org_{org_id}",
            "connection-user": user,
            "connection-password": password,
            "statistics.enabled": "false",
        },
    )


def otel_object_store() -> dict[str, str]:
    """The MinIO/S3 coordinates of the OTel parquet store (otlp2parquet writes, Iceberg reads)."""
    return {
        "endpoint": os.environ.get("PROVISA_OTEL_S3_ENDPOINT", "http://minio:9000"),
        "bucket": os.environ.get("PROVISA_OTEL_BUCKET", "provisa-otel"),
        "access_key": os.environ.get("PROVISA_OTEL_S3_ACCESS_KEY", "minioadmin"),
        "secret_key": os.environ.get("PROVISA_OTEL_S3_SECRET_KEY", "minioadmin"),
        "region": os.environ.get("PROVISA_OTEL_S3_REGION", "us-east-1"),
    }


def _iceberg_spec(
    name: str, url: URL, bucket: str, store: dict[str, str], extra: dict[str, str]
) -> CatalogSpec:
    host, port, database, user, password = _pg_parts(url)
    host, port = engine_visible_address(host, port)
    props = {
        "iceberg.catalog.type": "jdbc",
        "iceberg.jdbc-catalog.driver-class": "org.postgresql.Driver",
        "iceberg.jdbc-catalog.connection-url": f"jdbc:postgresql://{host}:{port}/{database}",
        "iceberg.jdbc-catalog.connection-user": user,
        "iceberg.jdbc-catalog.connection-password": password,
        "iceberg.jdbc-catalog.catalog-name": name,
        "iceberg.jdbc-catalog.default-warehouse-dir": f"s3://{bucket}/warehouse",
        "fs.native-s3.enabled": "true",
        "s3.endpoint": store["endpoint"],
        "s3.aws-access-key": store["access_key"],
        "s3.aws-secret-key": store["secret_key"],
        "s3.path-style-access": "true",
        "s3.region": store["region"],
        "iceberg.register-table-procedure.enabled": "true",
        "iceberg.max-previous-versions": "1",
    }
    props.update(extra)
    return CatalogSpec(name=name, connector="iceberg", properties=props)


def otel_spec(url: URL) -> CatalogSpec:
    """The ``otel`` Iceberg catalog over the telemetry bucket."""
    store = otel_object_store()
    return _iceberg_spec(
        OTEL_CATALOG,
        url,
        store["bucket"],
        store,
        {
            "iceberg.metadata-cache.enabled": "true",
            # Snapshot expiry never runs against telemetry younger than the ops retention window.
            "iceberg.expire-snapshots.min-retention": "7d",
        },
    )


def results_spec(url: URL) -> CatalogSpec:
    """The ``results`` Iceberg catalog the CTAS redirect writes into (REQ-045)."""
    from provisa.executor.redirect import RedirectConfig

    # PROVISA_REDIRECT_BUCKET names the bucket ensure_results_bucket creates; the endpoint and
    # credentials come from the object-store env, because one bundled MinIO backs both the results
    # and the telemetry bucket (docker-compose.core.yml provisions them in the same mc block).
    return _iceberg_spec(
        RESULTS_CATALOG, url, RedirectConfig.from_env().bucket, otel_object_store(), {}
    )


def system_catalog_specs(url: URL, org_id: str) -> list[CatalogSpec]:
    """Every Provisa-owned catalog, derived from the live control plane + object store."""
    return [control_plane_spec(url, org_id), otel_spec(url), results_spec(url)]


def spec_for(name: str, url: URL, org_id: str) -> CatalogSpec:
    """The spec for one system catalog by name; raises for anything else."""
    for spec in system_catalog_specs(url, org_id):
        if spec.name == name:
            return spec
    raise ValueError(f"{name!r} is not a Provisa system catalog ({', '.join(SYSTEM_CATALOGS)})")


def register_catalog(conn: trino.dbapi.Connection, spec: CatalogSpec) -> None:
    """Drop and recreate one system catalog so its properties match ``spec`` exactly.

    Trino exposes no way to read a live catalog's properties back, and ``CREATE CATALOG IF NOT
    EXISTS`` against an existing catalog is a no-op, so refreshing means dropping first. A catalog
    that refuses to drop is file-loaded — the shadowing this module exists to end — and that is
    raised, never worked around: a silently stale catalog is what pointed the SaaS control plane at
    the wrong database.
    """
    from provisa.core.catalog import _escape_sql_string, _validate_identifier

    name = _validate_identifier(spec.name)
    connector = _validate_identifier(spec.connector)
    cur = conn.cursor()
    try:
        cur.execute(f"DROP CATALOG IF EXISTS {name}")
        cur.fetchall()
    except trino.exceptions.TrinoQueryError as exc:
        raise RuntimeError(
            f"Trino catalog {name!r} cannot be dropped ({exc}) — it is loaded from a static "
            f"/etc/trino/catalog/{name}.properties file, which shadows the runtime definition. "
            "Remove that file from the mounted catalog directory."
        ) from exc

    props_sql = ", ".join(
        f"\"{k}\" = '{_escape_sql_string(v)}'" for k, v in spec.properties.items()
    )
    cur.execute(f"CREATE CATALOG {name} USING {connector} WITH ({props_sql})")
    cur.fetchall()
    log.info("Registered Trino system catalog %s (%s)", name, connector)


# The two tables Iceberg's JdbcCatalog reads on initialize. Trino's TrinoJdbcCatalogFactory does
# not create them, so a CREATE CATALOG against a database that lacks them fails the whole statement
# with "Cannot check and eventually update SQL schema" / relation "iceberg_tables" does not exist.
# Column types match db/init.sql, which is the same DDL — that file reaches the BUNDLED Postgres
# through docker-entrypoint-initdb.d and reaches nothing else, so a managed control plane (the SaaS
# node's Cloud SQL) never got it and every boot died registering the otel/results catalogs.
_ICEBERG_CATALOG_DDL = (
    """
    CREATE TABLE IF NOT EXISTS iceberg_tables (
        catalog_name VARCHAR(255) NOT NULL,
        table_namespace VARCHAR(255) NOT NULL,
        table_name VARCHAR(255) NOT NULL,
        metadata_location VARCHAR(1000),
        previous_metadata_location VARCHAR(1000),
        iceberg_type VARCHAR(5),
        PRIMARY KEY (catalog_name, table_namespace, table_name)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS iceberg_namespace_properties (
        catalog_name VARCHAR(255) NOT NULL,
        namespace VARCHAR(255) NOT NULL,
        property_key VARCHAR(255),
        property_value VARCHAR(1000),
        PRIMARY KEY (catalog_name, namespace, property_key)
    )
    """,
)


def ensure_iceberg_catalog_tables(url: URL) -> None:
    """Create the Iceberg JDBC metastore tables in the control-plane database if they are absent.

    Runs against the same database ``_iceberg_spec`` hands Trino, so the catalog Trino is about to
    open always finds its metastore. Idempotent (``IF NOT EXISTS``) and unconditional — the tables
    belong to the Iceberg catalog, not to any org schema, and live in the connection's default
    schema exactly as the JDBC URL in ``_iceberg_spec`` resolves them.
    """
    import psycopg2

    host, port, database, user, password = _pg_parts(url)
    with psycopg2.connect(
        host=host, port=port, dbname=database, user=user, password=password
    ) as pg:
        with pg.cursor() as cur:
            for ddl in _ICEBERG_CATALOG_DDL:
                cur.execute(ddl)
    pg.close()


def register_system_catalogs(conn: trino.dbapi.Connection, url: URL, org_id: str) -> None:
    """Register every Provisa-owned catalog from runtime values. Boot-time; blocking."""
    from provisa.core.catalog import wait_until_ready

    wait_until_ready(conn)  # a coordinator that just restarted races app boot
    ensure_iceberg_catalog_tables(url)
    for spec in system_catalog_specs(url, org_id):
        register_catalog(conn, spec)
