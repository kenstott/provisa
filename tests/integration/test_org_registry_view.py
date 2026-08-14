# Copyright (c) 2026 Kenneth Stott
# Canary: 63c7fbcf-172d-4279-868c-6ce85edb2cac
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1301: the deployment's own tenancy, queryable as a dataset in the root org.

"Who runs org acme, and when did it appear" was answerable only by an admin endpoint or raw SQL
against the control plane. These tests build the view the requirement asks for and then read it the
way any other dataset is read, including the meta-domain registration that makes it visible to the
query surfaces at all.

The two facts sit in different planes — the org row is admin-plane, the org_admin holder is one
``user_role_assignments`` table per org schema — so the interesting cases are the joins across them:
an org with an admin, an org whose schema does not exist yet, and the confinement of the view to the
root org.

DDL and seeding run on a synchronous psycopg2 engine so the async engines are only ever driven
inside the test's own event loop.
"""

from __future__ import annotations

import os

import pytest
from sqlalchemy import create_engine, text

from provisa.core.database import Database, create_engine_from_url
from provisa.core.org_registry_view import (
    VIEW_COLUMNS,
    VIEW_NAME,
    RegistryViewUnavailable,
    build_view_sql,
    refresh_org_registry_view,
)
from provisa.core.schema_admin import REGISTRY_TABLES
from provisa.core.schema_admin import metadata as admin_metadata
from provisa.core.schema_org import (
    domains,
    registered_tables,
    roles,
    sources,
    table_columns,
    user_role_assignments,
)
from provisa.core.schema_org import metadata as org_metadata

pytestmark = [pytest.mark.integration]

_PG_HOST = os.environ.get("PG_HOST", "localhost")
_PG_PORT = os.environ.get("PG_PORT", "5432")
_SYNC_URL = f"postgresql+psycopg2://provisa:provisa@{_PG_HOST}:{_PG_PORT}/provisa"
_ASYNC_URL = f"postgresql+asyncpg://provisa:provisa@{_PG_HOST}:{_PG_PORT}/provisa"

_ADMIN_SCHEMA = "test_req1301_admin"
_ROOT = "r1301root"
_ACME = "r1301acme"
_ROOT_SCHEMA = f"org_{_ROOT}"
_ACME_SCHEMA = f"org_{_ACME}"


def _prepare_sync():
    """Two provisioned orgs, each with its own org_admin, plus the root org's catalog tables."""
    engine = create_engine(_SYNC_URL, pool_pre_ping=True)
    with engine.begin() as conn:
        for schema in (_ADMIN_SCHEMA, _ROOT_SCHEMA, _ACME_SCHEMA):
            conn.execute(text(f"DROP SCHEMA IF EXISTS {schema} CASCADE"))
            conn.execute(text(f"CREATE SCHEMA {schema}"))

        conn.execute(text(f"SET search_path TO {_ADMIN_SCHEMA}"))
        admin_metadata.create_all(conn, tables=REGISTRY_TABLES)
        conn.execute(
            text("INSERT INTO orgs (id, name) VALUES (:i, :n)"),
            [{"i": _ROOT, "n": "Enterprise"}, {"i": _ACME, "n": "Acme"}],
        )
        conn.execute(
            text("INSERT INTO user_profiles (user_id, email, display_name) VALUES (:u, :e, :d)"),
            [
                {"u": "uid-root", "e": "root@example.test", "d": "Root Admin"},
                {"u": "uid-acme", "e": "acme@example.test", "d": "Acme Admin"},
            ],
        )

        # The root org additionally carries the dataset catalog, because the view is registered
        # there as a meta-domain table.
        conn.execute(text(f"SET search_path TO {_ROOT_SCHEMA}"))
        org_metadata.create_all(
            conn,
            tables=[roles, user_role_assignments, sources, domains, registered_tables,
                    table_columns],
        )
        conn.execute(text("INSERT INTO roles (id) VALUES ('org_admin')"))
        conn.execute(
            text(
                "INSERT INTO user_role_assignments (user_id, role_id, domain_id)"
                " VALUES ('uid-root', 'org_admin', '*')"
            )
        )
        conn.execute(text("INSERT INTO sources (id, type) VALUES ('provisa-admin', 'postgres')"))
        conn.execute(text("INSERT INTO domains (id) VALUES ('meta')"))

        conn.execute(text(f"SET search_path TO {_ACME_SCHEMA}"))
        org_metadata.create_all(conn, tables=[roles, user_role_assignments])
        conn.execute(text("INSERT INTO roles (id) VALUES ('org_admin')"))
        conn.execute(
            text(
                "INSERT INTO user_role_assignments (user_id, role_id, domain_id)"
                " VALUES ('uid-acme', 'org_admin', '*')"
            )
        )
    return engine


@pytest.fixture
def planes():
    try:
        sync_engine = _prepare_sync()
    except Exception as exc:  # noqa: BLE001 — the suite provisions this PG; a miss is a config fault
        pytest.skip(f"live Postgres not reachable at {_SYNC_URL}: {exc}")

    admin_db = Database(create_engine_from_url(_ASYNC_URL), name="admin", search_path=_ADMIN_SCHEMA)
    tenant_db = Database(
        create_engine_from_url(_ASYNC_URL), name="org", search_path=_ROOT_SCHEMA
    )
    yield admin_db, tenant_db, sync_engine

    with sync_engine.begin() as conn:
        for schema in (_ADMIN_SCHEMA, _ROOT_SCHEMA, _ACME_SCHEMA):
            conn.execute(text(f"DROP SCHEMA IF EXISTS {schema} CASCADE"))
    sync_engine.dispose()


def _read_view(sync_engine) -> list[tuple]:
    with sync_engine.begin() as conn:
        return conn.execute(
            text(
                f"SELECT org_id, org_name, provisioning_state, org_admin_user_id, org_admin_email,"
                f" org_admin_display_name FROM {_ROOT_SCHEMA}.{VIEW_NAME} ORDER BY org_id"
            )
        ).all()


async def test_view_names_each_org_and_its_admin(planes):
    """One row per org, carrying the org_admin's identity from the other plane."""
    admin_db, tenant_db, sync_engine = planes
    await refresh_org_registry_view(tenant_db=tenant_db, admin_db=admin_db)

    assert _read_view(sync_engine) == [
        (_ACME, "Acme", "ready", "uid-acme", "acme@example.test", "Acme Admin"),
        (_ROOT, "Enterprise", "ready", "uid-root", "root@example.test", "Root Admin"),
    ]


async def test_view_reads_live_rows_without_a_rebuild(planes):
    """It is a view, not a snapshot: a role granted after the build shows up on the next read."""
    admin_db, tenant_db, sync_engine = planes
    await refresh_org_registry_view(tenant_db=tenant_db, admin_db=admin_db)

    with sync_engine.begin() as conn:
        conn.execute(
            text(
                f"INSERT INTO {_ADMIN_SCHEMA}.user_profiles (user_id, email, display_name)"
                " VALUES (:u, :e, :d)"
            ),
            {"u": "uid-acme2", "e": "second@example.test", "d": "Second Admin"},
        )
        conn.execute(
            text(
                f"INSERT INTO {_ACME_SCHEMA}.user_role_assignments (user_id, role_id, domain_id)"
                " VALUES ('uid-acme2', 'org_admin', '*')"
            )
        )

    rows = _read_view(sync_engine)
    assert [r[0] for r in rows] == [_ACME, _ACME, _ROOT]
    assert {r[3] for r in rows if r[0] == _ACME} == {"uid-acme", "uid-acme2"}


async def test_org_without_a_provisioned_schema_keeps_its_row(planes):
    """A still-provisioning org has no user_role_assignments table. It is listed with no admin —
    absent from the registry would read as "this org does not exist", which is the wrong answer."""
    admin_db, tenant_db, sync_engine = planes
    with sync_engine.begin() as conn:
        conn.execute(
            text(
                f"INSERT INTO {_ADMIN_SCHEMA}.orgs (id, name, provisioning_state)"
                " VALUES ('r1301pending', 'Pending', 'provisioning')"
            )
        )
    await refresh_org_registry_view(tenant_db=tenant_db, admin_db=admin_db)

    rows = {r[0]: r for r in _read_view(sync_engine)}
    assert rows["r1301pending"] == (
        "r1301pending", "Pending", "provisioning", None, None, None,
    )


async def test_view_exists_only_in_the_root_org(planes):
    """No tenant may read another tenant's roster, so the registry is root's dataset alone."""
    admin_db, tenant_db, sync_engine = planes
    await refresh_org_registry_view(tenant_db=tenant_db, admin_db=admin_db)

    with sync_engine.begin() as conn:
        assert conn.execute(
            text(f"SELECT to_regclass('{_ROOT_SCHEMA}.{VIEW_NAME}')")
        ).scalar() is not None
        assert conn.execute(
            text(f"SELECT to_regclass('{_ACME_SCHEMA}.{VIEW_NAME}')")
        ).scalar() is None


async def test_seed_registers_the_view_in_the_meta_domain(planes, monkeypatch):
    """Building the view is not enough — an unregistered view is invisible to every query surface."""
    admin_db, tenant_db, sync_engine = planes

    from provisa.api.app import state as app_state
    from provisa.api.startup_seed import seed_org_registry_view

    monkeypatch.setattr(app_state, "admin_db", admin_db, raising=False)
    monkeypatch.setattr(app_state, "tenant_db", tenant_db, raising=False)

    assert await seed_org_registry_view() is True

    with sync_engine.begin() as conn:
        row = conn.execute(
            text(
                f"SELECT id, source_id, domain_id, schema_name FROM"
                f" {_ROOT_SCHEMA}.registered_tables WHERE table_name = :t"
            ),
            {"t": VIEW_NAME},
        ).one()
        assert (row[1], row[2], row[3]) == ("provisa-admin", "meta", _ROOT_SCHEMA)

        cols = conn.execute(
            text(
                f"SELECT column_name FROM {_ROOT_SCHEMA}.table_columns WHERE table_id = :i"
                " ORDER BY column_name"
            ),
            {"i": row[0]},
        ).scalars().all()
    assert set(cols) == set(VIEW_COLUMNS)


async def test_seed_is_idempotent(planes, monkeypatch):
    """Startup runs it every boot, and org create/delete runs it again — it must not accumulate."""
    admin_db, tenant_db, sync_engine = planes

    from provisa.api.app import state as app_state
    from provisa.api.startup_seed import seed_org_registry_view

    monkeypatch.setattr(app_state, "admin_db", admin_db, raising=False)
    monkeypatch.setattr(app_state, "tenant_db", tenant_db, raising=False)

    await seed_org_registry_view()
    await seed_org_registry_view()

    with sync_engine.begin() as conn:
        registrations = conn.execute(
            text(
                f"SELECT count(*) FROM {_ROOT_SCHEMA}.registered_tables WHERE table_name = :t"
            ),
            {"t": VIEW_NAME},
        ).scalar()
    assert registrations == 1


async def test_non_postgresql_plane_is_refused_not_silently_skipped(planes, tmp_path):
    """The view is PostgreSQL DDL. A SQLite platform plane is a supported topology, so the caller is
    told why it did not happen rather than left believing it did."""
    admin_db, tenant_db, _sync = planes
    sqlite_admin = Database(
        create_engine_from_url(f"sqlite+aiosqlite:///{tmp_path}/platform.db"), name="admin"
    )
    with pytest.raises(RegistryViewUnavailable):
        await refresh_org_registry_view(tenant_db=tenant_db, admin_db=sqlite_admin)
    await sqlite_admin.close()


def test_empty_deployment_still_yields_a_typed_view():
    """Before any org schema exists the holder union has no branches; the statement must still be
    valid SQL with the same column types, or the first boot fails before the first org is created."""
    sql = build_view_sql(root_schema="org_x", admin_schema="pub", org_schemas={})
    assert "WHERE false" in sql
    for col in VIEW_COLUMNS:
        assert col in sql
