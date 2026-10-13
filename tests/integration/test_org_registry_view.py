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

The control plane is the test's own PostgreSQL database (REQ-2014): the platform registry in
``public``, the root org ``default``'s schema and a second org's beside it. Seeding runs on the
control plane's synchronous engine so the async engines are only ever driven inside the test's own
event loop.
"""

from __future__ import annotations

import pytest
import pytest_asyncio
from sqlalchemy import text

from provisa.core.db import init_schema
from provisa.core.org_registry_view import (
    VIEW_COLUMNS,
    VIEW_NAME,
    RegistryViewUnavailable,
    build_view_sql,
    refresh_org_registry_view,
)
from provisa.core.database import Database, create_engine_from_url
from tests.pg_control_plane import SCHEMA_SQL

pytestmark = [pytest.mark.integration]

_ROOT = "default"
_ACME = "r1301acme"
_ROOT_SCHEMA = f"org_{_ROOT}"
_ACME_SCHEMA = f"org_{_ACME}"


async def _prepare(plane):
    """Two provisioned orgs, each with its own org_admin: the control plane's org ``default`` (the
    root, named Enterprise) and a second org laid out from schema.sql."""
    await init_schema(plane.org(_ACME), SCHEMA_SQL, org_id=_ACME)
    engine = plane.engine()
    with engine.begin() as conn:
        conn.execute(text("UPDATE orgs SET name = 'Enterprise' WHERE id = :i"), {"i": _ROOT})
        conn.execute(text("INSERT INTO orgs (id, name) VALUES (:i, :n)"), {"i": _ACME, "n": "Acme"})
        conn.execute(
            text("INSERT INTO user_profiles (user_id, email, display_name) VALUES (:u, :e, :d)"),
            [
                {"u": "uid-root", "e": "root@example.test", "d": "Root Admin"},
                {"u": "uid-acme", "e": "acme@example.test", "d": "Acme Admin"},
            ],
        )
        # every authenticated request mirrors the caller into user_directory (REQ-1439).
        for schema, uid in ((_ROOT_SCHEMA, "uid-root"), (_ACME_SCHEMA, "uid-acme")):
            conn.execute(
                text(
                    f"INSERT INTO {schema}.user_role_assignments (user_id, role_id, domain_id)"
                    f" VALUES ('{uid}', 'org_admin', '*')"
                )
            )
        # The root org carries the dataset catalog: the view is registered there as a meta-domain
        # table, whose source is the platform database.
        conn.execute(
            text(
                f"INSERT INTO {_ROOT_SCHEMA}.sources (id, type) VALUES ('provisa-admin', 'postgres')"
            )
        )
        conn.execute(
            text(f"INSERT INTO {_ROOT_SCHEMA}.domains (id) VALUES ('meta') ON CONFLICT DO NOTHING")
        )
    return engine


@pytest_asyncio.fixture
async def planes(control_plane):
    sync_engine = await _prepare(control_plane)
    yield control_plane.platform, control_plane.org(_ROOT), sync_engine


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
    await refresh_org_registry_view(model_db=tenant_db, admin_db=admin_db)

    assert _read_view(sync_engine) == [
        (_ROOT, "Enterprise", "ready", "uid-root", "root@example.test", "Root Admin"),
        (_ACME, "Acme", "ready", "uid-acme", "acme@example.test", "Acme Admin"),
    ]


async def test_view_reads_live_rows_without_a_rebuild(planes):
    """It is a view, not a snapshot: a role granted after the build shows up on the next read."""
    admin_db, tenant_db, sync_engine = planes
    await refresh_org_registry_view(model_db=tenant_db, admin_db=admin_db)

    with sync_engine.begin() as conn:
        conn.execute(
            text("INSERT INTO user_profiles (user_id, email, display_name) VALUES (:u, :e, :d)"),
            {"u": "uid-acme2", "e": "second@example.test", "d": "Second Admin"},
        )
        conn.execute(
            text(
                f"INSERT INTO {_ACME_SCHEMA}.user_role_assignments (user_id, role_id, domain_id)"
                " VALUES ('uid-acme2', 'org_admin', '*')"
            )
        )

    rows = _read_view(sync_engine)
    assert [r[0] for r in rows] == [_ROOT, _ACME, _ACME]
    assert {r[3] for r in rows if r[0] == _ACME} == {"uid-acme", "uid-acme2"}


async def test_org_without_a_provisioned_schema_keeps_its_row(planes):
    """A still-provisioning org has no user_role_assignments table. It is listed with no admin —
    absent from the registry would read as "this org does not exist", which is the wrong answer."""
    admin_db, tenant_db, sync_engine = planes
    with sync_engine.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO orgs (id, name, provisioning_state)"
                " VALUES ('r1301pending', 'Pending', 'provisioning')"
            )
        )
    await refresh_org_registry_view(model_db=tenant_db, admin_db=admin_db)

    rows = {r[0]: r for r in _read_view(sync_engine)}
    assert rows["r1301pending"] == (
        "r1301pending",
        "Pending",
        "provisioning",
        None,
        None,
        None,
    )


async def test_view_exists_only_in_the_root_org(planes):
    """No tenant may read another tenant's roster, so the registry is root's dataset alone."""
    admin_db, tenant_db, sync_engine = planes
    await refresh_org_registry_view(model_db=tenant_db, admin_db=admin_db)

    with sync_engine.begin() as conn:
        assert (
            conn.execute(text(f"SELECT to_regclass('{_ROOT_SCHEMA}.{VIEW_NAME}')")).scalar()
            is not None
        )
        assert (
            conn.execute(text(f"SELECT to_regclass('{_ACME_SCHEMA}.{VIEW_NAME}')")).scalar() is None
        )


async def test_seed_registers_the_view_in_the_meta_domain(planes, monkeypatch):
    """Building the view is not enough — an unregistered view is invisible to every query surface."""
    admin_db, tenant_db, sync_engine = planes

    from provisa.api.app import state as app_state
    from provisa.api.startup_seed import seed_org_registry_view

    monkeypatch.setattr(app_state, "admin_db", admin_db, raising=False)
    monkeypatch.setattr(app_state, "tenant_db", tenant_db, raising=False)
    monkeypatch.setattr(app_state, "record_db", app_state.tenant_db, raising=False)
    monkeypatch.setattr(app_state, "model_db", app_state.tenant_db, raising=False)

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

        cols = (
            conn.execute(
                text(
                    f"SELECT column_name FROM {_ROOT_SCHEMA}.table_columns WHERE table_id = :i"
                    " ORDER BY column_name"
                ),
                {"i": row[0]},
            )
            .scalars()
            .all()
        )
    assert set(cols) == set(VIEW_COLUMNS)


async def test_seed_is_idempotent(planes, monkeypatch):
    """Startup runs it every boot, and org create/delete runs it again — it must not accumulate."""
    admin_db, tenant_db, sync_engine = planes

    from provisa.api.app import state as app_state
    from provisa.api.startup_seed import seed_org_registry_view

    monkeypatch.setattr(app_state, "admin_db", admin_db, raising=False)
    monkeypatch.setattr(app_state, "tenant_db", tenant_db, raising=False)
    monkeypatch.setattr(app_state, "record_db", app_state.tenant_db, raising=False)
    monkeypatch.setattr(app_state, "model_db", app_state.tenant_db, raising=False)

    await seed_org_registry_view()
    await seed_org_registry_view()

    with sync_engine.begin() as conn:
        registrations = conn.execute(
            text(f"SELECT count(*) FROM {_ROOT_SCHEMA}.registered_tables WHERE table_name = :t"),
            {"t": VIEW_NAME},
        ).scalar()
    assert registrations == 1


async def test_non_postgresql_plane_is_refused_not_silently_skipped(planes, tmp_path):
    """The view is PostgreSQL DDL. A SQLite platform plane is a supported topology, so the caller is
    told why it did not happen rather than left believing it did."""
    admin_db, tenant_db, _sync = planes
    sqlite_admin = Database(
        create_engine_from_url(f"sqlite+pysqlite:///{tmp_path}/platform.db"), name="admin"
    )
    with pytest.raises(RegistryViewUnavailable):
        await refresh_org_registry_view(model_db=tenant_db, admin_db=sqlite_admin)
    await sqlite_admin.close()


def test_empty_deployment_still_yields_a_typed_view():
    """Before any org schema exists the holder union has no branches; the statement must still be
    valid SQL with the same column types, or the first boot fails before the first org is created."""
    sql = build_view_sql(root_schema="org_x", admin_schema="pub", org_schemas={})
    assert "WHERE false" in sql
    for col in VIEW_COLUMNS:
        assert col in sql
