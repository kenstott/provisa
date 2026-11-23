# Copyright (c) 2026 Kenneth Stott
# Canary: 3b9e4f12-6a7d-4c28-b5e1-9f0d2a8c7e46
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The SQLAlchemy dialect reflects only what the connecting user's role is served -- on a real
server with auth enforced (REQ-363).

One server, two domains: ``sales.orders`` is granted to ``seller``, ``hr.staff`` to ``hr_reader``,
and each table's ``secret`` column to ``org_admin`` alone. ``sam`` (seller), ``hana`` (hr_reader)
and ``both`` connect through ``provisa+http://user:password@host:port/?role=<roles>`` and reflect
the catalog with SQLAlchemy's inspector: each is listed the schemas, tables and columns its roles are served
and nothing else. (A server that authenticates nobody has no role to narrow by and lists the
whole catalog -- endpoint_dev.catalog_endpoint -- so the scope is checked with auth on.)

Lands on the TEST instance only: one real server over a database the harness creates."""

from __future__ import annotations

import os

import bcrypt
import pytest
import sqlalchemy as sa

from tests.integration.worker_boot_harness import WorkerBoot

pytestmark = [pytest.mark.integration]

_PG_HOST = os.environ.get("PG_HOST", "localhost")
_PG_PORT = int(os.environ.get("PG_PORT", "5432"))
_PASSWORD = "correct horse"
_DDL = [
    "ALTER TABLE public.orders ADD COLUMN secret varchar",
    "CREATE TABLE public.staff (id integer PRIMARY KEY, region varchar, secret varchar)",
]


def _model() -> dict:
    def _table(domain: str, table: str, role: str) -> dict:
        return {
            "source_id": "sales-pg",
            "domain_id": domain,
            "schema": "public",
            "table": table,
            "columns": [
                {"name": "id", "data_type": "integer", "visible_to": [role]},
                {"name": "region", "data_type": "varchar", "visible_to": [role]},
                {"name": "secret", "data_type": "varchar", "visible_to": ["org_admin"]},
            ],
        }

    def _user(name: str, roles: list[str]) -> dict:
        hashed = bcrypt.hashpw(_PASSWORD.encode(), bcrypt.gensalt()).decode()
        return {"username": name, "password_hash": hashed, "roles": roles}

    reads = ["query_development", "full_results"]
    return {
        "auth": {
            "provider": "simple",
            "allow_simple_auth": True,
            "jwt_secret": "sqlalchemy-dialect-scope-test-signing-key",
            "default_role": "seller",
            "simple": {
                "users": [
                    _user("sam", ["seller"]),
                    _user("hana", ["hr_reader"]),
                    _user("both", ["seller", "hr_reader"]),
                ]
            },
        },
        "domains": [
            {"id": "sales", "description": "orders"},
            {"id": "hr", "description": "staff"},
        ],
        "tables": [_table("sales", "orders", "seller"), _table("hr", "staff", "hr_reader")],
        "roles": [
            {
                "description": "Test role.",
                "id": "seller",
                "capabilities": reads,
                "domain_access": ["sales"],
            },
            {
                "description": "Test role.",
                "id": "hr_reader",
                "capabilities": reads,
                "domain_access": ["hr"],
            },
        ],
    }


@pytest.fixture(scope="module")
def server():
    boot = WorkerBoot(
        1,
        pg_host=_PG_HOST,
        pg_port=_PG_PORT,
        extra_config=_model(),
        env={"PROVISA_REDIRECT_ENABLED": "false"},
    )
    boot.create_database()
    own = sa.create_engine(boot.url, isolation_level="AUTOCOMMIT")
    with own.connect() as conn:
        for ddl in _DDL:
            conn.execute(sa.text(ddl))
    own.dispose()
    try:
        boot.start()
        boot.wait_all_ready(timeout=300)
        yield boot
    finally:
        boot.cleanup()


def _reflect(boot, user: str, role: str) -> dict[str, dict[str, list[str]]]:
    """{schema: {table: [columns]}} as ``user``'s SQLAlchemy inspector reflects it acting as
    ``role`` (``?role=``, a comma-separated set for several), for the two domains of the model."""
    url = sa.engine.URL.create(
        "provisa+http",
        username=user,
        password=_PASSWORD,
        host="127.0.0.1",
        port=boot.ports["http"],
        query={"role": role},
    )
    engine = sa.create_engine(url)
    try:
        inspector = sa.inspect(engine)
        out: dict[str, dict[str, list[str]]] = {}
        for schema in sorted({"sales", "hr"} & set(inspector.get_schema_names())):
            out[schema] = {
                table: sorted(c["name"] for c in inspector.get_columns(table, schema=schema))
                for table in inspector.get_table_names(schema=schema)
            }
        return out
    finally:
        engine.dispose()


@pytest.mark.parametrize(
    ("user", "role", "expected"),
    [
        ("sam", "seller", {"sales": {"orders": ["id", "region"]}}),
        ("hana", "hr_reader", {"hr": {"staff": ["id", "region"]}}),
        (
            "both",
            "seller,hr_reader",
            {"hr": {"staff": ["id", "region"]}, "sales": {"orders": ["id", "region"]}},
        ),
    ],
)
def test_the_dialect_reflects_only_what_the_users_roles_are_served(server, user, role, expected):
    assert _reflect(server, user, role) == expected
