# Copyright (c) 2026 Kenneth Stott
# Canary: 8e3b1d76-4c29-4a5f-b0e8-6d2f9a7c1e43
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""``CREATE VIEW`` over pgwire, on a real server, is refused and leaves nothing behind.

A role holding ``ddl`` that sees one of two orders (a row filter) with a column it may not read
sends ``CREATE VIEW ... AS SELECT *`` over the live table. The statement used to go to the
engine as written. It is now refused with SQLSTATE 0A000, naming the admin action: nothing is
run on the engine, no relation is created at the source, the model holds no row for it, and no
role can read the name afterwards."""

from __future__ import annotations

import os

import psycopg
import pytest
import sqlalchemy as sa

from tests.integration.worker_boot_harness import WorkerBoot, _config

pytestmark = [pytest.mark.integration]

_PG_HOST = os.environ.get("PG_HOST", "localhost")
_PG_PORT = int(os.environ.get("PG_PORT", "5432"))
_ROLES = ["org_admin", "analyst", "maker"]
_STATEMENTS = [
    "CREATE VIEW leak AS SELECT * FROM sales_pg.public.orders",
    "CREATE OR REPLACE VIEW leak AS SELECT * FROM sales.orders",
    "CREATE MATERIALIZED VIEW leak AS SELECT id, region, amount FROM public.orders",
    "/* nightly */ create view leak as select * from orders",
]


def _server(domain: dict) -> WorkerBoot:
    base = _config(_PG_HOST, _PG_PORT, "unused")
    orders = base["tables"][0]
    for column in orders["columns"]:
        column["visible_to"] = _ROLES
    orders["columns"].append(
        {"name": "amount", "data_type": "integer", "visible_to": ["org_admin"]}
    )
    return WorkerBoot(
        1,
        pg_host=_PG_HOST,
        pg_port=_PG_PORT,
        extra_config={
            "tables": [orders],
            "domains": [{"id": "sales", "description": "pgwire view refusal", **domain}],
            "roles": base["roles"]
            + [
                {
                    "id": "maker",
                    "capabilities": ["query_development", "ddl", "full_results"],
                    "domain_access": ["*"],
                }
            ],
            "rls_rules": [{"table_id": "orders", "role_id": "maker", "filter": "region = 'east'"}],
        },
        env={"PROVISA_REDIRECT_ENABLED": "false"},
    )


# The domain's DDL target: an engine catalog (the default), and a registered source's catalog.
_TARGETS = {
    "engine catalog": {},
    "source catalog": {"ddl_catalog": "sales_pg", "ddl_schema": "public"},
}


@pytest.fixture(scope="module", params=sorted(_TARGETS))
def server(request):
    boot = _server(_TARGETS[request.param])
    boot.create_database()
    try:
        own = sa.create_engine(boot.url, isolation_level="AUTOCOMMIT")
        with own.connect() as conn:
            conn.execute(sa.text("ALTER TABLE public.orders ADD COLUMN amount integer"))
            conn.execute(sa.text("UPDATE public.orders SET amount = id * 100"))
        own.dispose()
        boot.start()
        boot.wait_all_ready(timeout=300)
        yield boot
    finally:
        boot.cleanup()


def _connect(boot, role: str):
    return psycopg.connect(
        host="127.0.0.1",
        port=boot.ports["pgwire"],
        user=role,
        password="provisa",
        dbname="provisa",
        autocommit=True,
        connect_timeout=30,
    )


def test_what_the_creating_role_may_read_of_the_table(server):
    with _connect(server, "maker") as conn:
        assert conn.execute("SELECT id FROM sales.orders").fetchall() == [(1,)]  # of two
        with pytest.raises(psycopg.Error, match="amount"):
            conn.execute("SELECT amount FROM sales.orders")


@pytest.mark.parametrize("statement", _STATEMENTS)
def test_create_view_is_refused_naming_the_admin_action(server, statement):
    with _connect(server, "maker") as conn:
        with pytest.raises(psycopg.Error) as raised:
            conn.execute(statement)
        assert raised.value.sqlstate == "0A000"
        message = str(raised.value)
        assert "CREATE VIEW is not available over pgwire" in message
        assert "registerTable" in message
        # The connection is still usable, and still the filtered role's.
        assert conn.execute("SELECT id FROM sales.orders").fetchall() == [(1,)]


def test_the_refused_statements_left_nothing_behind(server):
    with _connect(server, "maker") as conn:
        for statement in _STATEMENTS:
            with pytest.raises(psycopg.Error):
                conn.execute(statement)

    own = sa.create_engine(server.url)
    with own.connect() as conn:
        # No relation at the source ...
        relations = conn.execute(
            sa.text(
                "SELECT n.nspname, c.relname FROM pg_class c JOIN pg_namespace n "
                "ON n.oid = c.relnamespace WHERE c.relname = 'leak'"
            )
        ).fetchall()
        assert relations == []
        # ... and no row in the model, in any org schema of the control plane.
        schemas = conn.execute(
            sa.text(
                "SELECT table_schema FROM information_schema.tables "
                "WHERE table_name = 'registered_tables'"
            )
        ).fetchall()
        assert schemas
        for (schema,) in schemas:
            held = conn.execute(
                sa.text(
                    f"SELECT count(*) FROM \"{schema}\".registered_tables WHERE table_name = 'leak'"
                )
            ).scalar()
            assert held == 0, schema
    own.dispose()

    # Nothing was run as DDL on the engine or on a source connection.
    log = server.log_text()
    assert "DDL(engine)" not in log and "DDL(direct" not in log

    # And the name is no relation to anyone.
    for role in _ROLES:
        with _connect(server, role) as conn:
            with pytest.raises(psycopg.Error, match="not a registered table"):
                conn.execute("SELECT * FROM leak")
