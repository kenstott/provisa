# Copyright (c) 2026 Kenneth Stott
# Canary: 4e8a1c97-3b26-4f50-9d7e-a2c5f0b8d134
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A synthetic dataset on a real server (REQ-1939, REQ-1487).

customers and orders are profiled in prod. A dev environment defines a dataset of both at scale 2
from those profile runs and generates it: in dev the two tables read their generated copies --
twice the customers, orders following the profiled children-per-parent distribution, its skew
kept -- while prod reads its own data. A statement joining a synthetic table to a table reading
real data is refused; a dataset naming orders without customers is refused; dropping the dataset
restores dev's bindings.
"""

from __future__ import annotations

import json
import os
import urllib.error
import urllib.request
from typing import Any

import pytest
from tests.helpers import PROFILER_RUN_DEFAULTS
import sqlalchemy as sa

from tests.integration.worker_boot_harness import WorkerBoot, _config

pytestmark = [pytest.mark.integration]

_ROLES = ["org_admin", "analyst"]
_CUSTOMERS = 200
# Customer 1 places 50 orders (the hot key); customers 2..101 place 5 each; the rest none.
_ORDERS = [(1, 1)] * 50 + [(c, c) for c in range(2, 102) for _ in range(5)]


def _col(name: str, data_type: str, **extra) -> dict:
    return {"name": name, "data_type": data_type, "visible_to": _ROLES, **extra}


def _table(name: str, columns: list[dict], **extra) -> dict:
    return {
        "source_id": "sales-pg",
        "domain_id": "sales",
        "schema": "public",
        "table": name,
        "columns": columns,
        **extra,
    }


@pytest.fixture(scope="module")
def server():
    pg_host = os.environ.get("PG_HOST", "localhost")
    pg_port = int(os.environ.get("PG_PORT", "5432"))
    boot = WorkerBoot(
        1, pg_host=pg_host, pg_port=pg_port, env={"PROVISA_REDIRECT_ENABLED": "false"}
    )
    base = _config(pg_host, pg_port, boot.database)
    boot._extra_config = {
        "sources": base["sources"]
        + [
            {
                "id": "profiler",
                "type": "data_profiler",
                "mapping": {"cron": "0 3 * * *", **PROFILER_RUN_DEFAULTS},
            }
        ],
        "tables": [
            _table(
                "customers",
                [
                    _col("id", "integer", is_primary_key=True),
                    _col("region", "varchar"),
                    _col("email", "varchar"),
                    # REQ-1494: declared fakes -- tier's values and shares stated; segment's
                    # values named, their shares measured.
                    _col("tier", "varchar", fake="categories((gold, silver), (.25, .75))"),
                    _col("segment", "varchar", fake="categories((retail, wholesale, online))"),
                    # REQ-1939, BOOLEANS: an undeclared boolean is bool(), at its profiled share.
                    _col("active", "boolean"),
                    # REQ-1939, GENERATION IN PASSES: a rule over the customer's generated
                    # purchases, computed in the second pass.
                    _col("spent", "integer", synthetic_rule="sql_group(SUM(purchases.amount))"),
                ],
                profiler_source_id="profiler",
            ),
            _table(
                "purchases",
                [
                    _col("id", "integer", is_primary_key=True),
                    _col("customer_id", "integer"),
                    # REQ-1494: a declared distribution decides its generated values.
                    _col("amount", "integer", fake="uniform(min=1, max=9)"),
                    # A window over the generated rows, computed in the table's own statement.
                    _col(
                        "position",
                        "integer",
                        synthetic_rule=(
                            "sql_group(ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY id))"
                        ),
                    ),
                ],
                profiler_source_id="profiler",
            ),
            # Its phone is tagged pii and declares no fake kind: it cannot be generated.
            _table(
                "contacts",
                [_col("id", "integer", is_primary_key=True), _col("phone", "varchar")],
                profiler_source_id="profiler",
            ),
            # Never in the dataset: dev reads it from its binding, as real data.
            _table(
                "orders", [_col("id", "integer", is_primary_key=True), _col("region", "varchar")]
            ),
        ],
        "relationships": [
            {
                "id": "customer-purchases",
                "source_table_id": "customers",
                "source_column": "id",
                "target_table_id": "purchases",
                "target_column": "customer_id",
                "cardinality": "one-to-many",
                "graphql_alias": "purchases",
            }
        ],
        "tag_assignments": [
            {
                "tag_id": "pii",
                "object_type": "column",
                "table_ref": "sales-pg.public.contacts",
                "column_name": "phone",
            }
        ],
        "roles": [
            {
                "id": "analyst",
                "capabilities": ["query_development", "full_results"],
                "domain_access": ["*"],
            }
        ],
    }
    boot.create_database()
    try:
        engine = sa.create_engine(boot.url, isolation_level="AUTOCOMMIT")
        with engine.connect() as conn:
            conn.execute(
                sa.text(
                    "CREATE TABLE public.customers (id integer PRIMARY KEY, region text, email text, "
                    "tier text, segment text, active boolean, spent integer)"
                )
            )
            conn.execute(
                sa.text(
                    "CREATE TABLE public.purchases (id integer PRIMARY KEY, customer_id integer, "
                    "amount integer, position integer)"
                )
            )
            conn.execute(
                sa.text("CREATE TABLE public.contacts (id integer PRIMARY KEY, phone text)")
            )
            conn.execute(
                sa.text("INSERT INTO public.contacts VALUES (1, '555-0101'), (2, '555-0102')")
            )
            for c in range(1, _CUSTOMERS + 1):
                conn.execute(
                    sa.text("INSERT INTO public.customers VALUES (:i, :r, :e, :t, :s, :a, 0)"),
                    {
                        "i": c,
                        "r": ("east", "west", "north")[c % 3],
                        "e": f"c{c}@example.com",
                        "t": "bronze",
                        # retail on 3 rows in 4, wholesale on 1 in 4; online never recorded.
                        "s": "wholesale" if c % 4 == 0 else "retail",
                        "a": c % 5 != 0,  # true on 4 rows in 5
                    },
                )
            for i, (cust, _) in enumerate(_ORDERS, 1):
                conn.execute(
                    sa.text("INSERT INTO public.purchases VALUES (:i, :c, :a, 1)"),
                    {"i": i, "c": cust, "a": 10 + i % 90},
                )
        engine.dispose()
        boot.start()
        boot.wait_all_ready(timeout=300)
        yield boot
    finally:
        boot.cleanup()


def _call(
    boot,
    method: str,
    path: str,
    body: Any = None,
    *,
    env: str | None = None,
    role: str = "org_admin",
) -> tuple[int, Any]:
    headers = {"Content-Type": "application/json", "x-provisa-role": role}
    if env is not None:
        headers["x-provisa-env"] = env
    req = urllib.request.Request(
        f"http://127.0.0.1:{boot.ports['http']}{path}",
        data=None if body is None else json.dumps(body).encode(),
        headers=headers,
        method=method,
    )
    try:
        with urllib.request.urlopen(req, timeout=600) as resp:
            return resp.status, json.loads(resp.read().decode())
    except urllib.error.HTTPError as exc:
        return exc.code, {"error": exc.read().decode()}


def _sql(boot, sql: str, env: str | None = None) -> tuple[int, Any]:
    status, body = _call(boot, "POST", "/data/sql", {"sql": sql}, env=env)
    return status, body["data"]["sql"] if status == 200 and "data" in body else body


def _one(boot, sql: str, env: str | None = None) -> Any:
    status, rows = _sql(boot, sql, env)
    assert status == 200, rows
    return next(iter(rows[0].values()))


def _environment(boot, name: str) -> dict:
    """A new environment ``name``, with the prod profile runs a dataset of it may name."""
    status, body = _call(
        boot,
        "POST",
        f"/admin/orgs/{boot.org_id}/environments",
        {"name": name, "inherit_connections": True},
    )
    assert status == 200, body
    status, runs = _call(boot, "GET", "/admin/synthetic-datasets/-/profile-runs?env=prod", env=name)
    assert status == 200, runs
    by_name = {r["tableName"]: r for r in runs}
    assert set(by_name) == {"customers", "purchases", "contacts"}, runs
    return {"boot": boot, "tables": by_name, "env": name}


@pytest.fixture(scope="module")
def profiled(server):
    status, body = _call(server, "POST", "/admin/profilers/profiler/run")
    assert status == 200 and [o["error"] for o in body] == [None, None, None], body
    return server


@pytest.fixture(scope="module")
def dev(profiled) -> dict:
    return _environment(profiled, "dev")


def _define(dev: dict, names: list[str], dataset: str = "load_test") -> tuple[int, Any]:
    return _call(
        dev["boot"],
        "PUT",
        f"/admin/synthetic-datasets/{dataset}",
        {
            "seed": 7,
            "scale": 2,
            "tables": [
                {
                    "tableId": dev["tables"][n]["tableId"],
                    "profileEnv": "prod",
                    "runId": dev["tables"][n]["runs"][0]["runId"],
                }
                for n in names
            ],
        },
        env=dev["env"],
    )


@pytest.fixture(scope="module")
def generated(dev) -> dict:
    status, body = _define(dev, ["customers", "purchases"])
    assert status == 200, body
    status, body = _call(
        dev["boot"], "POST", "/admin/synthetic-datasets/load_test/generate", env="dev"
    )
    assert status == 200, (body, dev["boot"].log_text()[-6000:])
    return dev


def test_a_dataset_naming_a_child_without_its_parent_is_refused(dev):
    status, body = _define(dev, ["purchases"])
    assert status == 422, body
    assert "customers" in body["error"], body


def test_the_dataset_is_read_in_its_environment_only(generated):
    boot = generated["boot"]
    assert _one(boot, "SELECT COUNT(*) AS n FROM sales.customers", env="dev") == 2 * _CUSTOMERS
    assert _one(boot, "SELECT COUNT(*) AS n FROM sales.customers") == _CUSTOMERS
    assert _one(boot, "SELECT COUNT(*) AS n FROM sales.purchases") == len(_ORDERS)


def test_children_per_parent_and_their_skew_survive(generated):
    boot = generated["boot"]
    per_parent = (
        "SELECT MAX(n) AS m FROM (SELECT customer_id, COUNT(*) AS n FROM sales.purchases "
        "GROUP BY customer_id) x"
    )
    hot = _one(boot, per_parent, env="dev")
    # The profiled customers place 0 or 5 orders; customer 1 places 50. That hot key keeps its
    # count, scaled with the customers, on the first generated customer: 50 x 2.
    assert hot == 100, hot
    assert (
        _one(boot, "SELECT COUNT(*) AS n FROM sales.purchases WHERE customer_id = 1", env="dev")
        == 100
    )
    orders = _one(boot, "SELECT COUNT(*) AS n FROM sales.purchases", env="dev")
    # Twice the customers, each placing what the profiled distribution gives: about twice the orders.
    assert 1.6 * len(_ORDERS) <= orders <= 2.4 * len(_ORDERS), orders
    # Every order's customer is a generated customer.
    orphans = _one(
        boot,
        "SELECT COUNT(*) AS n FROM sales.purchases p LEFT JOIN sales.customers c "
        "ON c.id = p.customer_id WHERE c.id IS NULL",
        env="dev",
    )
    assert orphans == 0


def test_no_value_of_a_column_that_is_no_category_is_a_real_one(generated):
    """REQ-1939, A COLUMN'S FAKE SETTINGS DECIDE ITS VALUES: email is no category, so its values
    are generated from its shapes, never taken from the values recorded in its profile."""
    status, rows = _sql(generated["boot"], "SELECT email FROM sales.customers", env="dev")
    assert status == 200, rows
    real = {f"c{c}@example.com" for c in range(1, _CUSTOMERS + 1)}
    emails = {r["email"] for r in rows}
    assert emails and not real & emails
    assert all("@" in e for e in emails if e is not None)


def test_a_declared_fake_generates_every_value_of_its_column(generated):
    """REQ-1494, REQ-1939: a column with a fake is generated through it, as a faked read shows it."""
    status, rows = _sql(generated["boot"], "SELECT amount FROM sales.purchases", env="dev")
    assert status == 200, rows
    amounts = [r["amount"] for r in rows if r["amount"] is not None]
    assert amounts and all(1 <= a <= 9 for a in amounts)
    assert len(set(amounts)) > 3


def test_rules_spanning_rows_are_computed_over_the_generated_rows(generated):
    """REQ-1939, GENERATION IN PASSES: a window over the table, and a sum over each customer's
    generated purchases computed in the second pass."""
    boot = generated["boot"]
    status, rows = _sql(
        boot,
        "SELECT customer_id, id, position FROM sales.purchases ORDER BY customer_id, id",
        "dev",
    )
    assert status == 200, rows
    seen: dict[int, int] = {}
    for r in rows:
        seen[r["customer_id"]] = seen.get(r["customer_id"], 0) + 1
        assert r["position"] == seen[r["customer_id"]]
    sums: dict[int, int] = {}
    status, rows = _sql(boot, "SELECT customer_id, amount FROM sales.purchases", "dev")
    assert status == 200, rows
    for r in rows:
        sums[r["customer_id"]] = sums.get(r["customer_id"], 0) + r["amount"]
    status, rows = _sql(boot, "SELECT id, spent FROM sales.customers", "dev")
    assert status == 200, rows
    assert rows and all(r["spent"] == sums.get(r["id"]) for r in rows)


def test_a_pii_column_with_no_fake_kind_refuses_by_name(dev):
    status, body = _define(dev, ["contacts"], dataset="pii_test")
    assert status == 200, body
    status, body = _call(
        dev["boot"], "POST", "/admin/synthetic-datasets/pii_test/generate", env="dev"
    )
    assert status == 422, body
    assert "contacts.phone" in body["error"], body


def test_a_low_cardinality_column_keeps_its_shares_on_generated_values(generated):
    """With no categories fake, region's three values are generated -- three values at the
    profiled shares (a third each), none of them a real one."""
    status, rows = _sql(
        generated["boot"],
        "SELECT region, COUNT(*) AS n FROM sales.customers GROUP BY region",
        env="dev",
    )
    assert status == 200, rows
    assert len(rows) == 3 and not {"east", "west", "north"} & {r["region"] for r in rows}
    for r in rows:
        assert r["n"] / (2 * _CUSTOMERS) == pytest.approx(1 / 3, abs=0.07)


def test_a_synthetic_table_is_never_read_beside_real_data(generated):
    status, body = _sql(
        generated["boot"],
        "SELECT id FROM sales.customers UNION ALL SELECT id FROM sales.orders",
        env="dev",
    )
    assert status != 200, body
    assert "synthetic dataset" in json.dumps(body), body


def test_the_report_compares_the_copy_with_its_profiles(generated):
    status, rows = _call(
        generated["boot"], "GET", "/admin/synthetic-datasets/load_test/report", env="dev"
    )
    assert status == 200, rows
    by = {(r["table"], r["column"], r["measure"]): r for r in rows}
    customers = by[("customers", None, "row_count")]
    assert customers["delta"] == 2.0, customers
    fanout = by[("customers", None, "fanout_ks")]
    assert fanout["delta"] < 0.2, fanout
    assert ("customers", "email", "undeclared_fake") in by


def test_dropping_the_dataset_restores_the_binding(profiled):
    """In an environment of its own, so no other test reads a dataset this one drops."""
    env = _environment(profiled, "dropped")
    status, body = _define(env, ["customers", "purchases"], dataset="drop_test")
    assert status == 200, body
    status, body = _call(
        profiled, "POST", "/admin/synthetic-datasets/drop_test/generate", env="dropped"
    )
    assert status == 200, body
    count = "SELECT COUNT(*) AS n FROM sales.customers"
    assert _one(profiled, count, env="dropped") == 2 * _CUSTOMERS
    status, body = _call(profiled, "DELETE", "/admin/synthetic-datasets/drop_test", env="dropped")
    assert status == 200, body
    assert _one(profiled, count, env="dropped") == _CUSTOMERS


def test_an_environment_can_start_on_synthetic_data(profiled):
    """REQ-1939, BOOTSTRAPPING AN ENVIRONMENT: the new environment is generated from prod's latest
    profile runs at its own scale."""
    boot = profiled
    status, body = _call(
        boot,
        "POST",
        f"/admin/orgs/{boot.org_id}/environments",
        {
            "name": "seeded",
            "inherit_connections": True,
            "synthetic": {
                "dataset": "boot",
                "tables": ["customers", "purchases"],
                "scale": 0.5,
                "seed": 3,
            },
        },
    )
    assert status == 200, body
    assert _one(boot, "SELECT COUNT(*) AS n FROM sales.customers", env="seeded") == _CUSTOMERS // 2
    status, emails = _sql(boot, "SELECT email FROM sales.customers", env="seeded")
    assert status == 200, emails
    assert all("@" in r["email"] for r in emails if r["email"] is not None)
    # Each real email is held by one row: none is a hot value, so none is drawn as itself.
    real = {f"c{c}@example.com" for c in range(1, _CUSTOMERS + 1)}
    assert not real & {r["email"] for r in emails}


def _shares(boot, column: str) -> dict[Any, float]:
    status, rows = _sql(
        boot, f"SELECT {column} AS v, COUNT(*) AS n FROM sales.customers GROUP BY {column}", "dev"
    )
    assert status == 200, rows
    return {r["v"]: r["n"] / (2 * _CUSTOMERS) for r in rows}


def test_a_declared_categories_fake_decides_the_values(generated):
    """REQ-1494, CATEGORY WEIGHTS: tier is drawn from its stated values at its stated shares;
    the real value (bronze) is never one of them."""
    tier = _shares(generated["boot"], "tier")
    assert set(tier) == {"gold", "silver"}, tier
    assert tier["gold"] == pytest.approx(0.25, abs=0.07)


def test_named_categories_take_their_measured_shares(generated):
    """REQ-1494, CATEGORIES FROM THE PROFILE: retail and wholesale at their profiled shares; online,
    never recorded, shares what remains -- nothing."""
    segment = _shares(generated["boot"], "segment")
    assert set(segment) <= {"retail", "wholesale", "online"}, segment
    assert segment["retail"] == pytest.approx(0.75, abs=0.07)
    assert segment.get("online", 0.0) == 0.0


def test_an_undeclared_boolean_keeps_its_share_of_true(generated):
    active = _shares(generated["boot"], "active")
    assert set(active) <= {True, False}, active
    assert active[True] == pytest.approx(0.8, abs=0.07)


def test_fill_from_profile_proposes_fakes_for_identifying_columns_and_rules_for_the_rest(
    profiled,
):
    """REQ-1494: from customers' latest profile run -- email and region (an address part, by its
    name) identifying, so fakes; active (boolean) a synthetic rule; tier and segment, already
    declared, nothing."""
    engine = sa.create_engine(profiled.url)
    with engine.connect() as conn:
        (table_id,) = conn.execute(
            sa.text(
                f"SELECT id FROM org_{profiled.org_id}.registered_tables WHERE table_name = 'customers'"
            )
        ).one()
    engine.dispose()
    status, body = _call(profiled, "POST", "/admin/fakes/propose", {"tableId": table_id})
    assert status == 200, body
    assert body["columns"]["email"] == {"fake": "email()"}
    assert body["columns"]["region"] == {"fake": "state()"}
    assert body["columns"]["active"] == {"syntheticRule": "bool()"}
    assert "tier" not in body["columns"] and "segment" not in body["columns"]
