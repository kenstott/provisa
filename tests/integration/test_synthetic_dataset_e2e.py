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
            # REQ-1939, DIFFERENTIAL PRIVACY: every column declares what it generates or is a
            # number or date, so a private dataset can generate it.
            _table(
                "accounts",
                [
                    _col("id", "integer", is_primary_key=True),
                    _col("balance", "double"),
                    _col("opened", "timestamp"),
                    _col("status", "varchar", fake="categories((open, closed))"),
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
                sa.text(
                    "CREATE TABLE public.accounts (id integer PRIMARY KEY, balance double precision, "
                    "opened timestamp, status text)"
                )
            )
            for a in range(1, 301):
                conn.execute(
                    sa.text(
                        "INSERT INTO public.accounts VALUES (:i, :b, TIMESTAMP '2024-01-01' + "
                        "make_interval(days => :d), :s)"
                    ),
                    {"i": a, "b": 100.0 + a, "d": a % 300, "s": "open" if a % 3 else "closed"},
                )
            # One account holds a balance no bound of a private dataset may reveal.
            conn.execute(sa.text("UPDATE public.accounts SET balance = 987654321 WHERE id = 7"))
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
        {"name": name, "data_mode": "inherit"},
    )
    assert status == 200, body
    status, runs = _call(boot, "GET", "/admin/synthetic-datasets/-/profile-runs?env=prod", env=name)
    assert status == 200, runs
    by_name = {r["tableName"]: r for r in runs}
    assert set(by_name) == {"customers", "purchases", "contacts", "accounts"}, runs
    return {"boot": boot, "tables": by_name, "env": name}


@pytest.fixture(scope="module")
def profiled(server):
    status, body = _call(server, "POST", "/admin/profilers/profiler/run")
    assert status == 200 and [o["error"] for o in body] == [None] * 4, body
    return server


@pytest.fixture(scope="module")
def dev(profiled) -> dict:
    return _environment(profiled, "dev")


def _define(
    dev: dict,
    names: list[str],
    dataset: str = "load_test",
    epsilon: float | None = None,
    closeness: tuple[float, int] | None = None,
) -> tuple[int, Any]:
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
            # REQ-1939: the last ten generated customers place exactly two purchases each.
            "fanoutConditions": (
                [
                    {
                        "relationship": "customer-purchases",
                        "condition": "id > 390",
                        "count": {"fixed": 2},
                    }
                ]
                if "purchases" in names
                else []
            ),
            "assertions": ["SELECT COUNT(*) > 0 FROM sales.purchases", "SELECT 1 = 2"]
            if "purchases" in names
            else [],
            "privateEpsilon": epsilon,
            "closenessThreshold": None if closeness is None else closeness[0],
            "closenessDraws": None if closeness is None else closeness[1],
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


def test_a_conditional_fanout_decides_the_children_of_the_parents_meeting_it(generated):
    status, rows = _sql(
        generated["boot"],
        "SELECT customer_id, COUNT(*) AS n FROM sales.purchases WHERE customer_id > 390 "
        "GROUP BY customer_id",
        "dev",
    )
    assert status == 200, rows
    assert len(rows) == 10 and all(r["n"] == 2 for r in rows), rows
    status, report = _call(
        generated["boot"], "GET", "/admin/synthetic-datasets/load_test/report", env="dev"
    )
    assert status == 200, report
    by = {(r["measure"], r["note"]): r for r in report}
    assert by[("conditional_parents", "id > 390")]["synthetic"] == 10.0
    assert by[("conditional_children", "id > 390")]["synthetic"] == 20.0
    # Assertions are reported, never enforced.
    assert by[("assertion", "SELECT COUNT(*) > 0 FROM sales.purchases")]["synthetic"] == 1.0
    assert by[("assertion", "SELECT 1 = 2")]["synthetic"] == 0.0


def test_a_conditional_fanout_over_a_column_the_parent_lacks_is_refused(dev):
    status, body = _call(
        dev["boot"],
        "PUT",
        "/admin/synthetic-datasets/bad_condition",
        {
            "seed": 1,
            "scale": 1,
            "tables": [],
            "fanoutConditions": [
                {
                    "relationship": "customer-purchases",
                    "condition": "nope = 1",
                    "count": {"fixed": 1},
                }
            ],
        },
        env=dev["env"],
    )
    assert status == 422, body


def test_the_report_states_how_the_dependence_was_kept(generated):
    """REQ-1939, DEPENDENCE KEPT: the copula's repair (0: as measured), and each pair of numbers'
    measured rank correlation beside the generated rows' own."""
    status, rows = _call(
        generated["boot"], "GET", "/admin/synthetic-datasets/load_test/report", env="dev"
    )
    assert status == 200, rows
    shrinks = [r for r in rows if r["measure"] == "dependence_copula_shrink"]
    assert shrinks and all(0.0 <= r["synthetic"] < 1.0 for r in shrinks), shrinks
    for r in rows:
        if r["measure"] == "dependence_spearman":
            assert r["source"] is not None and -1.0 <= r["source"] <= 1.0, r


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


def test_a_private_dataset_is_measured_under_its_budget(profiled, generated):
    """REQ-1939, DIFFERENTIAL PRIVACY: in an environment of its own."""
    env = _environment(profiled, "private")
    # A budget large enough for 300 rows to say something; the guarantee holds at any ε.
    status, body = _define(env, ["accounts"], dataset="private_test", epsilon=50.0)
    assert status == 200, body
    status, body = _call(
        profiled, "POST", "/admin/synthetic-datasets/private_test/generate", env="private"
    )
    assert status == 200, body
    status, rows = _call(
        profiled, "GET", "/admin/synthetic-datasets/private_test/report", env="private"
    )
    assert status == 200, rows
    by = {r["measure"]: r for r in rows}
    assert by["privacy_epsilon"]["synthetic"] == 50.0
    assert abs(by["privacy_epsilon_charged"]["synthetic"] - 50.0) < 1e-9
    families = [r for r in rows if r["measure"] == "privacy_epsilon_family"]
    assert families and abs(sum(r["synthetic"] for r in families) - 50.0) < 1e-9
    assert "privacy_guarantee" not in by
    status, balances = _sql(profiled, "SELECT MAX(balance) AS m FROM sales.accounts", "private")
    assert status == 200, balances
    assert balances[0]["m"] < 1e6  # the one large balance moved no bound
    status, plain = _call(
        generated["boot"], "GET", "/admin/synthetic-datasets/load_test/report", env="dev"
    )
    assert status == 200, plain
    [none] = [r for r in plain if r["measure"] == "privacy_guarantee"]
    assert none["note"].startswith("none")


def test_generated_rows_keep_their_distance_from_real_rows(profiled):
    """REQ-1939, NOT TOO CLOSE TO A REAL ROW (maintainer rulings W1, Z2, C1), in an environment of
    its own. The distance reads five of a customer's columns (spent, a rule's, is not read). A
    real customer's nearest other differs in its email only: 0.2. A generated one differs from
    every real one at least in its email, its tier (declared values no real row holds) and its
    region (an undeclared column of generated values): 0.6, under 3.5 times 0.2, so every customer
    is dropped, and every purchase with it. An account is drawn again or dropped by its balance
    and date."""
    import duckdb

    boot = profiled
    env = _environment(boot, "close")
    status, body = _define(
        env, ["customers", "purchases", "accounts"], dataset="close_test", closeness=(3.5, 3)
    )
    assert status == 200, body
    status, body = _call(boot, "POST", "/admin/synthetic-datasets/close_test/generate", env="close")
    assert status == 200, (body, boot.log_text()[-6000:])
    status, rows = _call(boot, "GET", "/admin/synthetic-datasets/close_test/report", env="close")
    assert status == 200, rows
    by = {(r["table"], r["measure"]): r for r in rows if r["measure"].startswith("closeness")}
    assert by[("customers", "closeness_dropped")]["synthetic"] == 2 * _CUSTOMERS, by
    assert _one(boot, "SELECT COUNT(*) AS n FROM sales.customers", env="close") == 0
    # C1: a dropped customer's purchases drop with it.
    assert by[("purchases", "closeness")]["note"].startswith("not checked"), by
    assert by[("purchases", "closeness_cascaded")]["synthetic"] > 0, by
    assert _one(boot, "SELECT COUNT(*) AS n FROM sales.purchases", env="close") == 0
    accounts = _one(boot, "SELECT COUNT(*) AS n FROM sales.accounts", env="close")
    redrawn = by[("accounts", "closeness_redrawn")]["synthetic"]
    dropped = by[("accounts", "closeness_dropped")]["synthetic"]
    assert accounts + dropped == 600 and redrawn > 0, by
    threshold = by[("accounts", "closeness_threshold")]
    assert threshold["synthetic"] == pytest.approx(3.5 * threshold["source"])
    distances = [
        r for r in rows if (r["table"], r["measure"]) == ("accounts", "closeness_distance")
    ]
    assert distances and all(r["synthetic"] >= threshold["synthetic"] for r in distances)
    assert 0.0 <= by[("accounts", "closeness_membership_auc")]["synthetic"] <= 1.0
    assert ("accounts", "closeness_nndr") in by
    status, datasets_ = _call(boot, "GET", "/admin/synthetic-datasets", env="close")
    assert status == 200, datasets_
    [close] = [d for d in datasets_ if d["id"] == "close_test"]
    assert (close["closenessThreshold"], close["closenessDraws"]) == (3.5, 3)
    schema = close["storeSchema"]
    # W1: the store's tables of the dataset are read only through the model, by no name of their
    # own -- the real samples were such tables, never registered.
    for role in ("analyst", "org_admin"):
        status, body = _call(
            boot,
            "POST",
            "/data/sql",
            {"sql": f'SELECT * FROM "{schema}"."__closeness__public__accounts"'},
            env="close",
            role=role,
        )
        assert status != 200, body
    # W1: the samples are dropped once generated. The store is read with the server stopped.
    boot.stop()
    try:
        con = duckdb.connect(f"{boot.data_dir}/store.duckdb", read_only=True)
        try:
            left = con.execute(
                "SELECT table_schema, table_name FROM information_schema.tables "
                "WHERE table_name LIKE '%closeness%'"
            ).fetchall()
            generated = con.execute(
                "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = ?", [schema]
            ).fetchone()
        finally:
            con.close()
    finally:
        boot.start()
        boot.wait_all_ready(timeout=300)
    assert left == []
    assert generated is not None and generated[0] == 3


def test_a_dataset_declaring_no_closeness_says_it_was_not_checked(generated):
    status, rows = _call(
        generated["boot"], "GET", "/admin/synthetic-datasets/load_test/report", env="dev"
    )
    assert status == 200, rows
    [entry] = [r for r in rows if r["measure"] == "closeness"]
    assert entry["note"].startswith("not checked"), entry


def test_a_private_dataset_is_not_compared_with_real_rows(profiled):
    env = _environment(profiled, "private_close")
    status, body = _define(
        env, ["accounts"], dataset="private_close", epsilon=1.0, closeness=(1, 2)
    )
    assert status == 422, body
    assert "declare ε or closeness" in body["error"], body


def test_a_test_synthetic_environment_plans_its_whole_model(profiled):
    """REQ-1942: every table not backed by an API is planned from its parent's latest successful
    profile run; one with none (orders, which no profiler covers) keeps Generate refused, by
    name."""
    boot = profiled
    status, body = _call(
        boot,
        "POST",
        f"/admin/orgs/{boot.org_id}/environments",
        {"name": "whole", "data_mode": "test_synthetic"},
    )
    assert status == 200, body
    status, plan = _call(
        boot, "GET", f"/admin/orgs/{boot.org_id}/environments/whole/synthetic/plan"
    )
    assert status == 200, plan
    by = {t["tableName"]: t for t in plan["tables"]}
    assert set(by) == {"customers", "purchases", "contacts", "accounts", "orders"}, by
    assert by["customers"]["selected"] == by["customers"]["runs"][0]["runId"]
    assert by["orders"]["selected"] is None and plan["ready"] is False
    # No API source in this model: nothing unavailable, no command left undefined.
    assert plan["unavailable"] == [] and plan["commandsNotDefined"] == []
    status, body = _call(
        boot, "POST", f"/admin/orgs/{boot.org_id}/environments/whole/synthetic", {}
    )
    assert status == 422 and "orders" in body["error"], body
    status, detail = _call(boot, "GET", f"/admin/orgs/{boot.org_id}/environments/whole/detail")
    assert status == 200 and detail["test_data"]["synthetic"]["status"] is None, detail


def _table_deltas(report: Any) -> list[dict]:
    """Every per-table delta ({table, added, changed, ...}) anywhere in a merge report."""
    if isinstance(report, dict):
        if {"table", "changed"} <= report.keys():
            return [report]
        return [d for v in report.values() for d in _table_deltas(v)]
    if isinstance(report, list):
        return [d for v in report for d in _table_deltas(v)]
    return []


def test_generate_answers_the_warning_and_generates_only_once_it_is_confirmed(profiled):
    """REQ-1942: Generate answers the Limitations of Synthetic Data warning and generates
    nothing; confirming its digest generates the whole model in the background; a digest that is
    not the warning's is refused with the warning as it stands."""
    import time

    boot = profiled
    base = f"/admin/orgs/{boot.org_id}/environments"
    status, body = _call(boot, "POST", base, {"name": "twostep", "data_mode": "test_synthetic"})
    assert status == 200, body
    status, plan = _call(boot, "GET", f"{base}/twostep/synthetic/plan")
    orders = next(t for t in plan["tables"] if t["tableName"] == "orders")
    profile = {
        "rowCount": 12,
        "columns": {
            "id": {"nullShare": 0, "distinctCount": 12, "range": {"min": 1, "max": 12}},
            "region": {"nullShare": 0, "values": [{"value": "mars", "weight": 1}]},
        },
    }
    status, body = _call(
        boot,
        "POST",
        f"/admin/tables/{orders['tableId']}/declared-profiles",
        {"profile": profile},
        env="twostep",
    )
    assert status == 200, body

    choices = {"seed": 5, "scale": 0.5}
    status, before = _call(boot, "GET", f"{base}/twostep/detail")
    real = {s["id"]: (s["type"], s["binding"]) for s in before["sources"]}
    # contacts.phone is sensitive and declares neither a fake nor a rule: nothing generates it,
    # so Generate is not ready, and says which column holds it back.
    status, plan = _call(boot, "GET", f"{base}/twostep/synthetic/plan")
    contacts = next(t for t in plan["tables"] if t["tableName"] == "contacts")
    assert contacts["uncovered"] == ["contacts.phone"] and plan["ready"] is False, plan
    status, body = _call(boot, "POST", f"{base}/twostep/synthetic", choices)
    assert status == 422 and "contacts.phone" in body["error"], body
    engine = sa.create_engine(boot.url, isolation_level="AUTOCOMMIT")
    with engine.connect() as conn:
        done = conn.execute(
            sa.text(
                f"UPDATE \"org_{boot.org_id}_env_twostep\".table_columns SET fake = 'phone_number()' "
                "WHERE column_name = 'phone'"
            )
        )
        assert done.rowcount == 1
    engine.dispose()
    status, body = _call(boot, "POST", f"{base}/twostep/synthetic", choices)
    assert status == 200 and body["status"] == "awaiting_confirmation", body
    shown = body["warning"]
    assert shown["title"] == "Limitations of Synthetic Data" and shown["limitations"], shown
    by = {t["tableName"]: t for t in shown["tables"]}
    assert set(by) == {"customers", "purchases", "contacts", "accounts", "orders"}, by
    assert by["orders"]["profile"]["origin"] == "declared" and by["orders"]["estimatedRows"] == 6
    assert by["customers"]["profile"]["origin"] == "measured"
    assert by["customers"]["estimatedRows"] == _CUSTOMERS // 2
    assert shown["unavailable"] == [] and shown["commandsNotDefined"] == [], shown
    # Nothing is generated by Generate itself.
    status, detail = _call(boot, "GET", f"{base}/twostep/detail")
    assert status == 200 and detail["test_data"]["synthetic"]["status"] is None, detail

    status, body = _call(
        boot, "POST", f"{base}/twostep/synthetic/confirm", {**choices, "digest": "not-it"}
    )
    assert status == 409 and "has changed since its warning was shown" in body["error"], body
    # Other choices than the ones the warning was shown for are not what was confirmed.
    status, body = _call(
        boot,
        "POST",
        f"{base}/twostep/synthetic/confirm",
        {"seed": 5, "scale": 2, "digest": shown["digest"]},
    )
    assert status == 409, body
    status, detail = _call(boot, "GET", f"{base}/twostep/detail")
    assert detail["test_data"]["synthetic"]["status"] is None, detail

    status, body = _call(
        boot, "POST", f"{base}/twostep/synthetic/confirm", {**choices, "digest": shown["digest"]}
    )
    assert status == 200 and body == {
        "status": "generating",
        "tables": 5,
        "warning": shown["digest"],
    }, body
    deadline = time.monotonic() + 300
    while True:
        status, detail = _call(boot, "GET", f"{base}/twostep/detail")
        synthetic = detail["test_data"]["synthetic"]
        if synthetic["status"] != "generating":
            break
        assert time.monotonic() < deadline, ("generation did not finish", boot.log_text()[-8000:])
        time.sleep(1)
    assert synthetic["status"] == "ready", (synthetic, boot.log_text()[-6000:])
    assert _one(boot, "SELECT COUNT(*) AS n FROM sales.orders", env="twostep") == 6
    assert _one(boot, "SELECT COUNT(*) AS n FROM sales.customers", env="twostep") == _CUSTOMERS // 2

    # Generating bound every source to the synthetic store, type and connection.
    status, detail = _call(boot, "GET", f"{base}/twostep/detail")
    becomes = {s["id"]: s["becomes"] for s in shown["sources"]}
    bound = {s["id"]: (s["type"], s["binding"]) for s in detail["sources"]}
    assert becomes and set(bound) == set(real), (shown["sources"], detail)
    for sid, state in bound.items():
        # Each source whose tables were generated; one with no table of the model is left alone.
        assert state == ((becomes[sid], "synthetic") if sid in becomes else real[sid]), detail
    # An environment created from it copies those connections and reads the same generated data.
    status, body = _call(
        boot, "POST", base, {"name": "twochild", "from_env": "twostep", "data_mode": "inherit"}
    )
    assert status == 200, body
    assert _one(boot, "SELECT COUNT(*) AS n FROM sales.orders", env="twochild") == 6
    status, child = _call(boot, "GET", f"{base}/twochild/detail")
    inherited = {s["id"]: (s["type"], s["binding"]) for s in child["sources"]}
    assert all(inherited[sid] == (kind, "synthetic") for sid, kind in becomes.items()), child
    # A merge of the synthetic environment carries the model's type of a source, never the store's.
    status, plan = _call(
        boot,
        "POST",
        f"{base}/prod/merge",
        {"from_env": "twostep", "dry_run": True, "message": "what a merge would carry"},
    )
    assert status == 200, plan
    touched = {t["table"] for t in _table_deltas(plan)}
    assert "table_columns" in touched and "sources" not in touched, plan
    # Leaving Test (synthetic) binds each source to what it was bound to before.
    status, body = _call(
        boot, "PATCH", f"{base}/twostep/data", {"data_mode": "unbound", "confirm_discard": True}
    )
    assert status == 200 and body["change"]["sources_restored"] == sorted(becomes), body
    status, detail = _call(boot, "GET", f"{base}/twostep/detail")
    assert {s["id"]: s["type"] for s in detail["sources"]} == {
        sid: kind for sid, (kind, _) in real.items()
    }, detail
    assert {s["binding"] for s in detail["sources"]} == {"unbound"}, detail


def _generate_whole_model(boot, env: str, choices: dict) -> dict:
    """Generate, confirm the warning, and wait for ``env``'s model to be Ready; its warning."""
    import time

    base = f"/admin/orgs/{boot.org_id}/environments"
    status, body = _call(boot, "POST", f"{base}/{env}/synthetic", choices)
    assert status == 200 and body["status"] == "awaiting_confirmation", body
    shown = body["warning"]
    status, body = _call(
        boot, "POST", f"{base}/{env}/synthetic/confirm", {**choices, "digest": shown["digest"]}
    )
    assert status == 200 and body["status"] == "generating", body
    deadline = time.monotonic() + 300
    while True:
        status, detail = _call(boot, "GET", f"{base}/{env}/detail")
        synthetic = detail["test_data"]["synthetic"]
        if synthetic["status"] != "generating":
            break
        assert time.monotonic() < deadline, ("generation did not finish", boot.log_text()[-8000:])
        time.sleep(1)
    assert synthetic["status"] == "ready", (synthetic, boot.log_text()[-6000:])
    return shown


def test_generating_again_measures_the_parents_rows_never_the_generated_ones(profiled):
    """REQ-1942: what generation measures from a table is measured from its real rows. Once the
    environment's sources are bound to its synthetic store, a regeneration reads them where they
    are still real -- its parent -- never the rows the previous generation wrote."""
    boot = profiled
    base = f"/admin/orgs/{boot.org_id}/environments"
    status, body = _call(boot, "POST", base, {"name": "regen", "data_mode": "test_synthetic"})
    assert status == 200, body
    engine = sa.create_engine(boot.url, isolation_level="AUTOCOMMIT")
    with engine.connect() as conn:
        conn.execute(
            sa.text(
                f'UPDATE "org_{boot.org_id}_env_regen".table_columns '
                "SET fake = 'phone_number()' WHERE column_name = 'phone'"
            )
        )
    status, plan = _call(boot, "GET", f"{base}/regen/synthetic/plan")
    ids = {t["tableName"]: t["tableId"] for t in plan["tables"]}
    whole = {"nullShare": 0, "distinctCount": 12, "range": {"min": 1, "max": 12}}
    orders = {
        "rowCount": 12,
        "columns": {
            "id": whole,
            "region": {"nullShare": 0, "values": [{"value": "mars", "weight": 1}]},
        },
    }
    # accounts.status declares categories((open, closed)) and this profile records nothing of
    # it, so its shares are measured from the table.
    accounts = {
        "rowCount": 300,
        "columns": {
            "id": {"nullShare": 0, "distinctCount": 300, "range": {"min": 1, "max": 300}},
            "balance": {"nullShare": 0, "distinctCount": 300, "range": {"min": 1, "max": 9}},
            "opened": {
                "nullShare": 0,
                "distinctCount": 300,
                "range": {"min": "2024-01-01", "max": "2024-12-01"},
            },
        },
    }
    declared = {}
    for name, profile in (("orders", orders), ("accounts", accounts)):
        status, body = _call(
            boot,
            "POST",
            f"/admin/tables/{ids[name]}/declared-profiles",
            {"profile": profile},
            env="regen",
        )
        assert status == 200, body
        declared[name] = body["runId"]
    choices = {"seed": 9, "scale": 1, "runs": {str(ids["accounts"]): declared["accounts"]}}
    open_rows = "SELECT COUNT(*) AS n FROM sales.accounts WHERE status = 'open'"
    try:
        _generate_whole_model(boot, "regen", choices)
        # The real accounts: open on two rows in three.
        assert 150 < _one(boot, open_rows, env="regen") < 250
        with engine.connect() as conn:
            conn.execute(sa.text("UPDATE public.accounts SET status = 'closed'"))
        _generate_whole_model(boot, "regen", choices)
        # Measured from the parent's rows as they are now; the generated rows were two in three
        # open, and a regeneration that measured them would generate that again.
        assert _one(boot, open_rows, env="regen") == 0
    finally:
        with engine.connect() as conn:
            conn.execute(
                sa.text(
                    "UPDATE public.accounts SET status = "
                    "CASE WHEN id % 3 = 0 THEN 'closed' ELSE 'open' END"
                )
            )
        engine.dispose()


def test_a_declared_profile_generates_a_table_with_no_data_to_profile(profiled):
    """REQ-1942: a declared profile is stored as a profile run is, listed beside the parent's runs,
    and generated from by the one path; a column with no fact, fake or rule is refused by name."""
    boot = profiled
    status, body = _call(
        boot,
        "POST",
        f"/admin/orgs/{boot.org_id}/environments",
        {"name": "declaring", "data_mode": "test_synthetic"},
    )
    assert status == 200, body
    plan_path = f"/admin/orgs/{boot.org_id}/environments/declaring/synthetic/plan"
    status, plan = _call(boot, "GET", plan_path)
    assert status == 200, plan
    orders = next(t for t in plan["tables"] if t["tableName"] == "orders")
    assert orders["selected"] is None, orders
    declare = f"/admin/tables/{orders['tableId']}/declared-profiles"
    status, body = _call(
        boot, "POST", declare, {"profile": {"rowCount": 12, "columns": {}}}, env="declaring"
    )
    assert status == 422 and "orders.region" in body["error"], body
    mars = {"nullShare": 0, "values": [{"value": "mars", "weight": 1}]}
    ids = {"nullShare": 0, "distinctCount": 12, "range": {"min": 1, "max": 12}}
    status, body = _call(
        boot,
        "POST",
        declare,
        {"profile": {"rowCount": 12, "columns": {"id": ids, "region": mars}}},
        env="declaring",
    )
    assert status == 200, body
    run_id = body["runId"]
    status, plan = _call(boot, "GET", plan_path)
    orders = next(t for t in plan["tables"] if t["tableName"] == "orders")
    assert orders["selected"] == run_id and orders["uncovered"] == [], orders
    assert orders["runs"][0]["origin"] == "declared" and orders["runs"][0]["env"] == "declaring"
    status, body = _call(
        boot,
        "PUT",
        "/admin/synthetic-datasets/declared",
        {
            "seed": 1,
            "scale": 1,
            "tables": [{"tableId": orders["tableId"], "profileEnv": "declaring", "runId": run_id}],
        },
        env="declaring",
    )
    assert status == 200, body
    status, body = _call(
        boot, "POST", "/admin/synthetic-datasets/declared/generate", env="declaring"
    )
    assert status == 200, (body, boot.log_text()[-6000:])
    assert _one(boot, "SELECT COUNT(*) AS n FROM sales.orders", env="declaring") == 12
    status, rows = _sql(boot, "SELECT DISTINCT region FROM sales.orders", env="declaring")
    assert status == 200 and rows == [{"region": "mars"}], rows

    # A measured run copied into a declared profile and changed: ten times the customers.
    customers = next(t for t in plan["tables"] if t["tableName"] == "customers")
    measured = next(r for r in customers["runs"] if r["origin"] == "measured")
    status, body = _call(
        boot,
        "GET",
        f"/admin/tables/{customers['tableId']}/profile-runs/{measured['runId']}/declared",
    )
    assert status == 200, body
    profile = body["profile"]
    assert profile["rowCount"] == _CUSTOMERS, profile
    profile["rowCount"] *= 10
    status, body = _call(
        boot,
        "POST",
        f"/admin/tables/{customers['tableId']}/declared-profiles",
        {"profile": profile},
        env="declaring",
    )
    assert status == 200, body
    status, plan = _call(boot, "GET", plan_path)
    customers = next(t for t in plan["tables"] if t["tableName"] == "customers")
    # The parent's measured run stays preselected; the what-if is listed to choose.
    assert customers["selected"] == measured["runId"], customers
    assert body["runId"] in {r["runId"] for r in customers["runs"]}, customers


def test_a_private_dataset_refuses_text_columns_that_declare_nothing(profiled):
    env = _environment(profiled, "private_refused")
    status, body = _define(env, ["customers"], dataset="private_no", epsilon=1.0)
    assert status == 200, body
    status, body = _call(
        profiled, "POST", "/admin/synthetic-datasets/private_no/generate", env="private_refused"
    )
    assert status == 422, body
    assert "customers.email: declare a fake or a synthetic rule" in str(body), body
    assert "customers.region: declare a fake or a synthetic rule" in str(body), body


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
            "data_mode": "inherit",
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


# -- a synthetic environment calls no source API (REQ-1942) -------------------------------------

_PET_SCHEMA = {
    "type": "object",
    "properties": {"id": {"type": "integer"}, "name": {"type": "string"}},
}
_PET_SPEC = {
    "openapi": "3.0.0",
    "info": {"title": "Pets", "version": "1.0.0"},
    "paths": {
        "/pets": {
            "get": {
                "operationId": "listPets",
                "responses": {
                    "200": {
                        "description": "ok",
                        "content": {
                            "application/json": {"schema": {"type": "array", "items": _PET_SCHEMA}}
                        },
                    }
                },
            }
        },
        "/pets/{petId}": {
            "get": {
                "operationId": "getPetById",
                "parameters": [
                    {"name": "petId", "in": "path", "required": True, "schema": {"type": "integer"}}
                ],
                "responses": {
                    "200": {
                        "description": "ok",
                        "content": {"application/json": {"schema": _PET_SCHEMA}},
                    }
                },
            }
        },
    },
}


@pytest.fixture(scope="module")
def pets():
    """A server whose model is one OpenAPI source the test serves: ``list_pets``, read in full,
    and ``get_pet_by_id``, a function of its required path parameter. ``asked``: every path the
    API was asked."""
    import tempfile
    import threading
    from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
    from pathlib import Path

    asked: list[str] = []

    class _Handler(BaseHTTPRequestHandler):
        def do_GET(self):  # noqa: N802 - http.server's handler name
            asked.append(self.path)
            if self.path == "/pets":
                body = json.dumps([{"id": i, "name": f"pet {i}"} for i in range(5)]).encode()
            elif self.path.startswith("/pets/") and self.path[6:].isdigit():
                body = json.dumps({"id": int(self.path[6:]), "name": "pet"}).encode()
            else:
                self.send_response(400)
                self.end_headers()
                return
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, format, *args):  # noqa: A002 - http.server's name
            pass

    api = ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
    threading.Thread(target=api.serve_forever, daemon=True).start()
    workdir = tempfile.TemporaryDirectory()
    spec = Path(workdir.name) / "spec.json"
    spec.write_text(json.dumps(_PET_SPEC))
    pg_host = os.environ.get("PG_HOST", "localhost")
    pg_port = int(os.environ.get("PG_PORT", "5432"))
    boot = WorkerBoot(
        1, pg_host=pg_host, pg_port=pg_port, env={"PROVISA_REDIRECT_ENABLED": "false"}
    )
    visible = {"visible_to": _ROLES}

    def table(name: str, *extra: dict) -> dict:
        return {
            "source_id": "petstore",
            "domain_id": "sales",
            "schema": "default",
            "table": name,
            "columns": [
                {"name": "id", "data_type": "integer", **visible},
                {"name": "name", "data_type": "varchar", **visible},
                *extra,
            ],
        }

    boot._extra_config = {
        "sources": [
            {
                "id": "petstore",
                "type": "openapi",
                "path": str(spec),
                "base_url": f"http://127.0.0.1:{api.server_address[1]}",
                "cache_ttl": 3600,
            }
        ],
        "tables": [
            table("list_pets"),
            table(
                "get_pet_by_id",
                {
                    "name": "petId",
                    "data_type": "integer",
                    "native_filter_type": "path_param",
                    **visible,
                },
            ),
        ],
    }
    boot.create_database()
    try:
        boot.start()
        boot.wait_all_ready(timeout=300)
        yield {"boot": boot, "asked": asked}
    finally:
        boot.cleanup()
        api.shutdown()
        workdir.cleanup()


def test_a_synthetic_environment_generates_api_tables_and_calls_no_api(pets):
    """REQ-1942: an API table read in full is generated like any table; one that needs a required
    parameter is not available, a read of it refused saying why, until a declared profile
    generates it -- its parameter then a column of it, a filter like any other; and nothing in
    the environment calls the API."""
    boot, asked = pets["boot"], pets["asked"]
    base = f"/admin/orgs/{boot.org_id}/environments"
    status, body = _call(boot, "POST", base, {"name": "apisyn", "data_mode": "test_synthetic"})
    assert status == 200, body
    status, plan = _call(boot, "GET", f"{base}/apisyn/synthetic/plan")
    assert status == 200, plan
    (listed,) = plan["tables"]
    assert listed["tableName"] == "list_pets" and listed["api"] is True, plan
    assert listed["requiredParameters"] == [] and plan["ready"] is False, plan
    (by_id,) = plan["unavailable"]
    assert by_id["tableName"] == "get_pet_by_id" and by_id["requiredParameters"] == ["petId"]
    assert "Declare a profile of it" in by_id["reason"], by_id
    assert plan["sources"][0]["id"] == "petstore" and plan["sources"][0]["type"] == "openapi"

    def declare(table_id: int, rows: int, **columns: dict) -> None:
        facts = {
            "id": {"nullShare": 0, "distinctCount": rows, "range": {"min": 1, "max": rows}},
            "name": {
                "nullShare": 0,
                "distinctCount": rows,
                "shapes": [{"shape": "aaa 9", "weight": 1}],
            },
            **columns,
        }
        status, body = _call(
            boot,
            "POST",
            f"/admin/tables/{table_id}/declared-profiles",
            {"profile": {"rowCount": rows, "columns": facts}},
            env="apisyn",
        )
        assert status == 200, body

    declare(listed["tableId"], 10)
    shown = _generate_whole_model(boot, "apisyn", {"seed": 2, "scale": 1})
    assert [t["tableName"] for t in shown["unavailable"]] == ["get_pet_by_id"], shown
    assert shown["sources"] == plan["sources"], shown
    status, detail = _call(boot, "GET", f"{base}/apisyn/detail")
    (source,) = [s for s in detail["sources"] if s["id"] == "petstore"]
    assert (source["type"], source["binding"]) == (plan["sources"][0]["becomes"], "synthetic")

    called = len(asked)
    assert _one(boot, "SELECT COUNT(*) AS n FROM sales.list_pets", env="apisyn") == 10
    status, body = _sql(boot, 'SELECT * FROM sales.get_pet_by_id WHERE "petId" = 3', env="apisyn")
    assert status != 200, body
    assert "is not available in a Test (synthetic) environment" in str(body), body
    assert "Declare a profile of it" in str(body), body
    assert len(asked) == called, asked[called:]

    # Declared, its parameter among its columns, it is generated: an ordinary table.
    declare(
        by_id["tableId"],
        20,
        petId={"nullShare": 0, "distinctCount": 5, "range": {"min": 1, "max": 5}},
    )
    status, plan = _call(boot, "GET", f"{base}/apisyn/synthetic/plan")
    assert plan["unavailable"] == [] and plan["ready"] is True, plan
    assert {t["tableName"]: t["requiredParameters"] for t in plan["tables"]} == {
        "list_pets": [],
        "get_pet_by_id": ["petId"],
    }, plan
    _generate_whole_model(boot, "apisyn", {"seed": 2, "scale": 1})
    called = len(asked)
    assert _one(boot, "SELECT COUNT(*) AS n FROM sales.get_pet_by_id", env="apisyn") == 20
    status, rows = _sql(
        boot,
        'SELECT COUNT(*) AS n, MIN("petId") AS lo, MAX("petId") AS hi FROM sales.get_pet_by_id '
        'WHERE "petId" BETWEEN 2 AND 4',
        env="apisyn",
    )
    assert status == 200 and 0 < rows[0]["n"] < 20, rows
    assert rows[0]["lo"] >= 2 and rows[0]["hi"] <= 4, rows
    assert len(asked) == called, asked[called:]
