# Copyright (c) 2026 Kenneth Stott
# Canary: d69b90d6-a893-4b7a-8955-551b257687fc
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The customer360 demo's data holds the story its tour tells (REQ-2034).

The rows it declares for Salesforce, ServiceNow, Jira and Stripe and its hub's db/init.sql are what
generate.py writes, for its AS_OF and for any other day. Loaded into DuckDB, with each view of its
config.yaml run as its own SQL in the order its views read each other: the answer through the hub
leaves out Larkspur Analytics, whose overdue invoice is on a Stripe customer the hub gives to Larkspur
Labs; the check finds that wrong merge, the duplicate customers, the coverage gaps and the stale
links planted beside it; and the answer through the corrected hub is the hub's answer plus Larkspur
Analytics."""

# Requirements: REQ-2034

from __future__ import annotations

import importlib.util
import re
import sys
from datetime import date
from pathlib import Path
from types import ModuleType

import duckdb
import pytest
import sqlglot
import yaml

_DEMO = Path(__file__).resolve().parents[2] / "demo" / "named" / "customer360"
_CONFIG = yaml.safe_load((_DEMO / "config.yaml").read_text())
#: Each view, in an order every view it reads comes before it.
_VIEW_ORDER = (
    "renewal_risk_hub",
    "system_records",
    "crosswalk",
    "master",
    "hub_findings",
    "hub_match_rate",
    "hub_corrected",
    "renewal_risk_checked",
)


def _generator() -> ModuleType:
    spec = importlib.util.spec_from_file_location("customer360_generate", _DEMO / "generate.py")
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


GEN = _generator()


def _relation_name(table: dict) -> str:
    return table.get("alias") or table["table"]


def _load(con: duckdb.DuckDBPyConnection, root: Path) -> None:
    """Every declared table and the hub's tables, under the names the views read them by."""
    files = {
        ("salesforce", "Account"): "salesforce/salesforce.Account.tsv",
        ("salesforce", "Opportunity"): "salesforce/salesforce.Opportunity.tsv",
        ("servicenow", "core_company"): "servicenow/servicenow.core_company.tsv",
        ("servicenow", "incident"): "servicenow/servicenow.incident.tsv",
        ("jira", "bug"): "jira/jira.bug.tsv",
        ("stripe", "get_customers"): "stripe/default.get_customers.tsv",
        ("stripe", "get_invoices"): "stripe/default.get_invoices.tsv",
    }
    types = {
        "varchar": "VARCHAR",
        "integer": "INTEGER",
        "bigint": "BIGINT",
        "double": "DOUBLE",
        "boolean": "BOOLEAN",
        "date": "DATE",
        "timestamp": "TIMESTAMP",
    }
    for table in _CONFIG["tables"]:
        if table["source_id"] == "hub":
            continue
        path = root / "replica_rows" / files[(table["source_id"], table["table"])]
        lines = path.read_text().rstrip("\n").split("\n")
        assert lines[0].split("\t") == [c["name"] for c in table["columns"]], path
        cols = ", ".join(f'"{c["name"]}" {types[c["data_type"]]}' for c in table["columns"])
        con.execute(f'CREATE TABLE "{_relation_name(table)}" ({cols})')
        rows = [
            [None if cell == "\\N" else cell for cell in line.split("\t")] for line in lines[1:]
        ]
        marks = ", ".join("?" for _ in table["columns"])
        con.executemany(f'INSERT INTO "{_relation_name(table)}" VALUES ({marks})', rows)
    hub = (root / "db" / "init.sql").read_text()
    con.execute(hub.replace("mdm.", "").replace("CREATE SCHEMA mdm;", ""))


def _duckdb_sql(sql: str) -> str:
    named = re.sub(r'"customer360"\."(view_)?([A-Za-z_]+)"', r'"\2"', sql)
    return sqlglot.transpile(named, read="postgres", write="duckdb")[0]


def _views(root: Path) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    _load(con, root)
    views = {v["id"]: v for v in _CONFIG["views"]}
    assert set(views) == set(_VIEW_ORDER)
    for view_id in _VIEW_ORDER:
        con.execute(f'CREATE TABLE "{view_id}" AS {_duckdb_sql(views[view_id]["sql"])}')
        declared = [c["name"] for c in views[view_id]["columns"]]
        assert [
            d[0] for d in con.execute(f'SELECT * FROM "{view_id}" LIMIT 0').description
        ] == declared
    return con


def _rows(con: duckdb.DuckDBPyConnection, sql: str) -> list[tuple]:
    return con.execute(sql).fetchall()


@pytest.fixture(scope="module")
def shipped() -> duckdb.DuckDBPyConnection:
    return _views(_DEMO)


def test_the_shipped_rows_are_what_the_generator_writes(tmp_path):
    GEN.write(tmp_path, GEN.AS_OF)
    shipped = sorted(p.relative_to(_DEMO) for p in (_DEMO / "replica_rows").rglob("*.tsv"))
    written = sorted(p.relative_to(tmp_path) for p in (tmp_path / "replica_rows").rglob("*.tsv"))
    assert shipped == written
    for rel in [*written, Path("db/init.sql")]:
        assert (tmp_path / rel).read_text() == (_DEMO / rel).read_text(), rel


def test_the_hub_answer_leaves_out_larkspur_analytics(shipped):
    hub = {r[0] for r in _rows(shipped, "SELECT account FROM renewal_risk_hub")}
    assert hub == {
        "Cobalt Freight",
        "Ferncliff Retail",
        "Granite Ridge Bank",
        "Ironbark Energy",
        "Lumen Dental Group",
        "Pinecrest Schools",
        "Vantage Payroll",
    }


def test_the_corrected_answer_adds_larkspur_analytics_and_its_overdue_invoice(shipped):
    checked = _rows(
        shipped, "SELECT account, overdue_invoices, missed_by_hub FROM renewal_risk_checked"
    )
    hub = {r[0] for r in _rows(shipped, "SELECT account FROM renewal_risk_hub")}
    assert {r[0] for r in checked} == hub | {"Larkspur Analytics"}
    assert [r for r in checked if r[2]] == [("Larkspur Analytics", 1, True)]


def test_the_check_finds_what_was_planted(shipped):
    found = _rows(
        shipped,
        "SELECT finding, system, COALESCE(record_name, ''), COALESCE(hub_customer, ''),"
        " COALESCE(evidence_customer, '') FROM hub_findings ORDER BY 1, 2, 3",
    )
    assert found == [
        ("coverage gap", "jira", "Xylo Robotics", "", "Xylo Robotics"),
        ("coverage gap", "servicenow", "Alder and Finch", "", "Alder & Finch"),
        ("coverage gap", "servicenow", "Tamarack Mining Corp", "", "Tamarack Mining"),
        ("coverage gap", "stripe", "Bramble Software Inc", "", "Bramble Software"),
        ("coverage gap", "stripe", "Yarrow Insurance Brokers Ltd", "", "Yarrow Insurance Brokers"),
        (
            "duplicate customer",
            "jira",
            "Willow Creek Clinics",
            "Willow Creek Clinics (support)",
            "Willow Creek Clinics",
        ),
        (
            "duplicate customer",
            "servicenow",
            "Brightwater Health, Inc.",
            "Brightwater Health, Inc.",
            "Brightwater Health",
        ),
        ("stale link", "servicenow", "", "Tamarack Mining", ""),
        ("stale link", "stripe", "", "Yarrow Insurance Brokers", ""),
        ("wrong merge", "stripe", "LARKSPUR ANALYTICS INC", "Larkspur Labs", "Larkspur Analytics"),
        ("wrong merge", "stripe", "Larkspur Labs", "Larkspur Analytics", "Larkspur Labs"),
    ]


def test_each_crosswalk_row_names_its_rule_and_every_rule_is_used(shipped):
    rules = dict(_rows(shipped, "SELECT rule, COUNT(*) FROM crosswalk GROUP BY rule"))
    assert set(rules) == {
        "anchor account",
        "website domain",
        "email domain",
        "normalised name",
        "no match",
    }
    # The personal mailbox is matched by name; the second Quillon account by its website's domain.
    assert _rows(
        shipped, "SELECT rule FROM crosswalk WHERE record_name = 'Saltmarsh Brewing Co'"
    ) == [("normalised name",)]
    assert _rows(
        shipped, "SELECT rule FROM crosswalk WHERE record_name = 'Quillon Systems Inc.'"
    ) == [("website domain",)]


def test_master_ids_are_derived_from_the_anchor_and_one_a_customer(shipped):
    masters = _rows(shipped, "SELECT master_id, anchor_account_id, name FROM master")
    assert len(masters) == len(GEN.COMPANIES)
    assert len({m[0] for m in masters}) == len(masters)
    assert all(m[0].startswith("C360-") and len(m[0]) == 13 for m in masters)
    quillon = [m for m in masters if m[2] == "Quillon Systems"]
    assert len(quillon) == 1


def test_the_match_rate_falls_with_the_bulk_import(shipped):
    trend = _rows(shipped, "SELECT month, links, match_rate_pct FROM hub_match_rate ORDER BY month")
    assert len(trend) >= 6
    rates = [r[2] for r in trend]
    bulk = GEN.add_months(GEN.AS_OF, -3)
    before = [r[2] for r in trend if r[0] < bulk]
    after = [r[2] for r in trend if r[0] >= bulk]
    assert min(after) < min(before), rates
    assert all(0 < r < 100 for r in rates)


@pytest.mark.parametrize("as_of", [date(2027, 2, 14), date(2027, 6, 30), date(2028, 1, 1)])
def test_any_day_tells_the_same_story(tmp_path, as_of):
    GEN.write(tmp_path, as_of)
    con = _views(tmp_path)
    hub = {r[0] for r in _rows(con, "SELECT account FROM renewal_risk_hub")}
    missed = _rows(con, "SELECT account FROM renewal_risk_checked WHERE missed_by_hub")
    assert len(hub) == 7 and missed == [("Larkspur Analytics",)]


def test_data_product_tables_use_the_registered_view_names():
    """Every name in a data product's tables list must match the name the config registers the
    table under — alias when set, otherwise table_name (REQ-2034).

    ``views_as_tables`` registers a view declared as ``id: master`` under the name
    ``view_master``, not ``master``. A data product listing ``master`` silently passes config
    parsing but fails at startup when ``set_product_members`` asks the registry for it.

    Works at the raw-dict level (after ``views_as_tables`` but before Pydantic) so that
    ``${env:CUSTOMER360_PG_NS}`` placeholders without defaults, which cannot be resolved
    in a unit-test environment, do not cause an error.
    """
    from provisa.core.config_loader import views_as_tables

    raw = yaml.safe_load((_DEMO / "config.yaml").read_text())
    expanded = views_as_tables(raw)
    # Virtual table name: alias when set, otherwise table (the field views_as_tables writes).
    declared = frozenset(
        t.get("alias") or t["table"] for t in expanded.get("tables", []) if isinstance(t, dict)
    )
    for dp in expanded.get("data_products", []):
        for name in dp.get("tables", []):
            assert name in declared, (
                f"data product {dp['id']!r} lists table {name!r}, "
                "which is not in the config's declared table names; "
                f"declared={sorted(declared)}"
            )
