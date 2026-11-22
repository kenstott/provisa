# Copyright (c) 2026 Kenneth Stott
# Canary: 3b5abc60-5c5a-4c86-9f93-4129e5168e1e
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2035: the named demo ``retail``'s rules, read from its config.yaml and run. Its tables are
built from the columns the config declares and a few rows of each subsidiary; its views' own SQL
then answers: group reporting sees EU customers only by pseudonym, EU marketing only those who
agreed, one query finds a person across both subsidiaries, and no message text reaches the review
queue. Where each table is homed, who reads it and the sender's ordered mask list are the model's
statements of the same rules, checked through the resolver the compiler uses."""

# Requirements: REQ-2035

from __future__ import annotations

import hashlib
from pathlib import Path

import duckdb
import pytest
import sqlglot
import yaml
from sqlglot import exp

from provisa.security.masking import MaskType, resolve

_DEMO = Path(__file__).resolve().parents[2] / "demo" / "named" / "retail"
_CONFIG = yaml.safe_load((_DEMO / "config.yaml").read_text())
_VIEWS = {v["id"]: v for v in _CONFIG["views"]}
_SECRET_BODY = "attached: every customer of the Berlin store"
_SECRET_SUBJECT = "the list you asked for"


def _name(table: dict) -> str:
    return table.get("alias") or table["table"]


_ROWS: dict[str, list[tuple]] = {
    "eu_customers": [
        (1, "anna@mail.example", "Anna Weber", "DE", "2022-01-02", True, "2023-05-01 00:00:00"),
        (2, "bruno@mail.example", "Bruno Conti", "IT", "2022-03-04", False, None),
    ],
    "eu_orders": [
        (10, 1, 5, 7, "2024-01-01 09:00:00", "40.00"),
        (11, 1, 5, 7, "2024-02-01 09:00:00", "2.50"),
        (12, 2, 5, 7, "2024-03-01 09:00:00", "9.00"),
    ],
    "eu_staff": [
        (3, "Clara Roth", "clara@group.example", 5, "crm_analyst", True),
    ],
    "eu_outbound_mail": [
        (
            30,
            3,
            "clara.private@gmail.com",
            "2024-04-01 12:00:00",
            _SECRET_SUBJECT,
            _SECRET_BODY,
            1500,
        ),
        (31, 3, "buyer@supplier.example", "2024-04-02 12:00:00", "invoice", "see attached", 0),
    ],
    "us_customers": [
        (7, "anna@mail.example", "Anna Weber", "US", "2023-06-07", True, "2023-06-07 00:00:00"),
        (8, "dan@mail.example", "Dan Reed", "US", "2023-08-09", False, None),
    ],
    "us_orders": [
        (20, 7, 6, 7, "2024-05-01 09:00:00", "15.00"),
    ],
    "products": [],
    "stores": [],
    "subject_requests": [
        (100, "anna@mail.example", "access", "2024-06-01 00:00:00"),
        (101, "bruno@mail.example", "erasure", "2024-06-02 00:00:00"),
    ],
}


@pytest.fixture(scope="module")
def retail():
    """The group's tables, as config.yaml declares them, and its views, in the config's order."""
    con = duckdb.connect(":memory:")
    con.execute("CREATE SCHEMA retail")
    for table in _CONFIG["tables"]:
        name = _name(table)
        columns = ", ".join(f'"{c["name"]}" {c["data_type"]}' for c in table["columns"])
        con.execute(f'CREATE TABLE retail."{name}" ({columns})')
        if _ROWS[name]:
            marks = ", ".join("?" for _ in table["columns"])
            con.executemany(f'INSERT INTO retail."{name}" VALUES ({marks})', _ROWS[name])
    for view in _CONFIG["views"]:
        con.execute(f'CREATE VIEW retail."view_{view["id"]}" AS {view["sql"]}')
    yield con
    con.close()


def _read(con, view: str) -> list[dict]:
    cursor = con.execute(f'SELECT * FROM retail."view_{view}"')
    names = [d[0] for d in cursor.description]
    return [dict(zip(names, row)) for row in cursor.fetchall()]


def _md5(text: str) -> str:
    return hashlib.md5(text.encode()).hexdigest()  # noqa: S324 -- the demo's pseudonym function


def test_each_subsidiary_s_tables_are_homed_in_its_own_region():
    expected = {"eu-shop": "eu", "us-shop": "us", "group-ref": None}
    tables = _CONFIG["tables"]
    assert {t["source_id"] for t in tables} == set(expected)
    for table in tables:
        assert table.get("region") == expected[table["source_id"]], _name(table)


def test_an_eu_row_is_read_only_from_a_replica_landed_once():
    eu_shop = next(s for s in _CONFIG["sources"] if s["id"] == "eu-shop")
    assert eu_shop["replicate"] == 0
    assert eu_shop["cache_ttl"] == -1


def test_each_region_keeps_its_rows_in_stores_of_its_own():
    regions = {r["id"]: r for r in _CONFIG["regions"]}
    assert set(regions) == {"eu", "us"}
    for role in ("engine", "replicas", "views", "state", "record"):
        assert regions["eu"][role] != regions["us"][role], role


def test_the_views_named_for_eu_are_built_in_eu():
    for name in ("eu_marketing_audience", "eu_customers_pseudonymised", "eu_mail_review"):
        assert _VIEWS[name]["region"] == "eu" and _VIEWS[name]["materialize"] is True, name


def test_the_group_analyst_is_granted_no_column_of_an_eu_homed_table():
    for table in _CONFIG["tables"]:
        if table.get("region") == "eu":
            for column in table["columns"]:
                assert "group_analyst" not in column["visible_to"], (_name(table), column["name"])


def test_group_reporting_reads_eu_customers_only_through_the_eu_pseudonyms():
    read = {t.name for t in sqlglot.parse_one(_VIEWS["group_customers"]["sql"]).find_all(exp.Table)}
    assert read == {"us_customers", "view_eu_customers_pseudonymised"}


def test_group_reporting_shows_an_eu_customer_by_pseudonym_and_a_us_customer_as_they_are(retail):
    rows = _read(retail, "group_customers")
    assert {r["customer"] for r in rows if r["subsidiary"] == "eu"} == {
        _md5("eu-customer:1"),
        _md5("eu-customer:2"),
    }
    assert {r["customer"] for r in rows if r["subsidiary"] == "us"} == {"7", "8"}
    assert set(rows[0]) == {"subsidiary", "customer", "country", "joined_on"}


def test_the_pseudonymised_view_keeps_no_name_or_address_and_sums_what_each_spent(retail):
    rows = {r["customer_pseudonym"]: r for r in _read(retail, "eu_customers_pseudonymised")}
    anna = rows[_md5("eu-customer:1")]
    assert anna["orders"] == 2 and float(anna["spent"]) == pytest.approx(42.5, rel=1e-14)
    assert not {"email", "full_name", "customer_id"} & set(anna)


def test_eu_marketing_reads_only_the_customers_who_agreed(retail):
    assert [r["customer_id"] for r in _read(retail, "eu_marketing_audience")] == [1]


def test_one_query_finds_everything_held_on_a_person_across_both_subsidiaries(retail):
    rows = _read(retail, "subject_access")
    assert sorted((r["subsidiary"], r["customer_id"]) for r in rows) == [("eu", 1), ("us", 7)]
    assert {r["request_id"] for r in rows} == {100}


def test_the_erasure_view_names_every_row_to_delete_for_the_request(retail):
    planned = {
        (r["subsidiary"], r["table_name"], r["row_key"]) for r in _read(retail, "erasure_plan")
    }
    assert planned == {("eu", "customers", 2), ("eu", "orders", 12)}


def test_the_review_queue_carries_the_score_and_signals_and_no_message_text(retail):
    by_id = {r["message_id"]: r for r in _read(retail, "eu_mail_review")}
    flagged = by_id[30]
    assert flagged["score"] == 100
    assert flagged["signals"] == (
        "personal address, customer records attached, bulk export, sender reads customer data"
    )
    assert flagged["sender_pseudonym"] == _md5("eu-staff:3")
    assert by_id[31]["score"] == 10
    assert not {"body", "subject"} & set(flagged)
    assert not any(_SECRET_BODY in str(v) or _SECRET_SUBJECT in str(v) for v in flagged.values())


def _sender_masks() -> list[dict]:
    columns = _VIEWS["eu_mail_review"]["columns"]
    return next(c for c in columns if c["name"] == "sender_email")["masks"]


def test_the_sender_is_revealed_to_a_reviewer_acting_with_the_eu_region():
    assert resolve(_sender_masks(), ["reviewer", "analyst", "region__eu"], "varchar") is None


@pytest.mark.parametrize(
    "chain",
    [["reviewer", "analyst"], ["reviewer", "analyst", "region__us"], ["analyst", "region__eu"]],
    ids=["no region", "us reviewer", "eu but not a reviewer"],
)
def test_the_sender_is_withheld_from_everyone_else(chain):
    rule = resolve(_sender_masks(), chain, "varchar")
    assert rule is not None
    assert rule.mask_type == MaskType.constant and rule.value == "withheld"


def test_no_source_reaches_outside_the_demo_s_own_containers():
    for source in _CONFIG["sources"]:
        assert source["host"] == "${env:RETAIL_PG_HOST:-localhost}", source["id"]
