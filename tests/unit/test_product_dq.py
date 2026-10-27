# Copyright (c) 2026 Kenneth Stott
# Canary: 7c4e1a96-3b2d-4f85-a0e9-5d8b2c6f1e37
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2157: a data product's data quality -- expectations, promises, related and unattached.

Whether a check is an expectation or a promise follows from what it checks (an input or a member);
whether it is the product's follows from ownership (the checker table's product). A check another
product owns, or none, on the product's member or input is related. A check the product owns on
neither is unattached: a curation gap its owner sees. A check whose checked columns the reader
cannot read is absent from every list (REQ-2152).
"""

# Requirements: REQ-2157, REQ-2152

from __future__ import annotations

from provisa.lineage.product import product_dq

MEMBER, INPUT, ELSEWHERE = 1, 2, 3


def _checker(cid: int, checked: int, owner: str | None, columns: tuple[str, ...] = ("id",)):
    return {
        "id": cid,
        "domain_id": "sales",
        "table_name": f"check_{cid}",
        "alias": None,
        "description": f"Claim {cid}",
        "product_id": owner,
        "checked_table_id": checked,
        "checked_table": {
            "id": checked,
            "domain_id": "sales",
            "table_name": f"t{checked}",
            "alias": None,
        },
        "rule_columns": list(columns),
        "latest_outcome": "pass",
        "latest_run_at": None,
    }


CHECKERS = [
    _checker(10, MEMBER, "p1"),
    _checker(11, INPUT, "p1"),
    _checker(12, INPUT, "p2"),
    _checker(13, MEMBER, None),
    _checker(14, ELSEWHERE, "p1"),
    _checker(15, ELSEWHERE, "p2"),
]
# The reader is served every checker table and every column of the checked tables.
ALL = {cid: {"ok"} for cid in range(10, 16)} | {t: {"id", "email"} for t in (1, 2, 3)}


def _ids(out: dict) -> dict[str, list[int]]:
    return {k: [e["checker_table_id"] for e in v] for k, v in out.items()}


def test_each_check_is_classified_by_what_it_checks_and_who_owns_it() -> None:
    out = product_dq("p1", CHECKERS, members={MEMBER}, inputs={INPUT}, served=ALL)
    assert _ids(out) == {
        "expectations": [11],
        "promises": [10],
        "related": [12, 13],
        "unattached": [14],
    }


def test_an_entry_names_its_checker_owner_and_latest_run() -> None:
    (promise,) = product_dq("p1", CHECKERS, members={MEMBER}, inputs={INPUT}, served=ALL)[
        "promises"
    ]
    assert promise == {
        "checker_table_id": 10,
        "domain_id": "sales",
        "table_name": "check_10",
        "alias": None,
        "description": "Claim 10",
        "owner_product_id": "p1",
        "checked_table_id": MEMBER,
        "checked_table": {"id": MEMBER, "domain_id": "sales", "table_name": "t1", "alias": None},
        "latest_outcome": "pass",
        "latest_run_at": None,
    }


def test_another_products_promise_on_an_input_is_related() -> None:
    # For p2, table 2 is its member: p2's own check on it is its promise, p1's is related to it.
    out = product_dq("p2", CHECKERS, members={INPUT}, inputs=set(), served=ALL)
    assert _ids(out) == {
        "expectations": [],
        "promises": [12],
        "related": [11],
        "unattached": [15],
    }


def test_a_check_on_a_column_the_reader_cannot_read_is_in_no_list() -> None:
    checkers = [_checker(10, MEMBER, "p1", ("email",)), _checker(11, INPUT, "p1", ("id",))]
    served = ALL | {MEMBER: {"id"}}
    out = product_dq("p1", checkers, members={MEMBER}, inputs={INPUT}, served=served)
    assert all(10 not in ids for ids in _ids(out).values())
    assert _ids(out)["expectations"] == [11]


def test_a_check_naming_no_column_needs_one_readable_column_of_its_table() -> None:
    checkers = [_checker(10, MEMBER, "p1", ())]
    shown = product_dq("p1", checkers, members={MEMBER}, inputs=set(), served=ALL)
    assert _ids(shown)["promises"] == [10]
    hidden = product_dq(
        "p1", checkers, members={MEMBER}, inputs=set(), served=ALL | {MEMBER: set()}
    )
    assert _ids(hidden)["promises"] == []


def test_a_checker_table_the_reader_cannot_read_is_in_no_list() -> None:
    served = {k: v for k, v in ALL.items() if k != 10}
    out = product_dq("p1", CHECKERS, members={MEMBER}, inputs={INPUT}, served=served)
    assert all(10 not in ids for ids in _ids(out).values())
