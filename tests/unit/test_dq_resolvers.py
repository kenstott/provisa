# Copyright (c) 2026 Kenneth Stott
# Canary: 6b0a3d92-14ce-4f77-9a28-3d5e0c81b6af
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1443 clause 7: the contract builder panel's parse/serialize pair.

The raw editor is the contract's source of truth, so what these prove is that the panel is a VIEW of
that text: text parses to rows, rows serialize back to text, and the checks survive the trip with
their bodies intact. A parse failure has to arrive as a result rather than an exception, because the
operator is typing into that editor and half-written YAML is its normal state.
"""

from __future__ import annotations

import json
from types import SimpleNamespace
from typing import TYPE_CHECKING, cast

import pytest
import yaml

from provisa.api.admin._dq_resolvers import (
    build_check,
    check_catalog_for,
    parse_contract,
    run_dq_check_now,
    serialize_contract,
)

from tests.helpers import dq_contexts

if TYPE_CHECKING:
    from provisa.core.database import Connection

SODA = """
dataset: provisa/sales/orders
columns:
  - name: customer
    checks:
      - missing:
      - invalid:
          valid_values: [gold, silver]
checks:
  - row_count:
      must_be_greater_than: 0
"""

GX = """
{
  "meta": {"dataset": "provisa/sales/orders"},
  "expectations": [
    {"type": "expect_column_values_to_not_be_null", "kwargs": {"column": "customer"}}
  ]
}
"""


def test_a_soda_contract_parses_into_its_checks():
    parsed = parse_contract("soda", SODA)
    assert parsed["error"] is None
    assert parsed["dataset"] == "provisa/sales/orders"
    assert [(c["column_name"], c["check_type"]) for c in parsed["checks"]] == [
        ("", "row_count"),
        ("customer", "missing"),
        ("customer", "invalid"),
    ]


def test_a_gx_suite_parses_into_its_expectations():
    parsed = parse_contract("great_expectations", GX)
    assert parsed["error"] is None
    assert parsed["dataset"] == "provisa/sales/orders"
    assert [(c["column_name"], c["check_type"]) for c in parsed["checks"]] == [
        ("customer", "expect_column_values_to_not_be_null")
    ]


def test_unparseable_text_comes_back_as_an_error_not_an_exception():
    """The panel keeps the operator's text and shows the message beside it — replacing a
    half-written contract with an empty builder would discard the edit in progress."""
    parsed = parse_contract("soda", "dataset: [unclosed\n")
    assert parsed["checks"] == []
    assert parsed["dataset"] is None
    assert "not parseable" in parsed["error"]


def test_text_that_names_no_dataset_comes_back_as_an_error():
    parsed = parse_contract("soda", "columns: []\n")
    assert parsed["dataset"] is None
    assert "names no dataset" in parsed["error"]


def test_an_unknown_checker_comes_back_as_an_error():
    parsed = parse_contract("deequ", SODA)
    assert "unknown data-quality checker" in parsed["error"]


def test_parsed_soda_checks_serialize_back_to_an_equivalent_contract():
    """Round trip through the panel: a pasted contract that is edited and saved must still carry the
    thresholds the builder has no editor for, which is why ``definition`` is the check's own text."""
    parsed = parse_contract("soda", SODA)
    built = serialize_contract("soda", parsed["dataset"], parsed["checks"])
    assert built["error"] is None
    assert parse_contract("soda", built["text"])["checks"] == parsed["checks"]


def test_parsed_gx_expectations_serialize_back_to_an_equivalent_suite():
    parsed = parse_contract("great_expectations", GX)
    built = serialize_contract("great_expectations", parsed["dataset"], parsed["checks"])
    assert built["error"] is None
    assert parse_contract("great_expectations", built["text"])["checks"] == parsed["checks"]


def test_serializing_against_a_two_part_dataset_comes_back_as_an_error():
    built = serialize_contract("soda", "sales/orders", [])
    assert built["text"] == ""
    assert "must have exactly 3" in built["error"]


def test_a_check_with_no_args_serializes_to_sodas_bodiless_form():
    """``- missing:`` takes no arguments, so an empty args cell is the check as authored, not a
    half-written one — the type the row already names is what soda needs (REQ-1443 clause 7)."""
    built = serialize_contract(
        "soda",
        "provisa/sales/orders",
        [{"column_name": "customer", "check_type": "missing", "definition": ""}],
    )
    assert built["error"] is None
    assert yaml.safe_load(built["text"])["columns"] == [
        {"name": "customer", "checks": [{"missing": None}]}
    ]


def test_serializing_a_check_with_no_type_comes_back_as_an_error():
    """The type is the dialect's envelope and the row's own identity — a row without one cannot be
    wrapped into any check at all."""
    built = serialize_contract(
        "soda",
        "provisa/sales/orders",
        [{"column_name": "customer", "check_type": "", "definition": "threshold: {must_be: 0}"}],
    )
    assert built["text"] == ""
    assert "no check_type" in built["error"]


class _Rows:
    """One result set, shaped the way SQLAlchemy hands rows back."""

    def __init__(self, mappings: list[dict]):
        self._rows = [SimpleNamespace(_mapping=m) for m in mappings]

    def fetchall(self):
        return self._rows

    def fetchone(self):
        return self._rows[0] if self._rows else None


class _Conn:
    """A connection that answers the resolver's one read: the resolved table's columns. Faked rather
    than provisioned because what is under test is the SCOPING — which checks the picker offers for
    which column type — and that is decided entirely from those rows and the compiled contexts."""

    def __init__(self, *results: _Rows):
        self._results = list(results)

    async def execute_core(self, _stmt):
        return self._results.pop(0)


@pytest.fixture(autouse=True)
def _governed_orders():
    """The dataset resolves against the compiled contexts — the pgwire names — not a catalog read."""
    from provisa.api.app import state

    state.contexts = dq_contexts((1, "wh", "sales", "sales", "orders"))
    yield
    state.contexts = {}


@pytest.mark.asyncio
async def test_the_catalog_scopes_each_column_to_its_own_type():
    conn = _Conn(
        _Rows(
            [
                {"column_name": "amount", "data_type": "numeric"},
                {"column_name": "ordered_at", "data_type": "date"},
            ]
        ),
    )
    catalog = await check_catalog_for(
        cast("Connection", conn), checker="soda", dataset="provisa/sales/orders"
    )
    assert catalog["error"] is None
    offered = {c["name"]: [k["check_type"] for k in c["checks"]] for c in catalog["columns"]}
    assert "aggregate" in offered["amount"]
    assert "aggregate" not in offered["ordered_at"]


@pytest.mark.asyncio
async def test_dataset_scoped_checks_come_back_beside_the_columns():
    conn = _Conn(_Rows([{"column_name": "amount", "data_type": "numeric"}]))
    catalog = await check_catalog_for(
        cast("Connection", conn), checker="soda", dataset="provisa/sales/orders"
    )
    assert "row_count" in [k["check_type"] for k in catalog["dataset_checks"]]


@pytest.mark.asyncio
async def test_a_dataset_that_resolves_nowhere_says_so_and_offers_no_columns():
    """The panel shows the message the registration would fail with, so the operator fixes the
    dataset line rather than wondering why the column picker is empty."""
    conn = _Conn()
    catalog = await check_catalog_for(
        cast("Connection", conn), checker="soda", dataset="provisa/sales/invoices"
    )
    assert "resolves to no governed table" in catalog["error"]
    assert catalog["columns"] == []
    assert [k["check_type"] for k in catalog["dataset_checks"]] != []


@pytest.mark.asyncio
async def test_an_unknown_checker_offers_nothing_at_all():
    catalog = await check_catalog_for(
        cast("Connection", _Conn()), checker="deequ", dataset="provisa/sales/orders"
    )
    assert "unknown data-quality checker" in catalog["error"]
    assert catalog["dataset_checks"] == []
    assert catalog["columns"] == []


def _build(**over):
    check = {
        "check_type": "missing",
        "column_name": "customer",
        "params": "",
        "comparator": "",
        "threshold_value": None,
        "metric": "",
        "unit": "",
        "level": "fail",
    }
    check.update(over)
    return build_check("soda", check)


def test_the_editors_arguments_become_one_checks_definition_text():
    built = _build(
        comparator="must_be_less_than", threshold_value=5, metric="percent", level="warn"
    )
    assert built["error"] is None
    assert yaml.safe_load(built["definition"]) == {
        "threshold": {"metric": "percent", "must_be_less_than": 5, "level": "warn"}
    }


def test_a_params_json_object_reaches_the_builder_as_the_checks_body():
    built = _build(check_type="invalid", params=json.dumps({"valid_values": ["gold", "silver"]}))
    assert built["error"] is None
    assert yaml.safe_load(built["definition"]) == {"valid_values": ["gold", "silver"]}


def test_a_rejected_build_comes_back_as_an_error_not_an_exception():
    """Same reason a parse failure does: the operator is mid-edit and the panel has to keep what
    they have typed while it says what is wrong with it."""
    built = _build(comparator="must_be_less_than")
    assert built["definition"] == ""
    assert "no value to compare against" in built["error"]


def test_unparseable_params_text_comes_back_as_an_error_too():
    built = _build(params="{not json")
    assert built["definition"] == ""
    assert built["error"] != ""


class _Job:
    def __init__(self):
        self.called = False

    async def func(self):
        self.called = True


class _Scheduler:
    """Answers get_job for exactly one registered id — the shape APScheduler's registry takes."""

    def __init__(self, job_id: str, job: _Job):
        self._job_id = job_id
        self._job = job

    def get_job(self, job_id: str):
        return self._job if job_id == self._job_id else None


def _contract_conn(dq_contract: str | None = "dataset: provisa/sales/orders\n"):
    return _Conn(_Rows([{"dq_contract": dq_contract}]))


@pytest.mark.asyncio
async def test_run_now_finds_the_bare_job_id_in_single_org_mode():
    """REQ-1266: register_poll_job/register_runtime namespace a job id by org ONLY when
    explicitly multi-org (current_org.get(None) → None in single-org/demo mode), so the caller
    must pass org_id=None here, not a resolved "default" org string — that would look up a
    ':org_default'-suffixed id the scheduler never registered."""
    job = _Job()
    scheduler = _Scheduler("poll:sales.orders", job)
    result = await run_dq_check_now(
        cast("Connection", _contract_conn()),
        scheduler=scheduler,
        org_id=None,
        schema_name="sales",
        table_name="orders",
    )
    assert result["success"] is True
    assert job.called is True


@pytest.mark.asyncio
async def test_run_now_namespaces_the_job_id_when_an_org_is_given():
    job = _Job()
    scheduler = _Scheduler("poll:sales.orders:org_acme", job)
    result = await run_dq_check_now(
        cast("Connection", _contract_conn()),
        scheduler=scheduler,
        org_id="acme",
        schema_name="sales",
        table_name="orders",
    )
    assert result["success"] is True
    assert job.called is True


@pytest.mark.asyncio
async def test_run_now_reports_the_missing_job_by_its_own_id_not_a_mismatched_one():
    """Regression: resolving org_id via `current_org.get() or state.org_id` in single-org mode
    produced the string "default", which looks up a suffixed id the bare-id job never has —
    this is the exact bug behind "quality.pets_scan has no scheduled poll job yet"."""
    scheduler = _Scheduler("poll:sales.orders", _Job())
    result = await run_dq_check_now(
        cast("Connection", _contract_conn()),
        scheduler=scheduler,
        org_id="default",
        schema_name="sales",
        table_name="orders",
    )
    assert result["success"] is False
    assert "has no scheduled poll job yet" in result["message"]
