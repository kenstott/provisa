# Copyright (c) 2026 Kenneth Stott
# Canary: a40c47e1-51a0-4dc3-aba8-4c9e2b7d19e8
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The affirmation demo's matching engine is a Python code table (REQ-2006).

trade_match, the @table function of the matcher code source, takes one allocation's four records
-- the order system's allocation, the execution system's average price, the broker's confirmation
and the custodian's instruction -- and returns a row for each break, naming it, or one matched row.
The config registers it as a table whose parameters are the function's, fed by the legs view
through a relationship; the archive is the one the config names. Each break generate.py seeds is
the break the function names, and every other allocation matches."""

# Requirements: REQ-2006
from __future__ import annotations

import importlib.util
import sys
import tarfile
import tempfile
from dataclasses import MISSING, fields
from datetime import date
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest
import yaml

from provisa.codesource.bundle import content_hash, unpack

pytestmark = pytest.mark.unit

_DEMO = Path(__file__).resolve().parents[2] / "demo" / "named" / "affirmation"
_CONFIG = yaml.safe_load((_DEMO / "config.yaml").read_text())
_SOURCES = {s["id"]: s for s in _CONFIG["sources"]}
_TABLES = {t["table"]: t for t in _CONFIG["tables"]}
_VIEWS = {v["id"]: v for v in _CONFIG["views"]}
_RELATIONSHIPS = {r["id"]: r for r in _CONFIG["relationships"]}


def _load(name: str, path: Path) -> ModuleType:
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module  # its dataclasses look their module up while they are made
    spec.loader.exec_module(module)
    return module


_MATCH = _load("trade_match", _DEMO / "matcher" / "trade_match.py")
_GEN = _load("affirmation_generate", _DEMO / "generate.py")


def _legs_of_every_allocation() -> dict[str, Any]:
    """Each allocation's TradeLegs, as the legs view reads them from what generate.py writes."""
    return {
        ref: _MATCH.TradeLegs(**_GEN.trade_legs(o, ref, fund, qty))
        for o, ref, fund, qty in _GEN.allocations_of()
    }


_LEGS = _legs_of_every_allocation()


@pytest.mark.parametrize("ref", sorted(_LEGS))
def test_each_allocation_breaks_exactly_as_seeded(ref):
    rows = _MATCH.trade_match(_LEGS[ref])
    seeded = _GEN.BREAKS.get(ref)
    if seeded is None:
        assert [(r.status, r.break_type, r.detail) for r in rows] == [("matched", None, None)]
    else:
        assert [(r.status, r.break_type) for r in rows] == [("break", seeded)]
        assert rows[0].detail and rows[0].trade_ref == ref


def test_a_quantity_break_is_not_also_named_a_net_amount_break():
    legs = _LEGS["A-1003-2"]
    assert legs.confirm_net != legs.instruction_net
    assert [r.break_type for r in _MATCH.trade_match(legs)] == ["quantity"]


def test_a_trade_neither_confirmed_nor_instructed_names_both_missing_legs():
    legs = _MATCH.TradeLegs(
        trade_ref="A-9",
        side="BUY",
        quantity=100.0,
        settle_date=date(2026, 11, 19),
        exec_price=10.0,
        ssi_custodian="MRDNUS33",
        ssi_account="GEF01-US-0001",
    )
    assert [r.break_type for r in _MATCH.trade_match(legs)] == [
        "no_confirmation",
        "no_instruction",
    ]


def test_every_break_it_can_name_is_one_the_table_describes():
    described = next(
        c["description"] for c in _TABLES["trade_match"]["columns"] if c["name"] == "break_type"
    )
    kinds = {
        "no_confirmation",
        "no_instruction",
        "side",
        "quantity",
        "price",
        "settlement_date",
        "ssi",
        "instruction_quantity",
        "instruction_settlement_date",
        "net_amount",
    }
    assert all(kind in described for kind in kinds)
    assert set(_GEN.BREAKS.values()) <= kinds


def test_the_table_s_parameters_are_the_function_s_inputs():
    params = {
        c["name"][len("_nf_") :]: c["native_filter_required"]
        for c in _TABLES["trade_match"]["columns"]
        if c.get("native_filter_type") == "query_arg"
    }
    wanted = {
        f.name: f.default is MISSING and f.default_factory is MISSING
        for f in fields(_MATCH.TradeLegs)
    }
    assert params == wanted
    outputs = [
        c["name"] for c in _TABLES["trade_match"]["columns"] if "native_filter_type" not in c
    ]
    assert outputs == [f.name for f in fields(_MATCH.Match)]


def test_the_legs_view_feeds_every_parameter_through_the_relationship():
    feed = _RELATIONSHIPS["legs-to-match"]
    assert (feed["source_table_id"], feed["target_table_id"]) == ("view_trade_legs", "trade_match")
    assert feed["parameters"] == {f.name: f.name for f in fields(_MATCH.TradeLegs)}
    legs_columns = {c["name"] for c in _VIEWS["trade_legs"]["columns"]}
    assert set(feed["parameters"].values()) <= legs_columns


def test_the_matched_trades_are_affirmed_after_the_match_is_stored():
    assert _VIEWS["trade_status"]["materialize"] is True
    steps = yaml.safe_load((_DEMO / "setup.yaml").read_text())["steps"]
    assert steps == [{"refresh": "trade_status"}, {"command": "affirm_matched"}]
    [command] = _CONFIG["functions"]
    assert command["impl_kind"] == "statement"
    assert '"postrade"."view_trade_status"' in command["binding"]["sql"]
    assert '"postrade"."affirmation_log"' in command["binding"]["sql"]


def test_the_code_archive_is_the_one_the_config_names():
    archive = _DEMO / "code" / "trade_match.tar.gz"
    with tempfile.TemporaryDirectory() as scratch:
        found = content_hash(unpack(archive.name, archive.read_bytes(), Path(scratch)))
    assert _SOURCES["matcher"]["mapping"]["code"] == {
        "kind": "upload",
        "upload": found,
        "filename": archive.name,
    }


def test_the_archive_holds_the_function_as_the_demo_shows_it():
    with tarfile.open(_DEMO / "code" / "trade_match.tar.gz") as archive:
        shipped = archive.extractfile("trade_match/trade_match.py")
        assert shipped is not None
        assert shipped.read() == (_DEMO / "matcher" / "trade_match.py").read_bytes()
