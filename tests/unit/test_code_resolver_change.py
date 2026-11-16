# Copyright (c) 2026 Kenneth Stott
# Canary: 8a1f4c27-6e3b-4d90-b5c2-1f7e9d0a3b64
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A landed code resolver's result kept fresh by its @change function (REQ-2249), one test per
rule sentence: what the decorator states and how the function is found; when it is called and
with what; how its rows are applied; when the cursor is stored; the three refusals; and the
incremental setting's availability and rebuild interval."""

# Requirements: REQ-2249
from __future__ import annotations

from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import Any

import pytest
from sqlalchemy import insert, select, update

from provisa.codesource import commands
from provisa.codesource.bundle import BundleRefused
from provisa.codesource.manifest import (
    CHANGE,
    Change,
    Manifest,
    TableOf,
    manifest_from_json,
    manifest_json,
    read_manifest,
)
from provisa.core.schema_org import mv_build_state as MVT
from provisa.mv import change_refresh
from provisa.mv.change_refresh import (
    CURSOR_BACKWARDS,
    KEY_MISSING,
    KEY_REPEATED,
    NEEDS_REBUILD_EVERY,
    NO_CHANGE,
    ChangeRefused,
    Kept,
)
from provisa.mv.models import MVDefinition

pytestmark = pytest.mark.unit

N = {"kind": "number"}
S = {"kind": "string"}
B = {"kind": "boolean"}


def _ref(name: str) -> dict:
    return {"kind": "ref", "name": name}


def _object(name: str, *fields: tuple[str, dict]) -> dict:
    return {
        "kind": "object",
        "name": name,
        "fields": [{"name": n, "optional": False, "description": "", "type": t} for n, t in fields],
    }


TYPES = {
    "Store": _object("Store", ("id", N), ("lat", N)),
    "Region": _object("Region", ("id", N), ("name", S)),
    "Match": _object("Match", ("store_id", N), ("region_id", N), ("score", N)),
    "Changed": _object(
        "Changed", ("store_id", N), ("region_id", N), ("score", N), ("removed", B), ("seq", N)
    ),
}

LINES = ("table=store_regions", "key=store_id,region_id", "removed=removed", "cursor=seq")


def _change_export(
    name: str = "store_regions_changes",
    lines: tuple[str, ...] = LINES,
    *,
    node: bool,
    cursor: tuple[str, ...] = ("since",),
) -> dict:
    """A @change export as each reader reports it: Node's lists the tag with each line's text;
    Python's, Go's and Rust's list a tag with an argument under tag_arguments alone."""
    parameters = [
        {"name": "stores", "optional": False, "rest": False,
         "type": {"kind": "table_ref", "row": _ref("Store")}},
        {"name": "regions", "optional": False, "rest": False,
         "type": {"kind": "table_ref", "row": _ref("Region")}},
    ] + [{"name": c, "optional": True, "rest": False, "type": N} for c in cursor]  # fmt: skip
    return {
        "name": name,
        "tags": [CHANGE] * len(lines) if node else [],
        "tag_arguments": [{"tag": CHANGE, "text": line} for line in lines],
        "description": "",
        "signatures": [
            {"parameters": parameters, "returns": {"kind": "array", "element": _ref("Changed")}}
        ],
    }


def _raw(*exports: dict) -> dict:
    return {"exports": list(exports), "types": TYPES}


# -- the decorator --------------------------------------------------------------------------------


@pytest.mark.parametrize("node", [True, False])
def test_a_change_function_states_its_table_key_removal_and_cursor_columns(node):
    """The decorator states what only the author knows: the key columns of a match, the column
    that marks a match as removed, and the cursor column (and the table it keeps fresh)."""
    (export,) = read_manifest(_raw(_change_export(node=node))).exports
    assert export.kind == CHANGE
    assert export.change == Change("store_regions", ("store_id", "region_id"), "removed", "seq")
    assert [p.name for p in export.parameters] == ["stores", "regions", "since"]
    assert isinstance(export.parameters[0].shape, TableOf)


@pytest.mark.parametrize(
    ("lines", "problem"),
    [
        (LINES[:3], "@change states cursor"),
        (("table=store_regions",), "@change states key, removed, cursor"),
        ((*LINES, "key=store_id"), "@change key is stated twice"),
        ((*LINES[:3], "cursor seq"), "write @change table=, key=, removed= or cursor="),
        ((*LINES[1:], "table="), "write @change table="),
        (("table=t", "key=a,,b", "removed=r", "cursor=c"), "name the key columns"),
    ],
)
def test_a_change_function_that_does_not_state_each_part_once_is_refused_by_name(lines, problem):
    with pytest.raises(BundleRefused, match=problem):
        read_manifest(_raw(_change_export(lines=lines, node=False)))


def test_a_change_function_takes_its_resolvers_tables_and_one_cursor():
    with pytest.raises(BundleRefused, match="takes its resolver's tables and one more parameter"):
        read_manifest(_raw(_change_export(node=False, cursor=())))
    with pytest.raises(BundleRefused, match="it takes 2 other parameter"):
        read_manifest(_raw(_change_export(node=False, cursor=("since", "until"))))


def test_two_change_functions_for_one_table_are_refused_naming_both():
    with pytest.raises(BundleRefused, match="is on store_regions_changes already"):
        read_manifest(_raw(_change_export(node=False), _change_export("again", node=False)))


def test_the_change_declaration_is_kept_with_a_built_source():
    manifest = read_manifest(_raw(_change_export(node=False)))
    assert manifest_from_json(manifest_json(manifest)) == manifest


_PYTHON_CODE = """
from dataclasses import dataclass
from provisa_services import TableRef


@dataclass
class Store:
    id: int


@dataclass
class Region:
    id: int


@dataclass
class Changed:
    store_id: int
    region_id: int
    removed: bool
    seq: int


def store_regions_changes(
    stores: TableRef[Store], regions: TableRef[Region], since: int | None = None
) -> list[Changed]:
    \"\"\"Store-region matches changed since ``since``.

    @change table=store_regions
    @change key=store_id,region_id
    @change removed=removed
    @change cursor=seq
    \"\"\"
    return []
"""


def test_the_python_reader_carries_a_change_function(tmp_path):
    """Python's reader reads @change lines from a docstring as it reads every tag, and the
    manifest carries the declaration."""
    import json
    import subprocess
    import sys
    from pathlib import Path

    from provisa.codesource.manifest import PYTHON_RULES
    from provisa.python.source import READER

    (tmp_path / "rows.py").write_text(_PYTHON_CODE)
    done = subprocess.run(  # noqa: S603 -- the test's interpreter and Provisa's reader
        [sys.executable, "-I", str(READER), str(tmp_path), "rows"],
        capture_output=True,
        text=True,
        env={"PYTHONPATH": str(Path(READER).parent / "services")},
    )
    assert done.returncode == 0, done.stderr
    (export,) = read_manifest(json.loads(done.stdout), PYTHON_RULES).exports
    assert export.change == Change("store_regions", ("store_id", "region_id"), "removed", "seq")
    assert [p.name for p in export.parameters] == ["stores", "regions", "since"]


# -- found by its name ----------------------------------------------------------------------------


def _manifest() -> Manifest:
    return read_manifest(_raw(_change_export(node=False)))


@pytest.fixture
def source(monkeypatch):
    """The resolver's source, its manifest the one read above (no worker is run)."""
    prepared = SimpleNamespace(manifest=_manifest(), remote=None)

    async def _prepared(state, source_id, function):
        del state, function
        assert source_id == "geo"
        return prepared

    monkeypatch.setattr(commands, "_prepared", _prepared)
    return prepared


RESOLVER = {
    "name": "match_stores",
    "function_name": "match_stores",
    "source_id": "geo",
    "impl_kind": commands.CODE_COMMAND,
    "kind": "query",
    "arguments": [
        {"name": "stores", "type": "text", "arg_kind": "table_ref", "columns": []},
        {"name": "regions", "type": "text", "arg_kind": "table_ref", "columns": []},
    ],
    "output_columns": [
        {"name": "store_id", "type": "bigint"},
        {"name": "region_id", "type": "bigint"},
        {"name": "score", "type": "double"},
    ],
}
VIEW_SQL = "SELECT * FROM match_stores('stores', 'regions') AS m"


async def test_the_change_function_is_found_by_the_name_of_the_table_it_keeps_fresh(source):
    found = await commands.change_of(None, "geo", "store_regions")
    assert found is not None and found[0].name == "store_regions_changes"
    assert await commands.change_of(None, "geo", "other_bridge") is None


# -- the incremental setting ----------------------------------------------------------------------


def _state(db: Any = None) -> SimpleNamespace:
    return SimpleNamespace(tenant_db=db, tracked_functions={"match_stores": RESOLVER})


async def test_a_resolver_with_no_change_function_has_the_incremental_option_disabled(source):
    """A code resolver with no @change function has the incremental option disabled, and the
    reason is shown."""
    option = await change_refresh.incremental_option(_state(), RESOLVER, "other_bridge")
    assert not option.available
    assert option.code == NO_CHANGE
    assert option.reason is not None and "@change table=other_bridge" in option.reason
    assert "REQ-" not in option.reason
    assert (await change_refresh.incremental_option(_state(), RESOLVER, "store_regions")).available


async def test_the_incremental_setting_cannot_be_saved_on_without_a_change_function(source):
    """It cannot be chosen for that table, in the UI or in configuration."""
    refused = await change_refresh.incremental_refusal(
        _state(), "other_bridge", VIEW_SQL, True, 3600
    )
    assert refused is not None and refused.code == NO_CHANGE
    assert (
        await change_refresh.incremental_refusal(_state(), "other_bridge", VIEW_SQL, False, None)
        is None
    )


async def test_a_periodic_whole_rebuild_is_required_while_kept_incrementally(source):
    refused = await change_refresh.incremental_refusal(
        _state(), "store_regions", VIEW_SQL, True, None
    )
    assert refused is not None and refused.code == NEEDS_REBUILD_EVERY
    assert "REQ-" not in str(refused)
    assert (
        await change_refresh.incremental_refusal(_state(), "store_regions", VIEW_SQL, True, 3600)
        is None
    )


def _mv(*, incremental: bool = True, rebuild_every: int | None = 3600) -> MVDefinition:
    return MVDefinition(
        id="view-store_regions",
        source_tables=[],
        target_catalog="mat_store",
        target_schema="main",
        target_table="mv_store_regions",
        view_table="store_regions",
        sql=VIEW_SQL,
        semantic_sql=VIEW_SQL,
        incremental=incremental,
        rebuild_every=rebuild_every,
    )


async def test_with_the_incremental_setting_off_change_is_not_called(source):
    """With the result landed and the incremental setting off, a refresh recomputes the whole
    result with the resolver's ordinary function and @change is not called."""
    assert await change_refresh.kept_by_change(_state(), _mv(incremental=False)) is None
    assert change_refresh.change_generate(_state(), None, _mv(incremental=False)) is None
    assert await change_refresh.kept_by_change(_state(), _mv()) is not None
    assert change_refresh.change_generate(_state(), None, _mv()) is not None


# -- calling it and applying what it returns ------------------------------------------------------


class _Engine:
    """The EngineRuntime's two landing faces, recorded."""

    def __init__(self, fail: bool = False) -> None:
        self.replaced: list[list[dict]] = []
        self.applied: list[list[tuple[str, dict]]] = []
        self.fail = fail

    async def persist_mv_table(self, *, schema, table, columns, rows, persist, pk_columns):
        assert (schema, table, persist, pk_columns) == (
            "main", "mv_store_regions", "replace", ["store_id", "region_id"]
        )  # fmt: skip
        assert [c for c, _ in columns] == ["store_id", "region_id", "score"]
        self.replaced.append(rows)
        return "main.mv_store_regions"

    async def apply_cdc_events(self, *, schema, table, columns, pk_columns, events):
        del schema, table, columns
        assert pk_columns == ["store_id", "region_id"]
        if self.fail:
            raise RuntimeError("the store refused the write")
        self.applied.append([(e.operation, e.row) for e in events])
        return {}


def _row(store: int, region: int, seq: int, *, removed: bool = False, score: float = 1.0) -> dict:
    return {"store_id": store, "region_id": region, "score": score, "removed": removed, "seq": seq}


@pytest.fixture
def calls(monkeypatch, source):
    """The @change function's calls, each answered by the next queued result."""
    from provisa.core import region_admin
    from provisa.executor import function_dispatch

    made: list[dict] = []
    answers: list[list[dict]] = []

    async def _dispatch(fn, args, state, role_id):
        del state
        made.append({"fn": fn, "args": args, "role": role_id})
        return answers.pop(0)

    monkeypatch.setattr(function_dispatch, "dispatch_function", _dispatch)
    monkeypatch.setattr(region_admin, "session_vars", lambda state: {})
    return SimpleNamespace(made=made, answers=answers)


@pytest.fixture
async def ledger(control_plane):
    return control_plane.org()


async def _cursor(ledger) -> tuple[Any, Any]:
    async with ledger.acquire() as conn:
        row = (
            await conn.execute_core(
                select(MVT.c.change_cursor, MVT.c.change_rebuilt_at).where(
                    MVT.c.mv_id == "view-store_regions"
                )
            )
        ).first()
    return (row.change_cursor, row.change_rebuilt_at) if row is not None else (None, None)


async def _refresh(ledger, engine, *, full: bool = False) -> change_refresh.Applied:
    state = _state(ledger)
    mv = _mv()
    kept = await change_refresh.kept_by_change(state, mv)
    assert kept is not None
    return await change_refresh.refresh_by_change(state, engine, mv, kept, full=full)


async def test_the_first_build_calls_change_with_no_cursor_and_lands_every_match(calls, ledger):
    """Called with no cursor, on the first run, it returns every match; every match it returns is
    landed."""
    calls.answers.append([_row(1, 10, 5), _row(2, 20, 7)])
    engine = _Engine()
    applied = await _refresh(ledger, engine)
    (call,) = calls.made
    assert call["args"]["since"] is None
    assert applied.whole == "first_build"
    assert engine.replaced == [
        [
            {"store_id": 1, "region_id": 10, "score": 1.0},
            {"store_id": 2, "region_id": 20, "score": 1.0},
        ]
    ]
    assert (await _cursor(ledger))[0] == "7"


async def test_a_refresh_passes_the_stored_cursor_and_the_resolvers_inputs_as_tables(calls, ledger):
    """Provisa passes the cursor it stored after the last successful run and the resolver's
    inputs as tables; the function never learns where its result is stored."""
    calls.answers += [[_row(1, 10, 5)], []]
    await _refresh(ledger, _Engine())
    await _refresh(ledger, _Engine())
    second = calls.made[1]
    assert second["args"] == {"stores": "stores", "regions": "regions", "since": 5}
    assert [a.get("arg_kind") for a in second["fn"]["arguments"]] == [
        "table_ref",
        "table_ref",
        None,
    ]
    assert second["fn"]["function_name"] == "store_regions_changes"
    assert "mv_store_regions" not in repr(second["args"])


async def test_returned_matches_are_upserted_by_key_and_a_removed_one_removes_its_key(
    calls, ledger
):
    """Provisa lands them with the existing incremental apply: upsert by key, and a returned row
    flagged as removed removes its key."""
    calls.answers += [[_row(1, 10, 5)], [_row(1, 10, 6, score=0.5), _row(2, 20, 7, removed=True)]]
    await _refresh(ledger, _Engine())
    engine = _Engine()
    applied = await _refresh(ledger, engine)
    assert applied.whole is None
    assert engine.replaced == []
    assert engine.applied == [
        [
            ("upsert", {"store_id": 1, "region_id": 10, "score": 0.5}),
            ("delete", {"store_id": 2, "region_id": 20}),
        ]
    ]
    assert (await _cursor(ledger))[0] == "7"


async def test_a_whole_rebuild_calls_change_with_no_cursor_when_rebuild_every_elapsed(
    calls, ledger
):
    """On every whole rebuild it is called with no cursor, so a periodic whole rebuild serves
    deletes it never reported."""
    calls.answers += [[_row(1, 10, 5)], [_row(3, 30, 9)]]
    await _refresh(ledger, _Engine())
    async with ledger.acquire() as conn:
        await conn.execute_core(
            update(MVT)
            .where(MVT.c.mv_id == "view-store_regions")
            .values(change_rebuilt_at=datetime.now(UTC) - timedelta(hours=2))
        )
    engine = _Engine()
    applied = await _refresh(ledger, engine)
    assert calls.made[1]["args"]["since"] is None
    assert applied.whole == "rebuild_interval_elapsed"
    assert engine.replaced == [[{"store_id": 3, "region_id": 30, "score": 1.0}]]


async def test_an_operators_full_refresh_is_a_whole_rebuild(calls, ledger):
    calls.answers += [[_row(1, 10, 5)], [_row(1, 10, 6)]]
    await _refresh(ledger, _Engine())
    applied = await _refresh(ledger, _Engine(), full=True)
    assert calls.made[1]["args"]["since"] is None
    assert applied.whole == "operator_requested"


async def test_the_cursor_is_stored_only_after_a_successful_apply(calls, ledger):
    calls.answers += [[_row(1, 10, 5)], [_row(1, 10, 8)]]
    await _refresh(ledger, _Engine())
    stored = await _cursor(ledger)
    with pytest.raises(RuntimeError, match="the store refused the write"):
        await _refresh(ledger, _Engine(fail=True))
    assert await _cursor(ledger) == stored


@pytest.mark.parametrize(
    ("rows", "code"),
    [
        ([{"store_id": 1, "score": 1.0, "removed": False, "seq": 6}], KEY_MISSING),
        ([_row(1, 10, 6), _row(1, 10, 7)], KEY_REPEATED),
        ([_row(1, 10, 4)], CURSOR_BACKWARDS),
    ],
)
async def test_rows_lacking_a_key_repeating_one_or_going_backwards_are_refused(
    calls, ledger, rows, code
):
    """A function whose returned rows lack a key column, repeat a key within one result, or whose
    cursor goes backwards is refused with the reason: nothing is applied, no cursor stored."""
    calls.answers += [[_row(1, 10, 5)], rows]
    await _refresh(ledger, _Engine())
    stored = await _cursor(ledger)
    engine = _Engine()
    with pytest.raises(ChangeRefused) as refused:
        await _refresh(ledger, engine)
    assert refused.value.code == code
    assert "REQ-" not in str(refused.value)
    assert refused.value.params["function"] == "store_regions_changes"
    assert engine.applied == [] and engine.replaced == []
    assert await _cursor(ledger) == stored


def test_the_checked_rows_need_a_kept_resolver():
    """``checked`` reads the declaration off the @change export it is given."""
    kept = Kept(RESOLVER, _manifest().exports[0], _manifest(), ("stores", "regions"))
    events, cursor = change_refresh.checked([_row(1, 10, 3)], kept, None)
    assert [e.operation for e in events] == ["upsert"] and cursor == 3


async def test_the_cursor_is_kept_on_the_views_build_state(ledger):
    """The build row is seeded when it is not there yet (the event loop's first fire)."""
    async with ledger.acquire() as conn:
        await conn.execute_core(insert(MVT).values(mv_id="view-x", region="default"))
    mv = _mv()
    await change_refresh._store(ledger, mv, 12, rebuilt_at=None)
    assert (await _cursor(ledger))[0] == "12"
