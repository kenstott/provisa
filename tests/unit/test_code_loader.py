# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Batched join keys through a ``loader`` (REQ-2100): a join's argument sets reach a function that
takes a ``loader`` as a hash of sparse keys, de-duplicated by their hashes, in batches of the size
it declares, and each set is given the rows answered under its own key. A read with the table's own
arguments calls it without one. The worker is a real Python host process (unsandboxed)."""

# Requirements: REQ-2100
from __future__ import annotations

import json
import subprocess
import sys
import textwrap
from types import SimpleNamespace

import pytest

from provisa.codesource import loader
from provisa.codesource import source as code_source
from provisa.codesource import table as code_table
from provisa.codesource import worker as w
from provisa.codesource.manifest import PYTHON_RULES, ManifestRefused, read_manifest
from provisa.python.source import HOST, READER

pytestmark = pytest.mark.unit

CODE = '''
from dataclasses import dataclass

_calls = 0


@dataclass
class Score:
    customer_id: str
    plan: str
    score: float
    batch: int
    call: int


def churn(
    customer_id: str | None = None, plan: str = "basic", loader: dict | None = None
) -> list[Score]:
    """@table
    @loader 2
    """
    global _calls
    _calls += 1
    if loader is None:
        return [Score(customer_id, plan, 0.0, 0, _calls)]
    return {
        key: [Score(k["customer_id"], k.get("plan", "basic"), 0.5, len(loader), _calls)]
        for key, k in loader.items()
    }


def unbatched(
    customer_id: str | None = None, plan: str = "basic", loader: dict | None = None
) -> list[Score]:
    """@table"""
    global _calls
    _calls += 1
    return {
        key: [Score(k["customer_id"], k.get("plan", "basic"), 0.5, len(loader), _calls)] * 2
        for key, k in loader.items()
    }


def stray(customer_id: str | None = None, loader: dict | None = None) -> list[Score]:
    """@table"""
    return {"not-a-key-sent": [Score("x", "basic", 0.0, 0, 0)]}
'''


def _read(tmp_path, source: str, entry: str) -> dict:
    (tmp_path / f"{entry}.py").write_text(textwrap.dedent(source))
    done = subprocess.run(
        [sys.executable, "-I", str(READER), str(tmp_path), entry], capture_output=True, text=True
    )
    assert done.returncode == 0, done.stderr
    return json.loads(done.stdout)


@pytest.fixture
def prepared(tmp_path, monkeypatch):
    code = tmp_path / "code"
    code.mkdir()
    manifest = read_manifest(_read(code, CODE, "scores"), PYTHON_RULES)
    command = w.HostCommand(
        argv=lambda address: [sys.executable, "-I", str(HOST), address, str(code), "scores"],
        env=w.private_environment(PYTHONDONTWRITEBYTECODE="1"),
        cwd=code,
    )
    ready = code_source.Prepared("scores-src", "h1", manifest, command, "acme")
    source = SimpleNamespace(id="scores-src", type=SimpleNamespace(value="python"), mapping={})

    async def _registered(_state):
        return [source]

    async def _prepare(*_args):
        return ready

    async def _context(_state):
        return None

    monkeypatch.setattr("provisa.federation.registry_view.registered_sources", _registered)
    monkeypatch.setattr(code_source, "prepare", _prepare)
    monkeypatch.setattr(code_source, "context_for", _context)
    yield ready
    w.pool.stop_source("scores-src")


def _column(name, data_type, *, kind=None):
    return {
        "column_name": name,
        "name": name,
        "data_type": data_type,
        "path": None,
        "native_filter_type": kind,
        "native_filter_required": False if kind else None,
    }


def _table(function: str = "churn") -> dict:
    columns = [
        _column("customer_id", "text"),
        _column("plan", "text"),
        _column("score", "double"),
        _column("batch", "bigint"),
        _column("call", "bigint"),
        _column("_nf_customer_id", "text", kind="query_arg"),
    ]
    if function != "stray":
        columns.append(_column("_nf_plan", "text", kind="query_arg"))
    return {"table_name": function, "source_id": "scores-src", "columns": columns}


async def _joined(function: str, sets: list[dict]) -> list[dict]:
    read = await code_table.read_each(None, _table(function), sets, joined=True)
    return read.rows


# -- one call per batch, de-duplicated, each set given its own key's rows --------------------------


async def test_n_distinct_keys_go_in_one_call_and_each_set_gets_its_keys_rows(prepared):
    rows = await _joined(
        "unbatched", [{"customer_id": "1"}, {"customer_id": "2"}, {"customer_id": "3"}]
    )
    assert sorted(r["customer_id"] for r in rows) == ["1", "1", "2", "2", "3", "3"]
    assert {r["batch"] for r in rows} == {3} and len({r["call"] for r in rows}) == 1


async def test_a_key_shared_by_several_sets_is_sent_once_and_fans_out_to_each(prepared):
    rows = await _joined(
        "unbatched", [{"customer_id": "1"}, {"customer_id": "1"}, {"customer_id": "2"}]
    )
    assert {r["batch"] for r in rows} == {2}  # two distinct keys sent
    assert sorted(r["customer_id"] for r in rows) == ["1", "1", "1", "1", "2", "2"]


async def test_a_sparse_key_whose_field_the_function_defaults_matches_back(prepared):
    # The second set gives no plan: its key leaves plan out, the function fills "basic", and the
    # row still comes back to that set, by its key's hash.
    rows = await _joined(
        "unbatched", [{"customer_id": "1", "plan": "pro"}, {"customer_id": "2", "plan": None}]
    )
    by_customer = {r["customer_id"]: r["plan"] for r in rows}
    assert by_customer == {"1": "pro", "2": "basic"}


async def test_a_declared_batch_size_chunks_the_keys_and_merges_their_rows(prepared):
    sets = [{"customer_id": str(i)} for i in range(5)]
    rows = await _joined("churn", sets)  # @loader 2: three calls of 2, 2 and 1 keys
    assert sorted(r["customer_id"] for r in rows) == ["0", "1", "2", "3", "4"]
    assert sorted(r["batch"] for r in rows) == [1, 2, 2, 2, 2]
    assert len({r["call"] for r in rows}) == 3


async def test_a_read_with_the_tables_own_arguments_calls_without_a_loader(prepared):
    read = await code_table.read(None, _table(), {"customer_id": "9", "plan": "pro"})
    assert [(r["customer_id"], r["plan"], r["batch"]) for r in read.rows] == [("9", "pro", 0)]


async def test_an_answer_under_a_key_the_batch_did_not_send_is_refused_by_name(prepared):
    with pytest.raises(code_table.CodeTableRefused, match="key 'not-a-key-sent'"):
        await _joined("stray", [{"customer_id": "1"}])


# -- the key's hash --------------------------------------------------------------------------------


def test_a_keys_hash_is_its_canonical_json_whatever_the_fields_order():
    assert loader.key_hash({"a": "1", "b": 2}) == loader.key_hash({"b": 2, "a": "1"})
    assert loader.key_hash({"a": "1"}) != loader.key_hash({"a": "2"})
    assert loader.sparse_key({"a": "1", "b": None}) == {"a": "1"}


# -- declarations ----------------------------------------------------------------------------------


def test_the_loader_is_no_parameter_of_the_table_and_its_size_is_read(tmp_path, prepared):
    from provisa.codesource.discovery import declared_columns

    manifest = prepared.manifest
    churn = manifest.export("churn")
    assert churn.loader and churn.loader_batch == 2
    assert manifest.export("unbatched").loader_batch is None
    names = [c["name"] for c in declared_columns(manifest, churn)]
    assert "_nf_loader" not in names and "_nf_customer_id" in names
    code_table.check_table(_table(), manifest)


@pytest.mark.parametrize(
    "source, problem",
    [
        (
            '''
            def f(x: int | None = None) -> list[dict]:
                """@table
                @loader 10
                """
                return []
            ''',
            "f: loader 10 declares a batch size, and f has no loader parameter",
        ),
        (
            '''
            def f(loader: dict | None = None) -> list[dict]:
                """@table
                @loader many
                """
                return []
            ''',
            "f: loader many: a batch size is a positive whole number",
        ),
        (
            """
            from typing import Annotated


            def f(loader: Annotated[dict | None, "loader 5"] = None) -> list[dict]:
                '''@table
                @loader 10
                '''
                return []
            """,
            "f: two batch sizes declared, 10 and 5",
        ),
    ],
)
def test_a_batch_size_the_function_cannot_take_is_refused_by_name(tmp_path, source, problem):
    with pytest.raises(ManifestRefused) as refused:
        read_manifest(_read(tmp_path, source, "m"), PYTHON_RULES)
    assert problem in refused.value.problems


def test_annotated_declares_the_batch_size(tmp_path):
    source = """
    from typing import Annotated


    def f(loader: Annotated[dict | None, "loader 50"] = None) -> list[dict]:
        '''@table'''
        return []
    """
    assert read_manifest(_read(tmp_path, source, "m"), PYTHON_RULES).export("f").loader_batch == 50
