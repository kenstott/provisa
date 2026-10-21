# Copyright (c) 2026 Kenneth Stott
# Canary: 163e5c2a-4cb2-4405-8804-61f66d0a90ce
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A code source's tables (REQ-1984, REQ-1993; part 5): a source's configuration checked; a table
named for its function, its parameters exactly the function's (REQ-1986); what Register Table
suggests from the code's declarations; and a read -- the function called in the worker with the
statement's arguments in declared order, one left out absent so its default applies, its rows
checked against the table's columns -- landed as a pipeline table's are (REQ-1980).

The worker is a real host process (the Python host, this interpreter, unsandboxed); the source's
preparation (install and read) is stood in with the fixture code's manifest."""

# Requirements: REQ-1984, REQ-1993, REQ-1986, REQ-1980
from __future__ import annotations

import json
import shutil
import subprocess
import sys
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

import pytest

from provisa.codesource import source as code_source
from provisa.codesource import table as code_table
from provisa.codesource import worker as w
from provisa.codesource.bundle import BundleRefused
from provisa.codesource.discovery import declared_columns
from provisa.codesource.manifest import PYTHON_RULES, read_manifest
from provisa.codesource.table import CodeTableRefused, check_table
from provisa.python.source import HOST, READER

pytestmark = pytest.mark.unit

FIXTURE = Path(__file__).parent.parent / "fixtures" / "python_source"


@pytest.fixture(scope="module")
def manifest():
    done = subprocess.run(
        [sys.executable, "-I", str(READER), str(FIXTURE), "orders"], capture_output=True, text=True
    )
    assert done.returncode == 0, done.stderr
    return read_manifest(json.loads(done.stdout), PYTHON_RULES)


def _column(name, data_type, *, kind=None, required=None):
    return {
        "column_name": name,
        "name": name,
        "data_type": data_type,
        "path": None,
        "native_filter_type": kind,
        "native_filter_required": required,
    }


def _orders_table(**changed) -> dict:
    table = {
        "id": 7,
        "table_name": "orders",
        "source_id": "orders-src",
        "query_template": None,
        "columns": [
            _column("id", "bigint"),
            _column("placed", "timestamp"),
            _column("due", "date"),
            _column("total", "numeric"),
            _column("tier", "text"),
            _column("lines", "json"),
            _column("note", "text"),
            _column("_nf_customer_id", "text", kind="query_arg", required=True),
            _column("_nf_since", "timestamp", kind="query_arg", required=False),
        ],
    }
    table.update(changed)
    return table


# -- the source's configuration -----------------------------------------------------------------


def test_a_code_sources_configuration_is_its_mapping():
    config = code_source.config_of(
        "python",
        {"code": {"kind": "pypi", "package": "rates", "version": "1.2.0"}, "content_hash": "h1"},
    )
    assert (config.language, config.runtime, config.content_hash) == ("python", "python", "h1")
    node = code_source.config_of(
        "node",
        {"runtime": "bun", "code": {"kind": "git", "repository": "https://g/r", "commit": "c"}},
    )
    assert node.runtime == "bun" and node.lockfile is None


@pytest.mark.parametrize(
    "code",
    [
        {"kind": "git", "repository": "https://g/r", "commit": "c"},
        {"kind": "upload", "upload": "h"},
        {"kind": "pypi", "package": "p", "version": "1"},
    ],
)
def test_a_lockfile_may_be_given_beside_any_code(code):
    lockfile = {"filename": "uv.lock", "upload": "x"}
    config = code_source.config_of("python", {"code": code, "lockfile": lockfile})
    assert config.lockfile == lockfile


@pytest.mark.parametrize(
    ("source_type", "mapping", "code"),
    [
        (
            "node",
            {"code": {"kind": "npm", "package": "p", "version": "1.0.0"}},
            "codesource.unknown_runtime",
        ),
        (
            "python",
            {"code": {"kind": "npm", "package": "p", "version": "1"}},
            "codesource.code_kind",
        ),
        ("python", {"code": {"kind": "pypi", "package": "p"}}, "codesource.code_incomplete"),
    ],
)
def test_a_configuration_that_cannot_be_used_is_refused_by_name(source_type, mapping, code):
    with pytest.raises(BundleRefused) as refused:
        code_source.config_of(source_type, mapping)
    assert refused.value.code == code


# -- registering a table ------------------------------------------------------------------------


def test_a_table_is_its_function_with_exactly_its_parameters(manifest):
    check_table(_orders_table(), manifest)


@pytest.mark.parametrize(
    ("changed", "reason"),
    [
        ({"table_name": "helper"}, "has no @table function 'helper'"),
        ({"table_name": "cancel_order"}, "has no @table function 'cancel_order'"),
        ({"query_template": "SELECT 1"}, "it has no query"),
    ],
)
def test_a_table_its_code_does_not_offer_is_refused(manifest, changed, reason):
    with pytest.raises(CodeTableRefused, match=reason):
        check_table(_orders_table(**changed), manifest)


def test_a_table_whose_parameters_are_not_its_functions_is_refused(manifest):
    columns = [c for c in _orders_table()["columns"] if c["name"] != "_nf_since"]
    with pytest.raises(CodeTableRefused, match="are not orders's parameters"):
        check_table(_orders_table(columns=columns), manifest)
    loosened = [
        {**c, "native_filter_required": False} if c["name"] == "_nf_customer_id" else c
        for c in _orders_table()["columns"]
    ]
    with pytest.raises(CodeTableRefused, match="required by the function exactly when"):
        check_table(_orders_table(columns=loosened), manifest)


def test_register_table_suggests_the_declared_columns_and_the_parameters(manifest):
    export = manifest.export("orders")
    suggested = declared_columns(manifest, export)
    assert [
        (c["name"], c["data_type"], c["native_filter_type"], c["native_filter_required"])
        for c in suggested
    ] == [
        ("id", "bigint", None, None),
        ("placed", "timestamp", None, None),
        ("due", "date", None, None),
        ("total", "numeric", None, None),
        ("tier", "text", None, None),
        ("lines", "json", None, None),
        ("note", "text", None, None),
        ("_nf_customer_id", "text", "query_arg", True),
        ("_nf_since", "timestamp", "query_arg", False),
    ]
    # Exactly what a registration of the suggestion passes.
    check_table(_orders_table(), manifest)


# -- reading ------------------------------------------------------------------------------------


def test_arguments_go_in_declared_order_and_one_left_out_is_absent(manifest):
    args, names = code_table.call_arguments(
        _orders_table(), manifest.export("orders"), {"customer_id": "acme"}
    )
    assert names == ["customer_id", "since"]
    assert args == ["acme", w.ABSENT]
    assert code_table.cache_arguments(_orders_table(), {"customer_id": "acme"}) == {
        "customer_id": "acme"
    }


def test_a_code_table_is_read_by_provisa_and_landed():
    from provisa.api.data.materialization import is_native_read

    types = {"orders-src": "python", "pg": "postgresql"}
    state = SimpleNamespace(source_types=types, federation_engine=None)
    assert is_native_read(_orders_table(), types, state)
    assert not is_native_read({**_orders_table(), "source_id": "pg"}, types, state)


@pytest.fixture
def prepared(tmp_path, manifest, monkeypatch):
    """The source prepared: the fixture code served by the real Python host."""
    code = tmp_path / "code"
    shutil.copytree(FIXTURE, code)
    command = w.HostCommand(
        argv=lambda address: [sys.executable, "-I", str(HOST), address, str(code), "orders"],
        env=w.private_environment(PYTHONDONTWRITEBYTECODE="1"),
        cwd=code,
    )
    ready = code_source.Prepared("orders-src", "h1", manifest, command, "acme")
    source = SimpleNamespace(id="orders-src", type=SimpleNamespace(value="python"), mapping={})

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
    w.pool.stop_source("orders-src")


async def test_a_read_calls_the_function_and_checks_its_rows(prepared):
    since = datetime(2025, 12, 1, tzinfo=UTC)
    answer = await code_table.read(
        None, _orders_table(), {"customer_id": "acme", "since": since.isoformat()}
    )
    [row] = answer.rows
    assert row["id"] == 2**60 and row["total"] == Decimal("12.50") and row["tier"] == "gold"
    assert row["placed"] == datetime(2026, 1, 2, 3, 4, 5, tzinfo=UTC)
    assert row["note"] == since.isoformat()
    assert row["lines"] == [{"sku": "acme", "qty": 2}]
    assert ("total", "numeric") in answer.columns


async def test_a_function_the_code_no_longer_offers_is_refused_at_read(prepared):
    with pytest.raises(CodeTableRefused, match="no @table function 'gone'"):
        await code_table.read(None, _orders_table(table_name="gone"), {"customer_id": "acme"})


# -- saving the source --------------------------------------------------------------------------


async def test_saving_a_code_source_records_the_content_hash_of_its_code(monkeypatch):
    from provisa.api.admin import _code_source_registration as registration

    seen = {}

    async def _prepare(source_id, source_type, mapping, ctx):
        seen["mapping"] = dict(mapping)
        return SimpleNamespace(content_hash="fresh-hash")

    async def _context(_state):
        return None

    monkeypatch.setattr(code_source, "prepare", _prepare)
    monkeypatch.setattr(code_source, "context_for", _context)
    mapping = {"code": {"kind": "upload", "upload": "u1"}, "content_hash": "stale"}
    assert await registration.record_code("orders-src", "python", mapping) is None
    assert "content_hash" not in seen["mapping"]  # pinned afresh, not checked against the old
    assert mapping["content_hash"] == "fresh-hash"
    assert await registration.record_code("pg", "postgresql", {}) is None


async def test_code_that_cannot_be_used_refuses_the_save_by_name(monkeypatch):
    from provisa.api.admin import _code_source_registration as registration

    async def _prepare(*_args):
        raise BundleRefused("no_lockfile", "the code declares dependencies but carries no uv.lock")

    async def _context(_state):
        return None

    monkeypatch.setattr(code_source, "prepare", _prepare)
    monkeypatch.setattr(code_source, "context_for", _context)
    refused = await registration.record_code(
        "orders-src", "python", {"code": {"kind": "upload", "upload": "u"}}
    )
    assert refused is not None and not refused.success
    assert refused.code == "codesource.no_lockfile"
