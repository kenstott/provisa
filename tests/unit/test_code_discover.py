# Copyright (c) 2026 Kenneth Stott
# Canary: 5f0e2c4b-8a1d-4f37-9b6e-2d7c9a4e1b80
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""@discover in a Python code source (REQ-2267, v0): one function, called when the code is read,
returns generated tables, queries and commands, each with its row type; they are recorded as
hand-written @table, @query and @command functions are, and one handler per kind serves each,
called with the item's name. A generated table may batch a join's keys under the @loader contract
(REQ-2100). A name a generated item shares with another item is refused by name. The worker is a
real Python host process (unsandboxed)."""

# Requirements: REQ-2267, REQ-2100
from __future__ import annotations

import json
import subprocess
import sys
import textwrap
from types import SimpleNamespace

import pytest

from provisa.codesource import source as code_source
from provisa.codesource import table as code_table
from provisa.codesource import worker as w
from provisa.codesource.arrow_rows import row_schema, schema_text
from provisa.codesource.manifest import (
    NODE_RULES,
    PYTHON_RULES,
    ManifestRefused,
    Named,
    TableOf,
    discovered_handlers,
    manifest_from_json,
    manifest_json,
    read_manifest,
)
from provisa.python.source import HOST, READER

pytestmark = pytest.mark.unit

CODE = '''
from dataclasses import dataclass, field, make_dataclass

from provisa_services import (
    Discovered,
    DiscoveredCommand,
    DiscoveredQuery,
    DiscoveredTable,
    ReadContext,
)


@dataclass
class Order:
    id: int
    status: str


def orders(context: ReadContext) -> list[Order]:
    """Hand-written beside the generated items.

    @table
    """
    return [Order(1, "open")]


# Row types made at discovery, as a wrapped API's would be.
Customer = make_dataclass("Customer", [("id", int), ("name", str)])
CustomerInputs = make_dataclass(
    "CustomerInputs", [("region", "str | None", field(default=None))]
)
Account = make_dataclass("Account", [("account_id", str), ("batch", int), ("call", int)])
AccountKey = make_dataclass("AccountKey", [("account_id", "str | None", field(default=None))])
StatusParams = make_dataclass("StatusParams", [("status", str)])
CloseParams = make_dataclass("CloseParams", [("order_id", int)])

_calls = 0


def discover() -> Discovered:
    """@discover"""
    return Discovered(
        tables=[
            DiscoveredTable(
                "customers", row=Customer, inputs=CustomerInputs, description="Every customer"
            ),
            DiscoveredTable("accounts", row=Account, inputs=AccountKey, loader_batch=2),
        ],
        queries=[
            DiscoveredQuery("orders_by_status", table="orders", parameters=StatusParams),
            DiscoveredQuery("customers_by_status", table="customers", parameters=StatusParams),
        ],
        commands=[DiscoveredCommand("close_order", parameters=CloseParams, returns=str)],
        table_handler=serve_table,
        query_handler=serve_query,
        command_handler=serve_command,
    )


def serve_table(name, inputs, context):
    global _calls
    _calls += 1
    if name == "customers":
        return [{"id": 1, "name": f"{name}:{inputs.get('region')}"}]
    if context.keys is None:
        return [{"account_id": inputs.get("account_id"), "batch": 0, "call": _calls}]
    return {
        key: [{"account_id": k["account_id"], "batch": len(context.keys), "call": _calls}]
        for key, k in context.keys.items()
    }


def serve_query(name, arguments):
    return [{"id": 7, "status": f"{name}:{arguments['status']}"}]


def serve_command(name, arguments):
    return f"{name}:{arguments['order_id']}"
'''


def _read(tmp_path, source: str, entry: str = "shop") -> dict:
    (tmp_path / f"{entry}.py").write_text(textwrap.dedent(source))
    done = subprocess.run(
        [sys.executable, "-I", str(READER), str(tmp_path), entry], capture_output=True, text=True
    )
    assert done.returncode == 0, done.stderr
    return json.loads(done.stdout)


def _problems(tmp_path, source: str) -> list[str]:
    with pytest.raises(ManifestRefused) as refused:
        read_manifest(_read(tmp_path, source), PYTHON_RULES)
    return refused.value.problems


@pytest.fixture
def prepared(tmp_path, monkeypatch):
    code = tmp_path / "code"
    code.mkdir()
    manifest = read_manifest(_read(code, CODE), PYTHON_RULES)
    command = w.HostCommand(
        argv=lambda address: [sys.executable, "-I", str(HOST), address, str(code), "shop"],
        env=w.private_environment(PYTHONDONTWRITEBYTECODE="1"),
        cwd=code,
        discovered=discovered_handlers(manifest),
    )
    ready = code_source.Prepared("shop-src", "h1", manifest, command, "acme")
    source = SimpleNamespace(id="shop-src", type=SimpleNamespace(value="python"), mapping={})

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
    w.pool.stop_source("shop-src")


def _column(name, data_type, *, kind=None):
    return {
        "column_name": name,
        "name": name,
        "data_type": data_type,
        "path": None,
        "native_filter_type": kind,
        "native_filter_required": False if kind else None,
    }


def _customers() -> dict:
    columns = [
        _column("id", "bigint"),
        _column("name", "text"),
        _column("_nf_region", "text", kind="query_arg"),
    ]
    return {"table_name": "customers", "source_id": "shop-src", "columns": columns}


def _accounts() -> dict:
    columns = [
        _column("account_id", "text"),
        _column("batch", "bigint"),
        _column("call", "bigint"),
        _column("_nf_account_id", "text", kind="query_arg"),
    ]
    return {"table_name": "accounts", "source_id": "shop-src", "columns": columns}


# -- discovery records what @discover generates ----------------------------------------------------


def test_discovery_records_the_generated_tables_queries_and_commands(prepared):
    manifest = prepared.manifest
    kinds = {e.name: (e.kind, e.handler) for e in manifest.exports}
    assert kinds == {
        "orders": ("table", None),
        "discover": ("discover", None),
        "customers": ("table", "serve_table"),
        "accounts": ("table", "serve_table"),
        "orders_by_status": ("query", "serve_query"),
        "customers_by_status": ("query", "serve_query"),
        "close_order": ("command", "serve_command"),
    }
    customers = manifest.export("customers")
    assert customers.description == "Every customer"
    assert customers.takes == ("inputs", "context")
    assert [(p.name, p.required) for p in customers.parameters] == [("region", False)]
    assert [f.name for f in manifest.types["Customer"].fields] == ["id", "name"]
    # A query returns the rows of the table it names, hand-written or generated.
    by_status = manifest.export("orders_by_status")
    assert by_status.many and by_status.returns == manifest.export("orders").returns
    assert manifest.export("customers_by_status").returns == customers.returns
    close = manifest.export("close_order")
    assert [p.name for p in close.parameters] == ["order_id"] and not close.many
    assert discovered_handlers(manifest) == {
        "customers": "serve_table",
        "accounts": "serve_table",
        "orders_by_status": "serve_query",
        "customers_by_status": "serve_query",
        "close_order": "serve_command",
    }


def test_the_generated_items_are_kept_with_a_built_source_and_get_name_tables(prepared):
    from provisa.codesource.discovery import declared_columns
    from provisa.codesource.names import name_tables

    manifest = prepared.manifest
    kept = manifest_from_json(manifest_json(manifest))
    assert kept == manifest
    tables = name_tables(manifest)
    assert set(tables) == {"orders", "customers", "accounts"}
    assert [c["name"] for c in tables["customers"]["columns"]] == ["id", "name"]
    assert [p["name"] for p in tables["customers"]["parameters"]] == ["_nf_region"]
    names = [c["name"] for c in declared_columns(manifest, manifest.export("customers"))]
    assert names == ["id", "name", "_nf_region"]
    code_table.check_table(_customers(), manifest)


def test_the_hosts_hello_offers_the_generated_items_and_not_the_handlers(prepared):
    worker = prepared.worker()
    assert sorted(worker.exports) == sorted(e.name for e in prepared.manifest.exports)


# -- one handler per kind, called with the item's name ---------------------------------------------


async def test_a_generated_table_is_read_through_its_handler_called_with_its_name(prepared):
    read = await code_table.read(None, _customers(), {"region": "EU"})
    assert [(r["id"], r["name"]) for r in read.rows] == [(1, "customers:EU")]


def test_a_generated_query_and_command_are_called_through_their_handlers_by_name(prepared):
    manifest = prepared.manifest
    worker = prepared.worker()
    query = manifest.export("orders_by_status")
    rows = worker.command(
        "orders_by_status",
        ["open"],
        ["status"],
        schema=schema_text(row_schema(manifest, query)),
    )
    assert rows == [{"id": 7, "status": "orders_by_status:open"}]
    assert worker.command("close_order", [42], ["order_id"]) == "close_order:42"


def test_a_hand_written_table_beside_discover_is_called_as_before(prepared):
    manifest = prepared.manifest
    export = manifest.export("orders")
    batches = list(
        prepared.worker().rows(
            "orders",
            "table",
            [],
            [],
            schema=schema_text(row_schema(manifest, export)),
            takes=export.takes,
            context={"filters": [], "order_by": [], "limit": None, "columns": []},
        )
    )
    assert [r for b in batches for r in b] == [{"id": 1, "status": "open"}]


# -- a generated table's loader batches under the @loader contract ---------------------------------


async def test_a_generated_tables_loader_batches_the_join_keys_by_its_declared_size(prepared):
    accounts = prepared.manifest.export("accounts")
    assert accounts.loader and accounts.loader_batch == 2
    sets = [{"account_id": str(i)} for i in range(5)]
    read = await code_table.read_each(None, _accounts(), sets, joined=True)
    rows = read.rows
    assert sorted(r["account_id"] for r in rows) == ["0", "1", "2", "3", "4"]
    assert sorted(r["batch"] for r in rows) == [1, 2, 2, 2, 2]
    assert len({r["call"] for r in rows}) == 3


# -- names -----------------------------------------------------------------------------------------


def test_a_generated_item_named_as_a_hand_written_one_is_refused_by_name(tmp_path):
    source = CODE.replace(
        'DiscoveredTable(\n                "customers"',
        'DiscoveredTable(\n                "orders"',
    )
    assert '"orders", row=Customer' in source
    problems = _problems(tmp_path, source)
    assert (
        "orders: generated by @discover and also a hand-written function; every table, query "
        "and command has a name of its own"
    ) in problems


def test_two_generated_items_of_one_name_are_refused_by_name(tmp_path):
    source = CODE.replace('DiscoveredCommand("close_order"', 'DiscoveredCommand("accounts"')
    problems = _problems(tmp_path, source)
    assert (
        "accounts: generated by @discover twice; every table, query and command has a name of "
        "its own"
    ) in problems


# -- what @discover must say -----------------------------------------------------------------------


@pytest.mark.parametrize(
    "change, problem",
    [
        (
            ("        table_handler=serve_table,\n", ""),
            "discover: it generates tables and names no table_handler",
        ),
        (
            ('table="orders", parameters', 'table="nowhere", parameters'),
            "orders_by_status: its table nowhere is no table of the code's",
        ),
        (
            ("        command_handler=serve_command,\n", "        command_handler=print,\n"),
            "discover: its command_handler is no function of the code's own module",
        ),
        (
            (
                '    """@discover"""\n    return',
                '    """@discover"""\n    raise RuntimeError("down")\n    return',
            ),
            "discover: @discover failed: RuntimeError: down",
        ),
    ],
)
def test_what_discover_must_say_is_refused_by_name(tmp_path, change, problem):
    old, new = change
    assert old in CODE
    problems = _problems(tmp_path, CODE.replace(old, new))
    assert any(problem in p for p in problems), problems


def test_a_second_discover_function_is_refused_by_name(tmp_path):
    source = (
        CODE
        + '\n\ndef discover_more() -> Discovered:\n    """@discover"""\n    return Discovered()\n'
    )
    problems = _problems(tmp_path, source)
    assert any("@discover is on discover already" in p for p in problems), problems


def test_discover_is_refused_outside_python_by_name():
    raw = {
        "exports": [
            {
                "name": "discover",
                "tags": ["discover"],
                "tag_arguments": [],
                "description": "",
                "signatures": [{"parameters": [], "returns": {"kind": "any"}}],
            }
        ],
        "types": {},
    }
    with pytest.raises(ManifestRefused) as refused:
        read_manifest(raw, NODE_RULES)
    assert "discover: @discover is read from a Python code source only" in refused.value.problems


# -- a TableRef parameter on a generated query or command (v0) -------------------------------------


def _with_table_parameter(source: str) -> str:
    imported = source.replace("    ReadContext,\n)", "    ReadContext,\n    TableRef,\n)")
    changed = imported.replace(
        'CloseParams = make_dataclass("CloseParams", [("order_id", int)])',
        'CloseParams = make_dataclass("CloseParams", [("order_id", int), ("orders", TableRef[Order])])',
    ).replace(
        'StatusParams = make_dataclass("StatusParams", [("status", str)])',
        'StatusParams = make_dataclass("StatusParams", [("status", str), ("rows", TableRef[Order])])',
    )
    assert changed.count("TableRef") == 3, changed
    return changed


def test_a_generated_command_taking_a_table_ref_is_refused_by_name(tmp_path):
    problems = _problems(tmp_path, _with_table_parameter(CODE))
    assert (
        "close_order: its parameter orders is a TableRef, and a generated command takes no "
        "TableRef parameter in v0"
    ) in problems
    assert (
        "orders_by_status: its parameter rows is a TableRef, and a generated query takes no "
        "TableRef parameter in v0"
    ) in problems


def test_a_generated_command_taking_scalars_is_recorded_beside_a_hand_written_table_ref(tmp_path):
    source = CODE.replace("    ReadContext,\n)", "    ReadContext,\n    TableRef,\n)") + (
        "\n\ndef archive(orders: TableRef[Order]) -> str:\n"
        '    """@command"""\n'
        '    return "archived"\n'
    )
    manifest = read_manifest(_read(tmp_path, source), PYTHON_RULES)
    close = manifest.export("close_order")
    assert [(p.name, p.required) for p in close.parameters] == [("order_id", True)]
    archive = manifest.export("archive")
    assert archive.kind == "command"
    assert archive.handler is None
    assert archive.parameters[0].shape == TableOf(Named("Order"))
