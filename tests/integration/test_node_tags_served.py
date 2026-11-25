# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Every tag a TypeScript code source carries, read and served (REQ-1984, REQ-2100, REQ-2221,
REQ-2226, REQ-2249): one small bundle -- JavaScript plus the TypeScript declarations that carry
its tags -- is read by Provisa's own reader under the pinned Node runtime and TypeScript compiler,
and each tag that no other Node test serves is then called on the Node host: a @loader table
through Provisa's read of a join's keys, a @query given a governed table, the @insert, @update and
@delete commands bound to a code table, a @change function given its resolver's tables and a
cursor, and, on the front end every built image runs, @on-start given the @requires values, @health
and @on-stop."""

# Requirements: REQ-1984, REQ-2100, REQ-2221, REQ-2226, REQ-2249
from __future__ import annotations

import json
import os
import subprocess  # nosec B404 - argv-list invocation of the front end
import sys
import threading
import time
from pathlib import Path
from types import SimpleNamespace

import pytest

from provisa.codehost import front
from provisa.codesource import remote
from provisa.codesource import source as code_source
from provisa.codesource import table as code_table
from provisa.codesource import worker as w
from provisa.codesource.arrow_rows import row_schema, schema_text
from provisa.codesource.manifest import (
    HEALTH,
    ON_START,
    ON_STOP,
    Change,
    Requirement,
    manifest_json,
)
from provisa.core import request_deadline
from provisa.node.reader import ensure_typescript, read_bundle
from provisa.node.worker import HOST, host_command
from provisa.runtime_deps.code_runtimes import CodeRuntimeResolver

pytestmark = [pytest.mark.integration]

SOURCE = "shop-node"

DECLARATIONS = """
import type { ReadContext, TableRef } from "@provisa/services";

export interface Account { account_id: string; batch: number; call: number; }
export interface AccountKey { account_id?: string; }
export interface Store { id: number; region: string; }
export interface Region { code: string; name: string; }
export interface Order { id: number; status: string; }
export interface Changed { store_id: number; region_id: string; removed: boolean; seq: number; }

/**
 * Accounts, looked up by a join's keys two at a time.
 * @table
 * @loader 2
 * @requires REGION
 * @requires API_KEY:secret
 */
export declare function accounts(
  inputs: AccountKey,
  context: ReadContext,
): Promise<Account[] | Record<string, Account[]>>;

/** The stores in one region. @query */
export declare function storesInRegion(stores: TableRef<Store>, region: string): Promise<Store[]>;

/**
 * @command
 * @insert orders
 */
export declare function addOrders(rows: Order[]): Promise<number>;

/**
 * @command
 * @update orders
 */
export declare function changeOrders(rows: Order[]): Promise<number>;

/**
 * @command
 * @delete orders
 */
export declare function removeOrders(rows: Order[]): Promise<number>;

/**
 * Store-region matches changed since `since`.
 * @change table=store_regions
 * @change key=store_id,region_id
 * @change removed=removed
 * @change cursor=seq
 */
export declare function storeRegionChanges(
  stores: TableRef<Store>,
  regions: TableRef<Region>,
  since?: number,
): Promise<Changed[]>;

/** @on-start */
export declare function setup(values?: Record<string, string>): Promise<void>;

/** @on-stop */
export declare function teardown(): Promise<void>;

/** @health */
export declare function status(): string;
"""

CODE = """
import { appendFileSync, readFileSync } from "node:fs";

let calls = 0;
let given = null;

function note(what) {
  if (process.env.LIFECYCLE_TRAIL) appendFileSync(process.env.LIFECYCLE_TRAIL, what + "\\n");
}

export async function accounts(inputs, context) {
  calls += 1;
  if (context.keys === null) return [{ account_id: inputs.account_id ?? null, batch: 0, call: calls }];
  const out = {};
  for (const [key, k] of Object.entries(context.keys)) {
    out[key] = [{ account_id: k.account_id, batch: Object.keys(context.keys).length, call: calls }];
  }
  return out;
}

export async function storesInRegion(stores, region) {
  return [{ id: stores.columns.length, region: `${stores.kind}:${stores.ref}:${region}` }];
}

export async function addOrders(rows) { return rows.length; }
export async function changeOrders(rows) { return rows.filter((r) => r.status !== "").length; }
export async function removeOrders(rows) { return -rows.length; }

export async function storeRegionChanges(stores, regions, since) {
  return [{ store_id: 1, region_id: `${stores.ref}|${regions.ref}`, removed: since === undefined, seq: (since ?? 0) + 1 }];
}

export async function setup(values) {
  given = values;
  note("start " + JSON.stringify(values, Object.keys(values ?? {}).sort()));
}

export async function teardown() { note("stop"); }

export function status() {
  note("health");
  return readFileSync(process.env.LIFECYCLE_HEALTH, "utf8").trim() + (given ? "" : " (not set up)");
}
"""


@pytest.fixture(autouse=True)
def _a_call_is_bounded():
    with request_deadline.within(60):
        yield


@pytest.fixture(scope="module")
def cache(tmp_path_factory):
    return tmp_path_factory.mktemp("runtime-deps")


@pytest.fixture(scope="module")
def node(cache):
    return CodeRuntimeResolver(cache_root=cache).resolve("node")


@pytest.fixture(scope="module")
def host_lib(cache):
    return CodeRuntimeResolver(cache_root=cache).resolve("apache-arrow").parent.parent


@pytest.fixture(scope="module")
def bundle(tmp_path_factory):
    root = tmp_path_factory.mktemp("bundle")
    (root / "package.json").write_text(
        json.dumps({"name": "shop", "type": "module", "main": "index.js", "types": "index.d.ts"})
    )
    (root / "index.d.ts").write_text(DECLARATIONS)
    (root / "index.js").write_text(CODE)
    return root


@pytest.fixture(scope="module")
def manifest(cache, node, bundle):
    compiler = ensure_typescript(cache, [])
    return read_bundle(runtime="node", executable=node, compiler=compiler, bundle=bundle)


@pytest.fixture
def prepared(manifest, node, bundle, host_lib, monkeypatch):
    """The bundle as Provisa runs it in dev mode: its manifest and its sandboxed Node host."""
    command = host_command(
        runtime="node", executable=node, bundle=bundle, egress=[], host_lib=host_lib
    )
    ready = code_source.Prepared(SOURCE, "h1", manifest, command, "acme")
    source = SimpleNamespace(id=SOURCE, type=SimpleNamespace(value="node"), mapping={})

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
    w.pool.stop_source(SOURCE)


def _table_ref(ref: str, columns: list[str]) -> dict:
    return {"kind": "table_ref", "ref": ref, "columns": columns, "sql": f"SELECT * FROM {ref}"}


# -- read: every tag reaches the manifest ----------------------------------------------------------


def test_every_tag_is_read_into_the_manifest(manifest):
    kinds = {e.name: e.kind for e in manifest.exports}
    assert kinds == {
        "accounts": "table",
        "storesInRegion": "query",
        "addOrders": "command",
        "changeOrders": "command",
        "removeOrders": "command",
        "storeRegionChanges": "change",
        "setup": ON_START,
        "teardown": ON_STOP,
        "status": HEALTH,
    }
    accounts = manifest.export("accounts")
    assert accounts.loader and accounts.loader_batch == 2
    assert accounts.takes == ("inputs", "context")
    assert manifest.requires == (Requirement("REGION"), Requirement("API_KEY", secret=True))
    assert manifest.export("addOrders").binds == (("insert", "orders"),)
    assert manifest.export("changeOrders").binds == (("update", "orders"),)
    assert manifest.export("removeOrders").binds == (("delete", "orders"),)
    assert manifest.export("storeRegionChanges").change == Change(
        "store_regions", ("store_id", "region_id"), "removed", "seq"
    )
    assert [p.name for p in manifest.export("storesInRegion").parameters] == ["stores", "region"]


# -- served on the dev-mode Node host --------------------------------------------------------------


def _accounts_table() -> dict:
    def column(name, data_type, kind=None):
        return {
            "column_name": name,
            "name": name,
            "data_type": data_type,
            "path": None,
            "native_filter_type": kind,
            "native_filter_required": False if kind else None,
        }

    return {
        "table_name": "accounts",
        "source_id": SOURCE,
        "columns": [
            column("account_id", "text"),
            column("batch", "double"),
            column("call", "double"),
            column("_nf_account_id", "text", kind="query_arg"),
        ],
    }


async def test_a_loader_answers_a_joins_keys_in_batches_of_its_declared_size(prepared):
    sets = [{"account_id": str(i)} for i in range(5)]
    read = await code_table.read_each(None, _accounts_table(), sets, joined=True)
    rows = read.rows
    assert sorted(r["account_id"] for r in rows) == ["0", "1", "2", "3", "4"]
    assert sorted(r["batch"] for r in rows) == [1, 2, 2, 2, 2]
    assert len({r["call"] for r in rows}) == 3


def test_a_query_is_given_its_governed_table(prepared):
    manifest = prepared.manifest
    query = manifest.export("storesInRegion")
    rows = prepared.worker().command(
        "storesInRegion",
        [_table_ref("main.public.stores", ["id", "region"]), "emea"],
        ["stores", "region"],
        schema=schema_text(row_schema(manifest, query)),
    )
    assert rows == [{"id": 2, "region": "table_ref:main.public.stores:emea"}]


@pytest.mark.parametrize(
    ("command", "answer"), [("addOrders", 2), ("changeOrders", 1), ("removeOrders", -2)]
)
def test_each_write_command_is_given_the_rows(prepared, command, answer):
    rows = [{"id": 1, "status": "open"}, {"id": 2, "status": ""}]
    assert prepared.worker().command(command, [rows], ["rows"]) == answer


def test_a_change_function_is_given_its_tables_and_the_cursor(prepared):
    manifest = prepared.manifest
    change = manifest.export("storeRegionChanges")
    schema = schema_text(row_schema(manifest, change))
    stores = _table_ref("main.public.stores", ["id", "region"])
    regions = _table_ref("main.public.regions", ["code", "name"])
    names = ["stores", "regions", "since"]
    first = prepared.worker().command(
        "storeRegionChanges", [stores, regions, w.ABSENT], names, schema=schema
    )
    assert first == [
        {
            "store_id": 1,
            "region_id": "main.public.stores|main.public.regions",
            "removed": True,
            "seq": 1,
        }
    ]
    later = prepared.worker().command(
        "storeRegionChanges", [stores, regions, 41], names, schema=schema
    )
    assert [(r["removed"], r["seq"]) for r in later] == [(False, 42)]


# -- served on the front end of a built image ------------------------------------------------------

BOUNDS = {"memory_mb": 256, "threads": 2, "spill_cap_mb": 1024}
GIVEN = {"API_KEY": "sk-test", "REGION": "eu"}


class FrontHost:
    """The front end every built image runs, its worker the Node host on the bundle."""

    def __init__(self, tmp_path: Path, node: Path, bundle: Path, host_lib: Path, manifest) -> None:
        self.trail = tmp_path / "trail.txt"
        self.trail.write_text("")
        health = tmp_path / "health.txt"
        health.write_text("ok")
        names = tmp_path / "names.json"
        names.write_text("{}")
        manifest_file = tmp_path / "manifest.json"
        manifest_file.write_text(json.dumps(manifest_json(manifest)))
        self.exports = sorted(e.name for e in manifest.exports)
        env = {
            **os.environ,
            "PROVISA_CODE_HASH": "h1",
            "PROVISA_HOST_LIB": str(host_lib),
            "PROVISA_MANIFEST_FILE": str(manifest_file),
            "PROVISA_DISCOVERED": "{}",
            "LIFECYCLE_TRAIL": str(self.trail),
            "LIFECYCLE_HEALTH": str(health),
        }
        self.process = subprocess.Popen(  # noqa: S603 -- the test's interpreter, Provisa's front
            [
                sys.executable,
                front.__file__,
                "0",
                str(tmp_path / "state"),
                str(tmp_path / "spill"),
                str(names),
                *[str(node), str(HOST), "{fd}", str(bundle / "index.js")],
            ],
            env=env,
            stderr=subprocess.PIPE,
            text=True,
        )
        assert self.process.stderr is not None
        self.port, self.pairing = 0, ""
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            line = self.process.stderr.readline()
            if line.startswith("pairing code: "):
                self.pairing = line.removeprefix("pairing code: ").strip()
            if line.startswith("remote host listening on "):
                self.port = int(line.rsplit(" ", 1)[1])
                break
        assert self.port and self.pairing, "the front end did not start"
        threading.Thread(target=self.process.stderr.read, daemon=True).start()

    def said(self) -> list[str]:
        return [line for line in self.trail.read_text().splitlines() if line]


@pytest.fixture
def front_host(tmp_path, node, bundle, host_lib, manifest):
    started = FrontHost(tmp_path, node, bundle, host_lib, manifest)
    yield started
    if started.process.poll() is None:
        started.process.terminate()
        started.process.wait(timeout=20)


def test_on_start_is_given_the_required_values_health_answers_and_on_stop_runs(front_host):
    identity = remote.new_identity()
    pin = remote.pair("127.0.0.1", front_host.port, front_host.pairing, identity, BOUNDS).pin
    worker = remote.RemoteWorker(
        SOURCE,
        "h1",
        address="127.0.0.1",
        port=front_host.port,
        pin=pin,
        identity=identity,
        exports=front_host.exports,
        setup=lambda: GIVEN,
    )
    worker.start()
    try:
        worker.ready()
        assert front_host.said() == ['start {"API_KEY":"sk-test","REGION":"eu"}']
        assert worker.health() == {"status": "ok"}
        assert front_host.said()[-1] == "health"
    finally:
        worker.stop()
    front_host.process.terminate()
    front_host.process.wait(timeout=20)
    assert front_host.said()[-1] == "stop"
