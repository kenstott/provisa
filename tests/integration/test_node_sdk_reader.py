# Copyright (c) 2026 Kenneth Stott
# Canary: e24e36a0-b319-40f7-aed3-969d3e1950a5
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The optional TypeScript SDK (@provisa/sdk) read by the real reader with the pinned TypeScript
(REQ-1984): code written with the SDK's wrappers is compiled to declarations by the pinned tsc,
and the reader reads each wrapper's brand as the JSDoc tags its twin carries, the SDK's aliases as
what they stand for; a non-literal option and a newer spelling are refused by name; and the pinned
tsc rejects the shapes the wrappers forbid."""

# Requirements: REQ-1984
from __future__ import annotations

import json
import re
import subprocess  # nosec B404 - argv-list invocation of the pinned runtime and compiler
from pathlib import Path

import pytest

from provisa.codesource.manifest import ManifestRefused, manifest_json
from provisa.node.reader import ensure_typescript, read_bundle
from provisa.runtime_deps.code_runtimes import CodeRuntimeResolver

pytestmark = [pytest.mark.integration]

SDK_PACKAGE = Path(__file__).resolve().parents[2] / "provisa-sdk" / "typescript"

SDK_SOURCE = """
import {
  table, stream, command, query, columns, onStart, onStop, health,
  type ReadContext, type Rows, type AsyncRows, type Batch, type Text, type Double, type Timestamp,
} from "@provisa/sdk";
export interface Risk {
  /** How likely the account leaves. */
  score: Double;
  reasons: Text[];
  at: Timestamp;
}
export interface RiskInputs { account_id: string; }
/** Churn risk of one account. */
export const churnRisk = table(async function churnRisk(inputs: RiskInputs): Promise<Risk[]> {
  return [];
});
/** Scored in batches. */
export const scored = table(
  async (inputs: RiskInputs, context: ReadContext): Promise<Risk[]> => [],
  { loader: 100, requires: ["API_KEY:secret", "REGION"] },
);
export const every = table((context: ReadContext): Rows<Risk> => [], { loader: true });
export const later = table(async function* (inputs: RiskInputs): AsyncRows<Risk> {});
export const feed = stream(async function* (inputs: RiskInputs): AsyncGenerator<Risk> {});
export const rescore = command(async (account_id: string): Promise<void> => {});
export const ask = query(async (account_id: string): Promise<Risk[]> => []);
export const upper = columns((names: string[]): Batch<{ name: string }> =>
  names.map((name) => ({ name })),
);
export const connect = onStart(async (values: Record<string, string>) => {});
export const close = onStop(() => {});
export const check = health(async () => "ok");
"""

#: The same code with JSDoc tags and the types written out, as declarations.
JSDOC_TWIN = """
import type { ReadContext } from "@provisa/services";
export interface Risk {
  /** How likely the account leaves. */
  score: number;
  reasons: string[];
  at: Date;
}
export interface RiskInputs { account_id: string; }
/** Churn risk of one account.
 * @table
 */
export declare function churnRisk(inputs: RiskInputs): Promise<Risk[]>;
/** Scored in batches.
 * @table
 * @loader 100
 * @requires API_KEY:secret
 * @requires REGION
 */
export declare function scored(inputs: RiskInputs, context: ReadContext): Promise<Risk[]>;
/** @table
 * @loader
 */
export declare function every(context: ReadContext): Iterable<Risk>;
/** @table */
export declare function later(inputs: RiskInputs): AsyncIterable<Risk>;
/** @stream */
export declare function feed(inputs: RiskInputs): AsyncGenerator<Risk>;
/** @command */
export declare function rescore(account_id: string): Promise<void>;
/** @query */
export declare function ask(account_id: string): Promise<Risk[]>;
/** @columns */
export declare function upper(names: string[]): { name: string }[];
/** @on-start */
export declare function connect(values: Record<string, string>): Promise<void>;
/** @on-stop */
export declare function close(): void;
/** @health */
export declare function check(): Promise<string>;
"""


@pytest.fixture(scope="module")
def cache(tmp_path_factory):
    return tmp_path_factory.mktemp("runtime-deps")


@pytest.fixture(scope="module")
def compiler(cache):
    return ensure_typescript(cache, [])


@pytest.fixture(scope="module")
def node(cache):
    return CodeRuntimeResolver(cache_root=cache).resolve("node")


def _project(root: Path, source: str) -> Path:
    """An author's TypeScript project using the SDK package from this repository."""
    (root / "src").mkdir(parents=True)
    (root / "src" / "index.ts").write_text(source)
    (root / "tsconfig.json").write_text(
        json.dumps(
            {
                "compilerOptions": {
                    "target": "ES2022",
                    "module": "NodeNext",
                    "moduleResolution": "NodeNext",
                    "strict": True,
                    "declaration": True,
                    "skipLibCheck": True,
                    "rootDir": "src",
                    "outDir": "dist",
                    "paths": {"@provisa/sdk": [str(SDK_PACKAGE / "index.d.ts")]},
                },
                "include": ["src"],
            }
        )
    )
    return root


def _tsc(node: Path, compiler: Path, project: Path) -> subprocess.CompletedProcess:
    tsc = compiler.parent.parent / "bin" / "tsc"
    return subprocess.run(  # nosec B603 - the pinned runtime and compiler, no shell
        [str(node), str(tsc), "-p", str(project), "--pretty", "false"],
        capture_output=True,
        text=True,
    )


def _compiled(node: Path, compiler: Path, root: Path, source: str) -> Path:
    project = _project(root, source)
    done = _tsc(node, compiler, project)
    assert done.returncode == 0, done.stdout + done.stderr
    return project / "dist"


def _read(node: Path, compiler: Path, bundle: Path):
    return read_bundle(runtime="node", executable=node, compiler=compiler, bundle=bundle)


def test_each_wrapper_reads_as_the_jsdoc_tags_of_its_twin(node, compiler, tmp_path):
    sdk = _compiled(node, compiler, tmp_path / "sdk", SDK_SOURCE)
    twin = tmp_path / "twin"
    twin.mkdir()
    (twin / "index.d.ts").write_text(JSDOC_TWIN)
    read = _read(node, compiler, sdk)
    assert manifest_json(read) == manifest_json(_read(node, compiler, twin))
    assert {e.name: e.kind for e in read.exports} == {
        "churnRisk": "table",
        "scored": "table",
        "every": "table",
        "later": "table",
        "feed": "stream",
        "rescore": "command",
        "ask": "query",
        "upper": "columns",
        "connect": "on-start",
        "close": "on-stop",
        "check": "health",
    }
    scored = read.export("scored")
    assert (scored.loader, scored.loader_batch) == (True, 100)
    assert read.export("every").loader and read.export("every").loader_batch is None
    assert [(r.name, r.secret) for r in read.requires] == [("API_KEY", True), ("REGION", False)]
    assert read.export("churnRisk").description == "Churn risk of one account."


def test_the_sdk_aliases_read_as_what_they_stand_for(node, compiler, tmp_path):
    raw = _raw(node, compiler, _compiled(node, compiler, tmp_path, SDK_SOURCE))
    returns = {e["name"]: e["signatures"][0]["returns"] for e in raw["exports"] if e["signatures"]}
    risk = {"kind": "ref", "name": "Risk"}
    assert returns["every"] == {
        "kind": "iterable",
        "async": False,
        "generator": False,
        "element": risk,
    }
    assert returns["later"] == {
        "kind": "iterable",
        "async": True,
        "generator": False,
        "element": risk,
    }
    assert returns["upper"]["kind"] == "array"
    assert [f["type"] for f in raw["types"]["Risk"]["fields"]] == [
        {"kind": "number"},
        {"kind": "array", "element": {"kind": "string"}},
        {"kind": "date"},
    ]


def test_a_write_binding_is_read_from_its_option(node, compiler, tmp_path):
    source = """
    import { table, command } from "@provisa/sdk";
    export interface Order { id: number; }
    export const orders = table((): Order[] => []);
    export const add = command(async (rows: Order[]): Promise<void> => {}, { insert: "orders" });
    """
    read = _read(node, compiler, _compiled(node, compiler, tmp_path, source))
    assert read.export("add").binds == (("insert", "orders"),)


def test_an_option_that_is_not_a_literal_is_refused_by_name(node, compiler, tmp_path):
    source = """
    import { table, command, type ReadContext } from "@provisa/sdk";
    export interface Row { id: number; }
    const size: number = 100;
    const name: string = "API_KEY";
    const names: string[] = ["API_KEY"];
    const target: string = "rows";
    export const sized = table(async (i: { id: number }, c: ReadContext): Promise<Row[]> => [], {
      loader: size,
    });
    export const needs = table((): Row[] => [], { requires: [name] });
    export const many = table((): Row[] => [], { requires: names });
    export const add = command(async (rows: Row[]): Promise<void> => {}, { insert: target });
    """
    with pytest.raises(ManifestRefused) as refused:
        _read(node, compiler, _compiled(node, compiler, tmp_path, source))
    problems = [
        p.removeprefix("the declarations do not type-check: ") for p in refused.value.problems
    ]
    assert problems == [
        "sized: a @loader size is a literal number, and this one is number",
        "needs: a @requires name is a literal string, and this one is string",
        "many: a @requires name is a literal string, and this one is readonly string[]",
        "add: an @insert table is a literal string, and this one is string",
    ]


def test_a_newer_spelling_is_refused_by_name(node, compiler, tmp_path):
    (tmp_path / "index.d.ts").write_text(
        """
        import type { Tagged } from "@provisa/sdk";
        export interface Row { id: number; }
        export declare const later: (() => Row[]) & Tagged<"table", {}, 2>;
        """
    )
    with pytest.raises(ManifestRefused) as refused:
        _read(node, compiler, tmp_path)
    assert refused.value.problems == [
        "the declarations do not type-check: later: it is branded with @provisa/sdk spelling 2; "
        "this server reads up to spelling 1"
    ]


def _raw(node: Path, compiler: Path, bundle: Path) -> dict:
    from provisa.node.reader import entry_declarations, reader_command, run_reader

    return run_reader(reader_command("node", node, compiler, entry_declarations(bundle)))


#: Each line the pinned tsc must reject, by its marker comment.
REJECTED = """
import { table, stream, columns, health, onStop, command, type ReadContext } from "@provisa/sdk";
export interface Row { id: number; }
export const one = table(async (): Promise<Row> => ({ id: 1 })); // single
export const three = table((a: { x: number }, c: ReadContext, d: number): Row[] => []); // arity
export const flipped = table((c: ReadContext, i: { x: number }): Row[] => []); // order
export const unbatched = table((i: { x: number }): Row[] => [], { loader: 10 }); // loader
export const plain = stream((): Row[] => []); // stream
export const scalar = columns((names: string): string[] => [names]); // columns
export const sick = health(() => 1); // health
export const busy = onStop((x: number) => {}); // stop
export const typo = command(async (): Promise<void> => {}, { inserts: "rows" }); // option
"""


def test_the_pinned_tsc_rejects_what_the_wrappers_forbid(node, compiler, tmp_path):
    project = _project(tmp_path, REJECTED)
    done = _tsc(node, compiler, project)
    assert done.returncode != 0
    markers = {n: line.rsplit("// ", 1)[-1] for n, line in enumerate(REJECTED.splitlines(), 1)}
    rejected = {
        markers[int(m.group(1))]
        for m in re.finditer(r"index\.ts\((\d+),\d+\): error TS\d+", done.stdout)
    }
    assert rejected == {
        "single", "arity", "order", "loader", "stream", "columns", "health", "stop", "option"
    }  # fmt: skip
