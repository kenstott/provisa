# Copyright (c) 2026 Kenneth Stott
# Canary: 92f50284-6070-46b3-985a-4f758c34c412
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A Node bundle's declarations read by the real reader (REQ-1984): the pinned runtime and the
pinned TypeScript compiler are fetched as a source's would be, the reader runs under each
runtime, and the bundle's tables, commands and streams are the same on Node, Bun and Deno."""

# Requirements: REQ-1984
from __future__ import annotations

import pytest

from provisa.codesource.manifest import (
    EnumOf,
    Field,
    ListOf,
    ManifestRefused,
    Named,
    Scalar,
    TableOf,
)
from provisa.node.reader import ensure_typescript, read_bundle
from provisa.runtime_deps.code_runtimes import CodeRuntimeResolver

pytestmark = [pytest.mark.integration]

DECLARATIONS = """
import type { ReadContext } from "@provisa/services";
export interface Line { sku: string; qty: number; }
export type Tier = "gold" | "silver";
export interface Order {
  /** The order's number. */
  id: number;
  placed: Date;
  tier: Tier;
  lines: Line[];
  note?: string | null;
}
export interface OrdersInputs {
  /** The customer whose orders these are. */
  customerId: string;
  since?: Date;
}
export interface PlacedInputs { region: string; }
/**
 * Orders of one customer.
 * @table
 */
export declare function orders(inputs: OrdersInputs, context: ReadContext): Promise<Order[]>;
/** Orders as they are placed. @stream */
export declare function placed(inputs: PlacedInputs): AsyncGenerator<Order>;
/** @command */
export declare function cancel(id: number): Promise<void>;
export declare function helper(): number;
"""


@pytest.fixture(scope="module")
def cache(tmp_path_factory):
    return tmp_path_factory.mktemp("runtime-deps")


@pytest.fixture(scope="module")
def compiler(cache):
    return ensure_typescript(cache, [])


@pytest.fixture
def bundle(tmp_path):
    (tmp_path / "index.d.ts").write_text(DECLARATIONS)
    return tmp_path


@pytest.mark.parametrize("runtime", ["node", "bun", "deno"])
def test_each_runtime_reads_the_same_tables_commands_and_streams(runtime, cache, compiler, bundle):
    executable = CodeRuntimeResolver(cache_root=cache).resolve(runtime)
    manifest = read_bundle(runtime=runtime, executable=executable, compiler=compiler, bundle=bundle)
    assert [(e.name, e.kind) for e in manifest.exports] == [
        ("orders", "table"),
        ("placed", "stream"),
        ("cancel", "command"),
    ]
    orders = manifest.export("orders")
    assert orders.description == "Orders of one customer."
    # REQ-2100: its inputs' fields are its parameters, and it takes its read context too.
    assert [(p.name, p.shape, p.required) for p in orders.parameters] == [
        ("customerId", Scalar("text"), True),
        ("since", Scalar("timestamp"), False),
    ]
    assert orders.parameters[0].description == "The customer whose orders these are."
    assert orders.takes == ("inputs", "context")
    assert manifest.export("placed").takes == ("inputs",)
    assert "ReadContext" not in manifest.types
    assert orders.returns == Named("Order")
    assert manifest.types["Order"].fields == (
        Field("id", Scalar("double"), False, "The order's number."),
        Field("placed", Scalar("timestamp"), False),
        Field("tier", EnumOf("Tier", ("gold", "silver")), False),
        Field("lines", ListOf(Named("Line")), False),
        Field("note", Scalar("text"), True),
    )
    assert manifest.export("cancel").returns is None


def test_the_lifecycle_tags_are_read_from_jsdoc(cache, compiler, tmp_path):
    """REQ-2221: a hyphenated tag reaches the manifest whole."""
    (tmp_path / "index.d.ts").write_text(
        """
        /** @on-start */
        export declare function connect(values: Record<string, string>): Promise<void>;
        /** @on-stop */
        export declare function close(): void;
        /** @health */
        export declare function check(): Promise<string>;
        """
    )
    executable = CodeRuntimeResolver(cache_root=cache).resolve("node")
    manifest = read_bundle(
        runtime="node", executable=executable, compiler=compiler, bundle=tmp_path
    )
    assert {e.name: e.kind for e in manifest.exports} == {
        "connect": "on-start",
        "close": "on-stop",
        "check": "health",
    }


def test_a_loose_row_type_is_refused_by_name(cache, compiler, tmp_path):
    (tmp_path / "index.d.ts").write_text(
        """
        export interface Row { id: number; payload: Record<string, unknown>; }
        /** @table */
        export declare function anything(): any[];
        /** @table */
        export declare function mystery(): Promise<unknown[]>;
        /** @table */
        export declare function mapped(): Record<string, number>[];
        /** @stream */
        export declare function feed(): AsyncGenerator<Record<string, unknown>>;
        /** @table */
        export declare function typed(): Row[];
        """
    )
    executable = CodeRuntimeResolver(cache_root=cache).resolve("node")
    with pytest.raises(ManifestRefused) as refused:
        read_bundle(runtime="node", executable=executable, compiler=compiler, bundle=tmp_path)
    assert refused.value.problems == [
        f"{name}: its row type is loose (any, unknown, a Record or a dict); declare a typed row "
        "whose fields are its columns"
        for name in ("anything", "mystery", "mapped", "feed")
    ]


def test_a_single_field_of_a_typed_row_may_still_be_loose(cache, compiler, tmp_path):
    (tmp_path / "index.d.ts").write_text(
        """
        export interface Row { id: number; payload: Record<string, unknown>; }
        /** @table */
        export declare function typed(): Row[];
        """
    )
    executable = CodeRuntimeResolver(cache_root=cache).resolve("node")
    manifest = read_bundle(
        runtime="node", executable=executable, compiler=compiler, bundle=tmp_path
    )
    assert manifest.types["Row"].fields == (
        Field("id", Scalar("double"), False),
        Field("payload", Scalar("json"), False),
    )


def test_a_bundle_that_breaks_the_rules_is_refused_with_every_problem(cache, compiler, tmp_path):
    (tmp_path / "index.d.ts").write_text(
        """
        export interface Order { id: number; }
        /** @tabel */
        export declare function typo(): Order[];
        /** @table */
        export declare function one(): Promise<Order>;
        """
    )
    executable = CodeRuntimeResolver(cache_root=cache).resolve("node")
    with pytest.raises(ManifestRefused) as refused:
        read_bundle(runtime="node", executable=executable, compiler=compiler, bundle=tmp_path)
    assert refused.value.problems == [
        "typo: unknown tag @tabel",
        "one: @table returns a single value, not a collection of rows",
    ]


TABLE_ARGUMENT = """
import type { TableRef } from "@provisa/services";
export interface Order { id: number; region: string; }
export interface Score { region: string; score: number; }
/** @query */
export declare function score(orders: TableRef<Order>): Promise<Score[]>;
/** @command */
export declare function rescore(orders: TableRef<Order>): Promise<Score[]>;
"""


def test_a_query_takes_a_table_and_a_command_is_a_mutation(cache, compiler, tmp_path):  # REQ-2226
    (tmp_path / "index.d.ts").write_text(TABLE_ARGUMENT)
    executable = CodeRuntimeResolver(cache_root=cache).resolve("node")
    manifest = read_bundle(
        runtime="node", executable=executable, compiler=compiler, bundle=tmp_path
    )
    assert manifest.export("score").kind == "query"
    assert manifest.export("rescore").kind == "command"
    assert manifest.export("score").parameters[0].shape == TableOf(Named("Order"))
