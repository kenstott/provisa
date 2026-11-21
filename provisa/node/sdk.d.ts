// Copyright (c) 2026 Kenneth Stott
// Canary: a32276ac-07b1-4845-987f-2102c9cecb33
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1984: the types of "@provisa/sdk", an optional package a Node source's TypeScript may import.
// Two copies are kept the same: provisa/node/sdk.d.ts, which Provisa's reader (reader.mjs) resolves
// "@provisa/sdk" to whatever version of the package a bundle installed, and the package's
// provisa-sdk/typescript/index.d.ts, which takes the services types from its own services.d.ts.
// They differ only in that one module name (tests/unit/test_node_sdk_package.py).
//
// Each wrapper returns the function it is given; its result type carries a phantom brand, the
// only thing a declaration file keeps of it. The reader reads the brand as the JSDoc tags it
// stands for: `table(fn, { loader: 100 })` reads as `@table` and `@loader 100`.

import type { ReadContext } from "@provisa/services";

export type { Filter, Order, ReadContext, TableRef } from "@provisa/services";

/** The SDK spelling this file declares. A bundle branded with a newer one is refused by name. */
export declare const SPELLING: 1;
export type Spelling = 1;

/** The brand's key. Never set at run time. */
export declare const provisa: unique symbol;

/** A JSDoc tag an export may stand for. */
export type Kind = "table" | "stream" | "command" | "query" | "columns" | "on-start" | "on-stop" | "health";

/** What the reader reads off a branded export: its kind, the spelling, and the options given. */
export type Tagged<K extends Kind, O extends object, S extends number> = {
  readonly [provisa]?: { readonly kind: K; readonly spelling: S; readonly options: O };
};

/** A setup value the code needs (`@requires NAME` or `@requires NAME:secret`). */
export type Requirement = string;

/** What every wrapper may also state. */
export interface Options {
  /** Each `@requires` line, `NAME` or `NAME:secret`. */
  readonly requires?: readonly Requirement[];
}

/** `@loader`: true for every key in one batch, or a literal batch size (`@loader N`). */
export interface TableOptions extends Options {
  readonly loader?: true | number;
}

/** `@insert T`, `@update T`, `@delete T`: which write of the table named this command is. */
export interface CommandOptions extends Options {
  readonly insert?: string;
  readonly update?: string;
  readonly delete?: string;
}

/** The rows a @table returns: an array or an iterable, synchronous or asynchronous. */
export type Rows<R> = Iterable<R>;
export type AsyncRows<R> = AsyncIterable<R>;
/** A @columns parameter or result: one value per row of the batch. */
export type Batch<T> = T[];

/** Column data types, aliases of what the reader already maps. */
export type Text = string;
export type Double = number;
export type BigInt = bigint;
export type Boolean = boolean;
export type Timestamp = Date;
export type Json = unknown;

type Collection<R> = R[] | Iterable<R> | AsyncIterable<R>;
type Result<R> = Collection<R> | Promise<Collection<R>>;

/** A @table's function: `(inputs, context)`, either left out. */
export type ReadFn =
  | (() => Result<object>)
  | ((inputs: never) => Result<object>)
  | ((inputs: never, context: ReadContext) => Result<object>);
/** A @loader's function: a @table's that takes its read context, alone or after its inputs. */
export type LoaderFn<F extends ReadFn> = Parameters<F> extends [ReadContext] | [unknown, ReadContext] ? F : never;
/** A @stream's function: an async generator. */
export type StreamFn =
  | (() => AsyncIterable<object>)
  | ((inputs: never) => AsyncIterable<object>)
  | ((inputs: never, context: ReadContext) => AsyncIterable<object>);
/** A @columns function: each parameter a list, returning a list of objects. */
export type ColumnsFn = (...batches: never[][]) => object[] | Promise<object[]>;

export declare function table<F extends ReadFn>(fn: F): F & Tagged<"table", {}, Spelling>;
export declare function table<F extends ReadFn, const O extends TableOptions & { readonly loader: true | number }>(
  fn: F & LoaderFn<F>,
  options: O,
): F & Tagged<"table", O, Spelling>;
export declare function table<F extends ReadFn, const O extends Options & { readonly loader?: never }>(
  fn: F,
  options: O,
): F & Tagged<"table", O, Spelling>;
export declare function stream<F extends StreamFn, const O extends Options = {}>(
  fn: F,
  options?: O,
): F & Tagged<"stream", O, Spelling>;
export declare function command<F extends (...args: never[]) => unknown, const O extends CommandOptions = {}>(
  fn: F,
  options?: O,
): F & Tagged<"command", O, Spelling>;
export declare function query<F extends (...args: never[]) => unknown, const O extends Options = {}>(
  fn: F,
  options?: O,
): F & Tagged<"query", O, Spelling>;
export declare function columns<F extends ColumnsFn, const O extends Options = {}>(
  fn: F,
  options?: O,
): F & Tagged<"columns", O, Spelling>;
export declare function onStart<
  F extends (() => unknown) | ((values: Record<string, string>) => unknown),
  const O extends Options = {},
>(fn: F, options?: O): F & Tagged<"on-start", O, Spelling>;
export declare function onStop<F extends () => unknown>(fn: F): F & Tagged<"on-stop", {}, Spelling>;
export declare function health<F extends () => string | Promise<string>>(fn: F): F & Tagged<"health", {}, Spelling>;
