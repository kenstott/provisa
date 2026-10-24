// Copyright (c) 2026 Kenneth Stott
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2104, REQ-2106, REQ-2100: the types of "@provisa/services" (services.mjs), for a Node
// source's TypeScript. Provisa's reader resolves "@provisa/services" to this file.

/** A condition the read puts on a return column: `column op value`. */
export interface Filter {
  column: string;
  op: "eq" | "ne" | "gt" | "gte" | "lt" | "lte" | "in" | "like";
  value: unknown;
}

/** A key the read orders its rows by. */
export interface Order {
  column: string;
  descending: boolean;
  nulls_first: boolean;
}

/**
 * REQ-2100: what a read asks of a @table or @stream function's rows, a function's second
 * parameter: `(inputs, context)`. Every part is a hint, and Provisa applies the whole statement to
 * the rows the function returns. If the function applies the limit, it applies every filter it is
 * given and the order first; otherwise it ignores the limit. A landed read gives an empty context.
 */
export interface ReadContext {
  filters: Filter[];
  order_by: Order[];
  /** The rows the read keeps at most, its offset folded in; null: every row. */
  limit: number | null;
  columns: string[];
  /** A @loader's join keys, `{hash: {input: value}}`, answered with `{hash: rows}`. */
  keys: Record<string, Record<string, unknown>> | null;
}

export declare class ServiceError extends Error {
  service: string;
  status: number;
}

export declare const secrets: { get(name: string): Promise<string> };
export declare const settings: {
  get(name: string, fallback?: unknown): Promise<unknown>;
  all(): Promise<Record<string, unknown>>;
};
export declare const cache: {
  get(key: string, fallback?: unknown): Promise<unknown>;
  set(key: string, value: unknown, ttl?: number | null): Promise<void>;
  delete(key: string): Promise<void>;
};
export declare const vectors: {
  embed(texts: string[], model: string): Promise<number[][]>;
  upsert(items: unknown[], model?: string | null): Promise<number>;
  search(query: string | number[], k?: number, model?: string | null): Promise<unknown[]>;
  delete(ids: string[]): Promise<void>;
};
export declare const http: {
  get(url: string, options?: { headers?: Record<string, string>; timeout?: number | null }): Promise<unknown>;
  post(url: string, options?: Record<string, unknown>): Promise<unknown>;
};
export declare const llm: {
  baseUrl(): string;
  chat(model: string, messages: unknown[], params?: Record<string, unknown>): Promise<unknown>;
  embeddings(model: string, input: unknown): Promise<unknown>;
};
export declare function log(message: string, level?: string, fields?: Record<string, unknown>): Promise<void>;
export declare function metric(
  name: string,
  value: number,
  kind?: string,
  attributes?: Record<string, unknown>,
): Promise<void>;
