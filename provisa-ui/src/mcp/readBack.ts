// Copyright (c) 2026 Kenneth Stott
// Canary: 7c1d9e04-52ab-4f36-9a0e-3f6be81d24c7
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { readScreen, type Screen, type ScreenInputs } from "./screenState";

/**
 * Polly's navigate reads the screen back before it answers: a page handed a payload has taken it
 * only when the screen shows it. Handing a page state is a request; a page can drop it (a method
 * the role's proto does not have, a table outside the domain filter, a page behind a capability),
 * so "Navigated" alone would be a false Done. Each check compares what was handed with what the
 * page publishes to read_screen, and names the difference.
 */

type Handed = Record<string, unknown>;
/** The first difference between the payload and the screen; null when the screen shows it. */
type Check = (state: Handed, screen: Screen) => string | null;

/** Whitespace is not a difference: an editor may lay a statement out as it likes. */
function sameText(a: string, b: string): boolean {
  return a.replace(/\s+/g, " ").trim() === b.replace(/\s+/g, " ").trim();
}

function editorCheck(key: string): Check {
  return (state, screen) => {
    const handed = state[key];
    if (typeof handed !== "string") return null;
    const shown = screen.editor?.text ?? "";
    return sameText(handed, shown)
      ? null
      : `the editor shows ${JSON.stringify(shown)}, not the ${key} handed to it`;
  };
}

/** Objects by content, whatever order their keys were written in. */
function sameJson(a: unknown, b: unknown): boolean {
  if (Array.isArray(a) && Array.isArray(b)) {
    return a.length === b.length && a.every((item, i) => sameJson(item, b[i]));
  }
  if (a && b && typeof a === "object" && typeof b === "object") {
    const [ka, kb] = [a as Handed, b as Handed];
    const keys = Object.keys(ka);
    return (
      keys.length === Object.keys(kb).length && keys.every((k) => k in kb && sameJson(ka[k], kb[k]))
    );
  }
  return a === b;
}

function parsedBody(text: unknown): unknown {
  try {
    return JSON.parse(String(text));
  } catch {
    return undefined; // not JSON: no handed object equals it, which is the difference reported
  }
}

/** The status a page that runs its own request publishes once the handed call has answered. */
function runCheck(state: Handed, inputs: ScreenInputs): string | null {
  if (state.autoRun !== true) return null;
  if (inputs.status === "failed") return `the page shows this error: ${String(inputs.error)}`;
  return inputs.status === "answered" ? null : "the page has not answered yet";
}

const checkGrpc: Check = (state, { inputs }) => {
  if (typeof state.grpcMethod !== "string") return null;
  if (!inputs) return "the page has published nothing";
  const handed = state.grpcMethod.replace(/\(.*$/, "");
  const method = String(inputs.method ?? "");
  if (handed !== method) return `the page has ${method || "no method"} selected, not ${handed}`;
  if (state.grpcBody !== undefined && !sameJson(state.grpcBody, parsedBody(inputs.body))) {
    return `the body on screen is ${String(inputs.body)}, not the grpcBody handed to it`;
  }
  return runCheck(state, inputs);
};

/** "/a/b?x=1" as its path and parameters. */
function splitUrl(url: string): { path: string; params: URLSearchParams } {
  const at = url.indexOf("?");
  return at < 0
    ? { path: url, params: new URLSearchParams() }
    : { path: url.slice(0, at), params: new URLSearchParams(url.slice(at + 1)) };
}

/** One parameter's value: JSON by content (OpenAPI's filter), else a comma-separated list in any
 * order. */
function sameList(a: string, b: string): boolean {
  const [ja, jb] = [parsedBody(a), parsedBody(b)];
  if (typeof ja === "object" && typeof jb === "object") return sameJson(ja, jb);
  return sameJson(a.split(",").sort(), b.split(",").sort());
}

/** Every parameter of `handed` that `shown` does not carry, each named. */
function missingParams(handed: URLSearchParams, shown: URLSearchParams): string[] {
  const missing: string[] = [];
  handed.forEach((value, key) => {
    const on = shown.get(key);
    // ?aggregate=true is "every function", which the page spells out in its picker.
    if (key === "aggregate" && value === "true" && on !== null) return;
    if (on === null || !sameList(value, on)) missing.push(`${key}=${value}`);
  });
  return missing;
}

const checkJsonApi: Check = (state, { inputs }) => {
  if (typeof state.jsonapiUrl !== "string") return null;
  if (!inputs) return "the page has published nothing";
  const handed = splitUrl(state.jsonapiUrl);
  const shown = splitUrl(String(inputs.url ?? ""));
  if (handed.path !== shown.path) {
    return `the page shows ${shown.path || "no table"}, not ${handed.path}`;
  }
  // Every column selected is the same request as none: the page's URL then names no fields.
  const fields = String(inputs.fields ?? "");
  handed.params.forEach((value, key) => {
    if (key.startsWith("fields[") && sameList(value, fields)) shown.params.set(key, value);
  });
  const missing = missingParams(handed.params, shown.params);
  if (missing.length > 0) return `the page's request does not carry ${missing.join(", ")}`;
  return runCheck(state, inputs);
};

const checkOpenApi: Check = (state, { inputs }) => {
  if (typeof state.openApiUrl !== "string") return null;
  if (!inputs) return "the page has published nothing";
  const [method = ""] = state.openApiUrl.trim().split(/\s/);
  const handed = splitUrl(state.openApiUrl.trim().slice(method.length).trim());
  if (inputs.status === "failed") return `the page shows this error: ${String(inputs.error)}`;
  const operation = String(inputs.operation ?? "");
  const [shownMethod = "", shownPath = ""] = operation.split(" ");
  if (shownMethod !== method.toUpperCase() || !handed.path.endsWith(shownPath) || !shownPath) {
    return `the page has ${operation || "no operation"} open, not ${method} ${handed.path}`;
  }
  const shown = new URLSearchParams();
  for (const [name, value] of Object.entries((inputs.parameters ?? {}) as Handed)) {
    shown.set(name, String(value));
  }
  const missing = missingParams(handed.params, shown);
  if (missing.length > 0) return `the operation on screen does not carry ${missing.join(", ")}`;
  if (state.openApiBody !== undefined && !sameJson(state.openApiBody, parsedBody(inputs.body))) {
    return `the body on screen is ${String(inputs.body)}, not the openApiBody handed to it`;
  }
  return runCheck(state, inputs);
};

const CHECKS: Record<string, Check> = {
  "/sql": editorCheck("sql"),
  "/query": editorCheck("query"),
  "/graph": editorCheck("query"),
  "/sparql": editorCheck("query"),
  "/python": editorCheck("code"),
  "/grpc": checkGrpc,
  "/jsonapi": checkJsonApi,
  "/openapi": checkOpenApi,
};

/** How often the screen is read while the page takes in what it was handed. */
const READ_EVERY_MS = 100;

export type ReadBack =
  | { shown: true; screen: Screen | null }
  | { shown: false; difference: string; screen: Screen | null };

/** Reads the screen until it shows `state`, up to `waitMs`. A route with no check takes no state
 * its screen can confirm, so there is nothing to read back. */
export async function readBack(route: string, state: Handed, waitMs: number): Promise<ReadBack> {
  const { path } = splitUrl(route);
  const check = CHECKS[path];
  if (!check) return { shown: true, screen: null };
  const deadline = Date.now() + waitMs;
  for (;;) {
    const left = Math.max(deadline - Date.now(), 0);
    const screen = await readScreen(left);
    if (typeof screen === "string") return { shown: false, difference: screen, screen: null };
    const onRoute = screen.route.split("?")[0] === path;
    const difference = onRoute ? check(state, screen) : `the browser is on ${screen.route}`;
    if (difference === null) return { shown: true, screen };
    if (Date.now() >= deadline) return { shown: false, difference, screen };
    await new Promise((resolve) => setTimeout(resolve, READ_EVERY_MS));
  }
}
