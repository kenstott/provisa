// Copyright (c) 2026 Kenneth Stott
// Canary: 2f7a9c41-8e36-4d05-b1e2-6c9d3a8f5b17
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// A backport of monaco-editor 0.57.0's own fix into the 0.52.2 that @graphiql/react pins exactly.
//
// In 0.52.2, editor/contrib/wordHighlighter/browser/wordHighlighter.js (lines 184 and 192) calls
// `this.runDelayer.trigger(...)` and drops the promise. Disposing the editor (leaving the GraphQL
// explorer while a highlight is pending) cancels the Delayer, and its CancellationError ("Canceled")
// surfaces as an unhandled rejection -- which the e2e coverage fixture fails any spec on. Monaco
// 0.57.0 catches both with `.catch(onUnexpectedError)`, and onUnexpectedError ignores cancellation;
// this adds exactly that, nothing else. When GraphiQL moves past monaco-editor 0.52.2 the config
// refuses to load until this backport is removed.

import fs from "fs";
import path from "path";
import type { Plugin } from "vite";

/** The one monaco-editor this backport is written against. */
export const PATCHED_MONACO = "0.52.2";

const FILE = "/editor/contrib/wordHighlighter/browser/wordHighlighter.js";

/** Each call that drops its promise, and the same call as 0.57.0 writes it. */
const SITES: readonly [string, string][] = [
  [
    "this.runDelayer.trigger(() => { this._onPositionChanged(e); });",
    "this.runDelayer.trigger(() => { this._onPositionChanged(e); }).catch(onUnexpectedError);",
  ],
  [
    "this.runDelayer.trigger(() => { this._run(); });",
    "this.runDelayer.trigger(() => { this._run(); }).catch(onUnexpectedError);",
  ],
];

/** ``code`` with both calls catching, if ``id`` is the word highlighter; else unchanged (null).
 * Throws when a call is not there as 0.52.2 writes it: a silent no-op would bring the error back. */
export function backportCancelCatch(code: string, id: string): string | null {
  if (!id.replaceAll("\\", "/").includes(`monaco-editor/esm/vs${FILE}`)) return null;
  let out = code;
  for (const [before, after] of SITES) {
    if (out.split(before).length !== 2) {
      throw new Error(
        `monacoCancelBackport: ${id} does not hold exactly one "${before}" -- the monaco-editor ` +
          `it was written for (${PATCHED_MONACO}) has changed; remove the backport if the new ` +
          "version catches these calls itself (0.57.0 does).",
      );
    }
    out = out.replace(before, after);
  }
  return out;
}

/** Refuses a monaco-editor other than the one the backport is for. */
export function assertPatchedMonaco(root: string): void {
  const pkg = path.join(root, "node_modules", "monaco-editor", "package.json");
  const version = (JSON.parse(fs.readFileSync(pkg, "utf8")) as { version: string }).version;
  if (version !== PATCHED_MONACO) {
    throw new Error(
      `monacoCancelBackport is written for monaco-editor ${PATCHED_MONACO}, but ${version} is ` +
        "installed: remove it from vite.config.ts if this version catches the word highlighter's " +
        "Delayer.trigger calls (0.57.0 does), or rewrite it for this one.",
    );
  }
}

/** The backport as a plugin: the same transform for Vite's build and for its dependency
 * pre-bundling (dev serves monaco pre-bundled, where Vite's own transform hooks do not run). */
export function monacoCancelBackport(): Plugin {
  return {
    name: "monaco-cancel-backport",
    enforce: "pre",
    transform(code, id) {
      const out = backportCancelCatch(code, id);
      return out === null ? null : { code: out, map: null };
    },
  };
}
