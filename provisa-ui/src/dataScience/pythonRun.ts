// Copyright (c) 2026 Kenneth Stott
// Canary: e0d40369-cbd2-47c2-85d5-4de83e8d1a73
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.

// REQ-2071: one run of the Python Playground, given a loaded Pyodide: a script of its own.
import type { FromWorker } from "./protocol";

/** Pyodide reports each package it loads; that is not the code's output, so it is not shown. */
export interface LoadOptions {
  messageCallback: (message: string) => void;
}
export const QUIET: LoadOptions = { messageCallback: () => {} };

/** The parts of a loaded Pyodide a run uses. */
export interface PyodideLike {
  loadPackagesFromImports(code: string, options?: LoadOptions): Promise<unknown>;
  runPythonAsync(code: string, options?: { globals?: unknown }): Promise<unknown>;
  globals: { get(name: string): (...args: unknown[]) => unknown };
}

type Result = Extract<FromWorker, { type: "result" }>;

/** Run ``code`` as a script of its own: fresh globals, so no variable and no sign-in carries over
 * from an earlier run (imported modules stay loaded). ``printed`` collects what it prints. */
export async function runScript(
  py: PyodideLike,
  id: number,
  code: string,
  printed: { text: string },
): Promise<Result> {
  printed.text = "";
  try {
    // A package the code imports (scikit-learn, matplotlib) is loaded from /pyodide first.
    await py.loadPackagesFromImports(code, QUIET);
    const namespace = py.globals.get("_provisa_fresh_globals")() as { destroy?: () => void };
    let value: unknown;
    try {
      value = await py.runPythonAsync(code, { globals: namespace });
    } finally {
      namespace.destroy?.();
    }
    const shown = JSON.parse(String(py.globals.get("_provisa_show")(value))) as {
      value: Result["value"];
      images: string[];
    };
    (value as { destroy?: () => void } | undefined)?.destroy?.();
    return {
      type: "result",
      id,
      stdout: printed.text,
      value: shown.value,
      images: shown.images,
      error: null,
    };
  } catch (err) {
    return {
      type: "result",
      id,
      stdout: printed.text,
      value: { kind: "none" },
      images: [],
      error: String((err as Error)?.message ?? err),
    };
  }
}
