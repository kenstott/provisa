// Copyright (c) 2026 Kenneth Stott
// Canary: d0bbf5dc-ec3c-4d18-a986-5b8c99237bad
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.

// REQ-2071: each Python Playground run is a script of its own: fresh globals every run, released
// when it ends, so nothing (a variable, a sign-in) carries over. Pyodide is a double.

import { describe, it, expect, vi } from "vitest";
import { runScript, type PyodideLike } from "../dataScience/pythonRun";

function pyodide() {
  const namespaces: Array<{ id: number; destroy: () => void; destroyed: boolean }> = [];
  const ran: Array<{ code: string; globals: unknown }> = [];
  const py: PyodideLike = {
    loadPackagesFromImports: vi.fn(async () => undefined),
    runPythonAsync: vi.fn(async (code: string, options?: { globals?: unknown }) => {
      ran.push({ code, globals: options?.globals });
      if (code === "x") throw new Error("NameError: name 'x' is not defined");
      return undefined;
    }),
    globals: {
      get: (name: string) => {
        if (name === "_provisa_fresh_globals") {
          return () => {
            const ns = { id: namespaces.length, destroyed: false, destroy: () => {} };
            ns.destroy = () => (ns.destroyed = true);
            namespaces.push(ns);
            return ns;
          };
        }
        return () => JSON.stringify({ value: { kind: "none" }, images: [] });
      },
    },
  };
  return { py, namespaces, ran };
}

describe("runScript", () => {
  it("runs every script in fresh globals of its own, released when it ends", async () => {
    const { py, namespaces, ran } = pyodide();
    const printed = { text: "" };
    await runScript(py, 1, "x = 1", printed);
    const second = await runScript(py, 2, "x", printed);
    expect(namespaces).toHaveLength(2);
    expect(ran[0].globals).toBe(namespaces[0]);
    expect(ran[1].globals).toBe(namespaces[1]);
    expect(namespaces.every((ns) => ns.destroyed)).toBe(true);
    expect(second.error).toContain("NameError");
  });

  it("reports only what this run printed", async () => {
    const { py } = pyodide();
    const printed = { text: "left over\n" };
    const result = await runScript(py, 3, "pass", printed);
    expect(result.stdout).toBe("");
  });
});
