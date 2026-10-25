// Copyright (c) 2026 Kenneth Stott
// Canary: 52f26bb5-5c4e-4ff7-8c48-93b8425433ce
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// A batch answered one result per statement: each statement's rows, or why it failed, said as
// that statement alone would have been refused.

import { afterEach, describe, expect, it, vi } from "vitest";
import { runSqlEach } from "../admin";

afterEach(() => vi.unstubAllGlobals());

describe("runSqlEach", () => {
  it("asks for each statement's result, and reads each one's rows or error", async () => {
    const fetched = vi.fn().mockResolvedValue(
      new Response(
        JSON.stringify({
          results: [
            {
              statement: "SELECT id FROM orders",
              warnings: [],
              data: [[1]],
              columns: ["id"],
              column_types: ["integer"],
            },
            {
              statement: "SELECT id FROM secret",
              warnings: [],
              error: { status: 403, body: { detail: "Table secret is not in your scope" } },
            },
          ],
        }),
        { status: 200, headers: { "Content-Type": "application/json" } },
      ),
    );
    vi.stubGlobal("fetch", fetched);
    const r = await runSqlEach("SELECT id FROM orders\n;\nSELECT id FROM secret", "analyst", false);
    expect(JSON.parse(fetched.mock.calls[0][1].body)).toMatchObject({ results: "each" });
    expect(r.error).toBeUndefined();
    expect(r.answers.map((a) => a.statement)).toEqual([
      "SELECT id FROM orders",
      "SELECT id FROM secret",
    ]);
    expect(r.answers[0]).toMatchObject({ columns: ["id"], rows: [{ id: 1 }] });
    expect(r.answers[0].error).toBeUndefined();
    expect(r.answers[1].error).toContain("Table secret is not in your scope");
    expect(r.answers[1].rows).toEqual([]);
  });
});
