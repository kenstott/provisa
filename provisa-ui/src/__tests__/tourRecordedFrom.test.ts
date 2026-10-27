// Copyright (c) 2026 Kenneth Stott
// Canary: c16292af-4f70-440d-95f4-5ea411974269
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2083: a tour step takes its "as recorded at" time from a value of an earlier sql step's
// answer, read when it runs -- the earlier statement run again as it ran, its role and time travel.

import { describe, it, expect, vi, beforeEach } from "vitest";

const runSql = vi.fn();
vi.mock("../api/admin", () => ({ runSql: (...a: unknown[]) => runSql(...a) }));

import { answerValue, demoTourSteps } from "../tour/demoTour";

const VERSIONS = 'SELECT desk, recorded_at FROM "reproduce"."view_exposure" AS e';

beforeEach(() => runSql.mockReset());

describe("a recording time taken from the tour's own answer", () => {
  it("binds it to the next statement's $1, carrying the earlier statement as it ran", () => {
    const steps = demoTourSteps(
      [
        { key: "who", route: "/sql", capability: "query_development", role: "regulator" },
        { key: "versions", sql: VERSIONS },
        { key: "then", as_recorded_from: { step: "versions", row: 0, column: "recorded_at" } },
      ],
      [],
    );
    expect(steps[2].openQuery).toEqual({
      stateKey: "params",
      text: "",
      from: { sql: VERSIONS, role: "regulator", row: 0, column: "recorded_at" },
    });
    expect(steps[2].element).toBe('[data-testid="sql-bound-params"]');
  });

  it("refuses a step whose earlier statement the tour does not run before it", () => {
    expect(() =>
      demoTourSteps(
        [
          { key: "s", route: "/sql", capability: "query_development", role: "regulator" },
          { key: "then", as_recorded_from: { step: "versions", row: 0, column: "recorded_at" } },
        ],
        [],
      ),
    ).toThrow('takes its time from "versions"');
  });

  it("reads the value when the step runs, from the statement run again as it ran", async () => {
    runSql.mockResolvedValue({
      columns: ["desk", "recorded_at"],
      rows: [
        { desk: "rates", recorded_at: "2026-10-18T09:00:01" },
        { desk: "rates", recorded_at: "2026-10-18T09:00:05" },
      ],
    });
    const from = { sql: VERSIONS, row: 0, column: "recorded_at" };
    await expect(answerValue(from, "analyst")).resolves.toBe("2026-10-18T09:00:01");
    expect(runSql).toHaveBeenCalledWith(VERSIONS, "analyst", false);
  });

  it("fails the step by name when the answer has no such value", async () => {
    runSql.mockResolvedValue({ columns: ["desk"], rows: [] });
    const from = { sql: VERSIONS, row: 0, column: "recorded_at" };
    await expect(answerValue(from, "analyst")).rejects.toThrow("no recorded_at in row 0");
    runSql.mockResolvedValue({ columns: [], rows: [], error: "refused" });
    await expect(answerValue(from, "analyst")).rejects.toThrow("refused");
  });
});
