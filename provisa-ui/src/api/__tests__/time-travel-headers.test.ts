// Copyright (c) 2026 Kenneth Stott
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1163: the SQL explorer's time travel travels in X-Provisa-As-Of and X-Provisa-As-Recorded,
// on a run and on an explain alike; a picker left empty sends nothing.
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { explainSql, runSql } from "../admin";

let fetchMock: ReturnType<typeof vi.fn>;

beforeEach(() => {
  fetchMock = vi.fn().mockResolvedValue(
    new Response(JSON.stringify({ data: { sql: [] }, columns: [] }), {
      status: 200,
      headers: { "Content-Type": "application/json" },
    }),
  );
  vi.stubGlobal("fetch", fetchMock);
});

afterEach(() => {
  vi.unstubAllGlobals();
});

function headers(): Record<string, string> {
  return (fetchMock.mock.calls[0][1] as RequestInit).headers as Record<string, string>;
}

const TRAVEL = { asOf: "2026-09-30T12:00:00", asRecorded: "2026-10-02T00:00:00" };

describe("time travel headers", () => {
  it("runSql sends both times", async () => {
    await runSql("select 1", "regulator", false, TRAVEL);
    expect(headers()["X-Provisa-As-Of"]).toBe(TRAVEL.asOf);
    expect(headers()["X-Provisa-As-Recorded"]).toBe(TRAVEL.asRecorded);
  });

  it("explainSql sends both times", async () => {
    await explainSql("select 1", "regulator", false, TRAVEL);
    expect(headers()["X-Provisa-As-Of"]).toBe(TRAVEL.asOf);
    expect(headers()["X-Provisa-As-Recorded"]).toBe(TRAVEL.asRecorded);
  });

  it("an empty picker sends nothing", async () => {
    await runSql("select 1", "regulator", false, { asOf: null, asRecorded: "" });
    expect(headers()).not.toHaveProperty("X-Provisa-As-Of");
    expect(headers()).not.toHaveProperty("X-Provisa-As-Recorded");
  });
});
