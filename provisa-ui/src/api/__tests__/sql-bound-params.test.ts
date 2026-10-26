// Copyright (c) 2026 Kenneth Stott
// Canary: d5190377-af83-4070-bbb4-1ee9b5a8eb69
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1163, REQ-2083: a statement's time travel is in its own text (each relation's as_of and
// as_recorded arguments), so no request carries a time header; a run sends the values bound to its
// $1…$n placeholders beside the text, and none when it has none.
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { explainSql, runSqlEach } from "../admin";

let fetchMock: ReturnType<typeof vi.fn>;

beforeEach(() => {
  fetchMock = vi.fn().mockResolvedValue(
    new Response(JSON.stringify({ results: [] }), {
      status: 200,
      headers: { "Content-Type": "application/json" },
    }),
  );
  vi.stubGlobal("fetch", fetchMock);
});

afterEach(() => {
  vi.unstubAllGlobals();
});

function request(): RequestInit {
  return fetchMock.mock.calls[0][1] as RequestInit;
}

const SQL = `SELECT * FROM "reproduce"."view_exposure"(as_recorded => $1) AS e`;

describe("a statement's bound values", () => {
  it("are sent beside its text", async () => {
    await runSqlEach(SQL, "regulator", false, ["2026-10-01T17:00:00"]);
    expect(JSON.parse(request().body as string)).toEqual({
      sql: SQL,
      results: "each",
      params: ["2026-10-01T17:00:00"],
    });
  });

  it("are left out when there are none", async () => {
    await runSqlEach("select 1", "regulator", false);
    expect(JSON.parse(request().body as string)).toEqual({ sql: "select 1", results: "each" });
  });

  it("travel with no time header on a run or an explain", async () => {
    await runSqlEach(SQL, "regulator", false, ["2026-10-01T17:00:00"]);
    fetchMock.mockResolvedValueOnce(new Response("{}", { status: 200 }));
    await explainSql("select 1", "regulator", false);
    for (const [, init] of fetchMock.mock.calls) {
      const headers = (init as RequestInit).headers as Record<string, string>;
      expect(Object.keys(headers).filter((h) => h.startsWith("X-Provisa-As"))).toEqual([]);
    }
  });
});
