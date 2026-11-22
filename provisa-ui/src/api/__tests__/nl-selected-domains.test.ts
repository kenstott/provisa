// Copyright (c) 2026 Kenneth Stott
// Canary: 92d61e08-f010-425d-b739-7c906a5b283a
// Canary: placeholder
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2253: natural-language query works only within the navbar's selected domains. Both NL
// requests carry the selection as a list; no narrowing sends none, so the server keeps the role's
// whole surface.

import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import { nlToSql, submitNlQuery } from "../admin";

let fetchMock: ReturnType<typeof vi.fn>;

beforeEach(() => {
  fetchMock = vi.fn().mockResolvedValue(
    new Response(JSON.stringify({ job_id: "j1", sql: "", attempts: 1 }), {
      status: 200,
      headers: { "Content-Type": "application/json" },
    }),
  );
  vi.stubGlobal("fetch", fetchMock);
});

afterEach(() => {
  vi.unstubAllGlobals();
});

function body(): Record<string, unknown> {
  const init = fetchMock.mock.calls[0][1] as RequestInit;
  return JSON.parse(init.body as string) as Record<string, unknown>;
}

describe("NL requests carry the selected domains", () => {
  it("nlToSql sends the selection, the no-domain domain among it", async () => {
    await nlToSql("how many orders", "analyst", true, new Set(["sales", ""]));
    expect(body().domains).toEqual(["sales", ""]);
  });

  it("nlToSql sends no domains when the view is not narrowed", async () => {
    await nlToSql("how many orders", "analyst", true, null);
    expect(body()).not.toHaveProperty("domains");
  });

  it("submitNlQuery sends the selection", async () => {
    await submitNlQuery("how many orders", "analyst", false, new Set(["sales"]));
    expect(body().domains).toEqual(["sales"]);
  });

  it("submitNlQuery sends no domains when the view is not narrowed", async () => {
    await submitNlQuery("how many orders", "analyst", false, null);
    expect(body()).not.toHaveProperty("domains");
  });
});
