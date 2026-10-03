// REQ-273, REQ-1620: the acting role travels only in X-Provisa-Role. Under "Role: All" that
// header is the comma-separated set; a body `role` repeating it differs from the server's acting
// role (one member of the set) and is refused as data.role_mismatch, so no request body carries it.
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { explainSql, nlToSql, runSql, submitNlQuery } from "../admin";

const ROLE_SET = "analyst,org_admin";

let fetchMock: ReturnType<typeof vi.fn>;

beforeEach(() => {
  fetchMock = vi.fn().mockResolvedValue(
    new Response(JSON.stringify({ data: { sql: [] }, columns: [], job_id: "j1", sql: "" }), {
      status: 200,
      headers: { "Content-Type": "application/json" },
    }),
  );
  vi.stubGlobal("fetch", fetchMock);
});

afterEach(() => {
  vi.unstubAllGlobals();
});

function sent(): { headers: Record<string, string>; body: Record<string, unknown> } {
  const init = fetchMock.mock.calls[0][1] as RequestInit;
  return {
    headers: init.headers as Record<string, string>,
    body: JSON.parse(init.body as string) as Record<string, unknown>,
  };
}

describe("the acting role is sent in the header, never in the body", () => {
  it("runSql", async () => {
    await runSql("select 1", ROLE_SET);
    const { headers, body } = sent();
    expect(headers["X-Provisa-Role"]).toBe(ROLE_SET);
    expect(body).not.toHaveProperty("role");
  });

  it("explainSql", async () => {
    await explainSql("select 1", ROLE_SET);
    const { headers, body } = sent();
    expect(headers["X-Provisa-Role"]).toBe(ROLE_SET);
    expect(body).not.toHaveProperty("role");
  });

  it("nlToSql", async () => {
    await nlToSql("how many orders", ROLE_SET);
    const { headers, body } = sent();
    expect(headers["X-Provisa-Role"]).toBe(ROLE_SET);
    expect(body).not.toHaveProperty("role");
  });

  it("submitNlQuery", async () => {
    await submitNlQuery("how many orders", ROLE_SET);
    const { headers, body } = sent();
    expect(headers["X-Provisa-Role"]).toBe(ROLE_SET);
    expect(body).not.toHaveProperty("role");
  });
});
