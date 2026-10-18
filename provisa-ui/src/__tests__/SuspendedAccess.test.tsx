// Copyright (c) 2026 Kenneth Stott
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2076: an administrator sees what is suspended for naming a role that was not held, the event
// behind each suspension, and lifts it. Nothing lifts a suspension on a timer.

import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent, waitFor } from "../test-utils/render";
import { SuspendedAccess } from "../components/admin/SuspendedAccess";

const fetchSpy = vi.fn();
const liftSpy = vi.fn();
const statementSpy = vi.fn();

vi.mock("../api/admin", () => ({
  fetchOrgSuspensions: (...a: unknown[]) => fetchSpy(...(a as [])),
  liftOrgSuspension: (...a: unknown[]) => liftSpy(...(a as [])),
  fetchAuditedStatement: (...a: unknown[]) => statementSpy(...(a as [])),
}));

const EVENT = {
  id: 7,
  subject_id: "ada",
  created_at: "2026-10-18T10:00:00Z",
  detail: {
    severity: "high",
    principal: "ada",
    credential: "pat",
    credential_id: "hash-1",
    client: "10.0.0.5",
    transport: "pgwire",
    endpoint: "startup",
    requested_role: "admin",
    held_roles: ["analyst"],
    at: "2026-10-18T10:00:00Z",
    suspended: "token",
    recent_statement_ids: [41],
  },
};

const SUSPENDED = {
  tokens: [
    {
      token_hash: "hash-1",
      prefix: "provisa_pat_abcd",
      name: "ci",
      user_id: "ada",
      suspended_at: "2026-10-18T10:00:00Z",
      suspended_reason: "named 'admin'",
    },
  ],
  principals: [
    {
      user_id: "bo",
      token_access_suspended_at: "2026-10-18T11:00:00Z",
      token_access_suspended_reason: "named 'admin' over mcp",
    },
  ],
  events: [EVENT],
};

describe("SuspendedAccess", () => {
  beforeEach(() => {
    fetchSpy.mockReset().mockResolvedValue(SUSPENDED);
    liftSpy.mockReset().mockResolvedValue(undefined);
    statementSpy.mockReset();
  });

  it("lists each suspension with the event that caused it", async () => {
    render(<SuspendedAccess orgId="acme" onError={vi.fn()} />);
    const token = await screen.findByTestId("suspended-token-provisa_pat_abcd");
    expect(token).toHaveTextContent("Named role admin over pgwire (startup)");
    expect(token).toHaveTextContent("Holds: analyst");
    expect(screen.getByTestId("suspended-principal-bo")).toHaveTextContent("Token access");
  });

  it("lifts a token's suspension and reads the list again", async () => {
    render(<SuspendedAccess orgId="acme" onError={vi.fn()} />);
    fireEvent.click(await screen.findByTestId("suspended-lift-token-provisa_pat_abcd"));
    await waitFor(() => expect(liftSpy).toHaveBeenCalledWith("acme", { token_hash: "hash-1" }));
    await waitFor(() => expect(fetchSpy).toHaveBeenCalledTimes(2));
  });

  it("lifts a person's token-access suspension", async () => {
    render(<SuspendedAccess orgId="acme" onError={vi.fn()} />);
    fireEvent.click(await screen.findByTestId("suspended-lift-principal-bo"));
    await waitFor(() => expect(liftSpy).toHaveBeenCalledWith("acme", { user_id: "bo" }));
  });

  it("reads a recent statement under its own right, and says when that right is missing", async () => {
    statementSpy.mockRejectedValue(Object.assign(new Error("403"), { status: 403 }));
    render(<SuspendedAccess orgId="acme" onError={vi.fn()} />);
    fireEvent.click(await screen.findByTestId("suspended-statement-41"));
    expect(await screen.findByTestId("suspended-statement-text-41")).toHaveTextContent(
      "Reading a statement needs the view_governance right.",
    );
  });

  it("says when nothing is suspended", async () => {
    fetchSpy.mockResolvedValue({ tokens: [], principals: [], events: [] });
    render(<SuspendedAccess orgId="acme" onError={vi.fn()} />);
    expect(await screen.findByText("Nothing is suspended.")).toBeInTheDocument();
  });
});
