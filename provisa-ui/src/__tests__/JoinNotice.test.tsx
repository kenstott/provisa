// Copyright (c) 2026 Kenneth Stott
// Canary: 9d4c2b71-6e08-4a53-b1f7-3c05e8a7d926
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1478: an org a user was put into — by their email domain or by an administrator — is
// explained the next time they sign in, and they are offered the way out. An org they created is
// never announced; one they were invited to is explained but has no exit, since accepting the
// invitation was their own act.
import { describe, it, expect, vi, beforeEach } from "vitest";
import userEvent from "@testing-library/user-event";
import { render, screen, waitFor } from "../test-utils/render";
import { JoinNotice } from "../components/JoinNotice";
import type { OrgMembership } from "../types/auth";

vi.mock("../api/admin", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../api/admin")>()),
  acknowledgeJoin: vi.fn(),
  leaveOrg: vi.fn(),
}));

const mockAuth: { orgMemberships: OrgMembership[]; refresh: ReturnType<typeof vi.fn> } = {
  orgMemberships: [],
  refresh: vi.fn(),
};
vi.mock("../context/AuthContext", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../context/AuthContext")>()),
  useAuth: () => mockAuth,
}));

import { acknowledgeJoin, leaveOrg } from "../api/admin";

const mockAcknowledge = vi.mocked(acknowledgeJoin);
const mockLeave = vi.mocked(leaveOrg);

beforeEach(() => {
  vi.clearAllMocks();
  mockAcknowledge.mockResolvedValue(undefined);
  mockLeave.mockResolvedValue(undefined);
  mockAuth.orgMemberships = [];
  mockAuth.refresh = vi.fn();
});

function membership(over: Partial<OrgMembership>): OrgMembership {
  return {
    org_id: "acme",
    org_name: "Acme",
    joined_via: "auto_join",
    acknowledged: false,
    ...over,
  };
}

describe("JoinNotice", () => {
  it("explains an org joined by email address", () => {
    mockAuth.orgMemberships = [membership({})];
    render(<JoinNotice />);
    expect(screen.getByTestId("join-notice-reason").textContent).toContain("email address");
    expect(screen.getByTestId("join-notice-reason").textContent).toContain("Acme");
  });

  it("says nothing about an org the user created", () => {
    mockAuth.orgMemberships = [membership({ joined_via: "created", acknowledged: true })];
    render(<JoinNotice />);
    expect(screen.queryByTestId("join-notice-modal")).toBeNull();
  });

  it("says nothing once the notice has been acknowledged", () => {
    mockAuth.orgMemberships = [membership({ acknowledged: true })];
    render(<JoinNotice />);
    expect(screen.queryByTestId("join-notice-modal")).toBeNull();
  });

  it("says nothing for a membership written before provenance was recorded", () => {
    mockAuth.orgMemberships = [membership({ joined_via: null })];
    render(<JoinNotice />);
    expect(screen.queryByTestId("join-notice-modal")).toBeNull();
  });

  it("acknowledges the org the notice is about, then re-reads the identity", async () => {
    mockAuth.orgMemberships = [membership({ org_id: "acme" })];
    render(<JoinNotice />);
    await userEvent.click(screen.getByTestId("join-notice-ack"));
    await waitFor(() => expect(mockAcknowledge).toHaveBeenCalledWith("acme"));
    expect(mockAuth.refresh).toHaveBeenCalled();
  });

  it("offers the way out of an org the user did not ask to be in", async () => {
    mockAuth.orgMemberships = [membership({ org_id: "acme" })];
    render(<JoinNotice />);
    await userEvent.click(screen.getByTestId("join-notice-leave"));
    await waitFor(() => expect(mockLeave).toHaveBeenCalledWith("acme"));
    expect(mockAuth.refresh).toHaveBeenCalled();
  });

  it("explains an invitation but offers no exit, since accepting it was the user's own act", () => {
    mockAuth.orgMemberships = [membership({ joined_via: "invite" })];
    render(<JoinNotice />);
    expect(screen.getByTestId("join-notice-reason").textContent).toContain("invitation");
    expect(screen.queryByTestId("join-notice-leave")).toBeNull();
  });

  it("names the administrator path", () => {
    mockAuth.orgMemberships = [membership({ joined_via: "admin" })];
    render(<JoinNotice />);
    expect(screen.getByTestId("join-notice-reason").textContent).toContain("administrator");
    expect(screen.getByTestId("join-notice-leave")).toBeTruthy();
  });

  it("keeps the notice up and reports why when leaving fails", async () => {
    mockAuth.orgMemberships = [membership({})];
    mockLeave.mockRejectedValue(new Error("last admin cannot leave"));
    render(<JoinNotice />);
    await userEvent.click(screen.getByTestId("join-notice-leave"));
    await waitFor(() =>
      expect(screen.getByTestId("join-notice-error").textContent).toContain(
        "last admin cannot leave",
      ),
    );
    expect(mockAuth.refresh).not.toHaveBeenCalled();
  });
});
