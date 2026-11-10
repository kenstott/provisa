// Copyright (c) 2026 Kenneth Stott
// Canary: 57e97027-e07c-4b82-adcf-e3314f6c0235
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2219 (ONE API, THREE WAYS TO USE IT): grants from the Team page. The people search finds a
// person by name or email and the write names their principal; a typed email that matches no one
// is written as a pending grant, listed until it attaches, and can be ended with a reason.

import { describe, it, expect, vi, beforeEach } from "vitest";
import userEvent from "@testing-library/user-event";
import { render, screen, waitFor, within } from "../test-utils/render";
import type { PendingGrant } from "../api/entitlements";

vi.mock("../api/entitlements", async (orig) => ({
  ...(await orig<typeof import("../api/entitlements")>()),
  fetchPending: vi.fn(),
  searchPeople: vi.fn(),
  setList: vi.fn(),
  endList: vi.fn(),
}));
vi.mock("../hooks/useAdminQueries", () => ({ useDomains: () => ({ domains: [] }) }));
vi.mock("../hooks/useTableQueries", () => ({ useTables: () => ({ tables: [] }) }));
vi.mock("@mantine/notifications", () => ({ notifications: { show: vi.fn() } }));

import { endList, fetchPending, searchPeople, setList } from "../api/entitlements";
import { EntitlementGrants } from "../components/entitlements/EntitlementGrants";

const mockPending = vi.mocked(fetchPending);
const mockSearch = vi.mocked(searchPeople);
const mockSet = vi.mocked(setList);
const mockEnd = vi.mocked(endList);

const WAITING: PendingGrant = {
  pending: true,
  email: "new@acme.example",
  role_id: "viewer",
  kind: "tables",
  identifiers: ["finance.revenue"],
  expires_at: null,
  set_by: "admin|9",
  reason: "onboarding",
};

beforeEach(() => {
  vi.clearAllMocks();
  mockPending.mockResolvedValue([WAITING]);
  mockSearch.mockResolvedValue([]);
});

async function pickOption(testId: string, text: string) {
  await userEvent.click(screen.getByTestId(testId));
  await userEvent.click(await screen.findByRole("option", { name: text, hidden: true }));
}

describe("granting access from the Team page", () => {
  it("lists the grants waiting on an email", async () => {
    render(<EntitlementGrants roleIds={["viewer"]} />);
    const row = await screen.findByTestId("entitlement-pending-new@acme.example|viewer|tables");
    expect(within(row).getByText("finance.revenue")).toBeInTheDocument();
    expect(within(row).getByText("onboarding")).toBeInTheDocument();
  });

  it("grants to a person the search finds, by their principal", async () => {
    mockSearch.mockResolvedValue([
      { principal: "okta|31", name: "Kenji Sato", email: "kenji@acme.example" },
    ]);
    mockSet.mockResolvedValue({
      principal: "okta|31",
      role_id: "viewer",
      lists: [],
      expiries: [],
      grant_ended: false,
      findings: [],
    });
    render(<EntitlementGrants roleIds={["viewer"]} />);
    await userEvent.click(await screen.findByTestId("entitlement-grant-open"));
    await userEvent.type(screen.getByTestId("entitlement-grant-form-person"), "kenji");
    await userEvent.click(
      await screen.findByRole("option", { name: "Kenji Sato · kenji@acme.example", hidden: true }),
    );
    expect(mockSearch).toHaveBeenCalledWith("kenji");
    await pickOption("entitlement-grant-form-role", "viewer");
    await pickOption("entitlement-grant-form-kind", "Sources");
    await userEvent.type(
      screen.getByTestId("entitlement-grant-form-picker-typed"),
      "pg-main{Enter}",
    );
    await userEvent.type(screen.getByTestId("entitlement-grant-form-reason"), "new project");
    await userEvent.click(screen.getByTestId("entitlement-grant-form-save"));
    await waitFor(() =>
      expect(mockSet).toHaveBeenCalledWith({
        grantee: { principal: "okta|31" },
        roleId: "viewer",
        kind: "sources",
        identifiers: ["pg-main"],
        reason: "new project",
        expiresAt: null,
      }),
    );
  });

  it("grants to a typed email no one has, as a pending grant", async () => {
    mockSet.mockResolvedValue({ ...WAITING, email: "later@acme.example", kind: "roles" });
    render(<EntitlementGrants roleIds={["viewer", "analyst"]} />);
    await userEvent.click(await screen.findByTestId("entitlement-grant-open"));
    await userEvent.type(screen.getByTestId("entitlement-grant-form-person"), "later@acme.example");
    await userEvent.click(
      await screen.findByRole("option", {
        name: "later@acme.example (waits for their first sign-in)",
        hidden: true,
      }),
    );
    await pickOption("entitlement-grant-form-role", "viewer");
    await pickOption("entitlement-grant-form-kind", "Roles");
    await userEvent.click(screen.getByTestId("entitlement-grant-form-picker-role-analyst"));
    await userEvent.type(screen.getByTestId("entitlement-grant-form-reason"), "starts monday");
    await userEvent.click(screen.getByTestId("entitlement-grant-form-save"));
    await waitFor(() =>
      expect(mockSet).toHaveBeenCalledWith({
        grantee: { email: "later@acme.example" },
        roleId: "viewer",
        kind: "roles",
        identifiers: ["analyst"],
        reason: "starts monday",
        expiresAt: null,
      }),
    );
    expect(mockPending).toHaveBeenCalledTimes(2);
  });

  it("ends a waiting grant with a reason", async () => {
    mockEnd.mockResolvedValue({ ...WAITING, pending: false });
    render(<EntitlementGrants roleIds={["viewer"]} />);
    await userEvent.click(
      await screen.findByTestId("entitlement-pending-end-new@acme.example|viewer|tables"),
    );
    const submit = screen.getByTestId("entitlement-pending-end-submit");
    expect(submit).toBeDisabled();
    await userEvent.type(screen.getByTestId("entitlement-pending-end-reason"), "offer withdrawn");
    await userEvent.click(submit);
    await waitFor(() =>
      expect(mockEnd).toHaveBeenCalledWith({
        grantee: { email: "new@acme.example" },
        roleId: "viewer",
        kind: "tables",
        reason: "offer withdrawn",
      }),
    );
  });
});
