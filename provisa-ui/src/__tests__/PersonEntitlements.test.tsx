// Copyright (c) 2026 Kenneth Stott
// Canary: 509272f3-3eed-4667-a1ca-68b1a0970644
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2219: a person's role grant on the Team page. Each list by kind with its origin; the
// identity provider's read-only, the list it superseded greyed; edited and ended with a reason
// through the one write API; read-only, saying why, when every list comes from the identity
// provider.

import { describe, it, expect, vi, beforeEach } from "vitest";
import userEvent from "@testing-library/user-event";
import { render, screen, waitFor, within } from "../test-utils/render";
import type { AccessView } from "../api/entitlements";

vi.mock("../api/entitlements", async (orig) => ({
  ...(await orig<typeof import("../api/entitlements")>()),
  fetchHeldLists: vi.fn(),
  fetchEffective: vi.fn(),
  setList: vi.fn(),
  endList: vi.fn(),
  searchPeople: vi.fn(),
}));

vi.mock("../hooks/useAdminQueries", () => ({
  useDomains: () => ({ domains: [{ id: "finance" }] }),
}));
vi.mock("../hooks/useTableQueries", () => ({
  useTables: () => ({
    tables: [
      { domainId: "finance", tableName: "revenue", columns: [] },
      { domainId: "finance", tableName: "costs", columns: [] },
    ],
  }),
}));

import {
  EntitlementError,
  endList,
  fetchEffective,
  fetchHeldLists,
  setList,
} from "../api/entitlements";
import { PersonEntitlements } from "../components/entitlements/PersonEntitlements";

const mockHeld = vi.mocked(fetchHeldLists);
const mockEffective = vi.mocked(fetchEffective);
const mockSet = vi.mocked(setList);
const mockEnd = vi.mocked(endList);

const MIXED: AccessView = {
  principal: "u|1",
  role_id: "viewer",
  lists: [
    {
      kind: "columns",
      identifiers: ["finance.revenue.amount"],
      origin: "idp",
      superseded: ["finance.costs.amount"],
      ended: [],
      set_by: null,
      reason: null,
    },
    {
      kind: "tables",
      identifiers: ["finance.revenue"],
      origin: "provisa",
      superseded: null,
      ended: ["finance.costs"],
      set_by: "admin|9",
      reason: "quarter close",
    },
  ],
  expiries: [
    {
      rule_key: "r1",
      ends_at: "2027-01-31T23:59:59+00:00",
      ends: {},
      list_kind: "tables",
      reason: "quarter close",
      passed: false,
    },
  ],
  grant_ended: false,
  findings: [],
};

const ALL_IDP: AccessView = { ...MIXED, lists: [MIXED.lists[0]], expiries: [] };

beforeEach(() => {
  vi.clearAllMocks();
  mockHeld.mockResolvedValue({
    principal: "u|1",
    lists: [
      {
        role_id: "viewer",
        kind: "tables",
        identifiers: ["finance.revenue"],
        capability: null,
        set_by: "admin|9",
        reason: "quarter close",
      },
    ],
  });
  mockEffective.mockResolvedValue(MIXED);
});

describe("a person's entitlement lists", () => {
  it("opens on the role with lists, showing each kind's origin, superseded list and end", async () => {
    render(<PersonEntitlements principal="u|1" roleIds={["viewer", "analyst"]} />);
    const tables = await screen.findByTestId("person-list-tables");
    expect(mockEffective).toHaveBeenCalledWith("u|1", "viewer");
    expect(within(tables).getByText("Set in Provisa")).toBeInTheDocument();
    expect(within(tables).getByText("2027-01-31")).toBeInTheDocument();
    expect(screen.getByTestId("person-list-tables-ended-finance.costs")).toHaveStyle({
      textDecoration: "line-through",
    });
    expect(within(tables).getByText("Set by admin|9: quarter close")).toBeInTheDocument();
    const columns = screen.getByTestId("person-list-columns");
    expect(within(columns).getByText("Identity provider")).toBeInTheDocument();
    expect(screen.getByTestId("person-list-columns-superseded")).toHaveTextContent(
      "Superseded: finance.costs.amount",
    );
    // The identity provider's list is changed there; Provisa's can be edited and ended.
    expect(screen.queryByTestId("person-list-columns-edit")).toBeNull();
    expect(screen.queryByTestId("person-list-columns-end")).toBeNull();
    expect(screen.getByTestId("person-list-tables-edit")).toBeInTheDocument();
    expect(screen.getByTestId("person-list-tables-end")).toBeInTheDocument();
    expect(screen.queryByTestId("person-entitlements-read-only")).toBeNull();
  });

  it("is read-only, and says why, when every list comes from the identity provider", async () => {
    mockEffective.mockResolvedValue(ALL_IDP);
    render(<PersonEntitlements principal="u|1" roleIds={["viewer"]} />);
    expect(await screen.findByTestId("person-entitlements-read-only")).toHaveTextContent(
      "comes from the identity provider",
    );
    expect(screen.queryByTestId("person-entitlements-add")).toBeNull();
    expect(screen.queryByTestId("person-list-columns-edit")).toBeNull();
  });

  it("edits a list with the tree picker and a reason, through the write API", async () => {
    mockSet.mockResolvedValue(MIXED);
    render(<PersonEntitlements principal="u|1" roleIds={["viewer"]} />);
    await userEvent.click(await screen.findByTestId("person-list-tables-edit"));
    const save = screen.getByTestId("person-entitlements-form-save");
    expect(save).toBeDisabled();
    await userEvent.click(screen.getByTestId("person-entitlements-form-picker-toggle-finance"));
    await userEvent.click(screen.getByTestId("person-entitlements-form-picker-node-finance.costs"));
    await userEvent.type(screen.getByTestId("person-entitlements-form-reason"), "audit");
    await userEvent.click(save);
    await waitFor(() =>
      expect(mockSet).toHaveBeenCalledWith({
        grantee: { principal: "u|1" },
        roleId: "viewer",
        kind: "tables",
        identifiers: ["finance.revenue", "finance.costs"],
        reason: "audit",
        expiresAt: new Date("2027-01-31T23:59:59").toISOString(),
      }),
    );
  });

  it("ends a list only with a reason", async () => {
    mockEnd.mockResolvedValue({ ...MIXED, lists: [MIXED.lists[0]] });
    render(<PersonEntitlements principal="u|1" roleIds={["viewer"]} />);
    await userEvent.click(await screen.findByTestId("person-list-tables-end"));
    const submit = screen.getByTestId("person-entitlements-end-submit");
    expect(submit).toBeDisabled();
    await userEvent.type(screen.getByTestId("person-entitlements-end-reason"), "left the team");
    await userEvent.click(submit);
    await waitFor(() =>
      expect(mockEnd).toHaveBeenCalledWith({
        grantee: { principal: "u|1" },
        roleId: "viewer",
        kind: "tables",
        reason: "left the team",
      }),
    );
    await waitFor(() => expect(screen.queryByTestId("person-list-tables")).toBeNull());
  });

  it("shows the server's refusal when the identity provider supersedes the write", async () => {
    mockSet.mockRejectedValue(
      new EntitlementError(409, "entitlements.idp_supersedes", "changed there, not here"),
    );
    render(<PersonEntitlements principal="u|1" roleIds={["viewer"]} />);
    await userEvent.click(await screen.findByTestId("person-list-tables-edit"));
    await userEvent.type(screen.getByTestId("person-entitlements-form-reason"), "x");
    await userEvent.click(screen.getByTestId("person-entitlements-form-save"));
    expect(await screen.findByTestId("person-entitlements-error")).toHaveTextContent(
      "changed there, not here",
    );
  });

  it("names a list for a kind that names nothing in the model", async () => {
    mockEffective.mockResolvedValue({
      ...MIXED,
      findings: [
        {
          code: "unknown_identifier",
          role_id: "viewer",
          kind: "tables",
          identifier: "finance.gone",
          message: "x",
        },
      ],
    });
    render(<PersonEntitlements principal="u|1" roleIds={["viewer"]} />);
    expect(await screen.findByTestId("person-entitlements-findings")).toHaveTextContent(
      "finance.gone in Tables names nothing in the model.",
    );
  });
});
