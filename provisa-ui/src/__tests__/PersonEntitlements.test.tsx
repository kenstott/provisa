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
  fetchKinds: vi.fn(),
  fetchPersonRoles: vi.fn(),
  setList: vi.fn(),
  endList: vi.fn(),
  searchPeople: vi.fn(),
}));

import {
  EntitlementError,
  endList,
  fetchEffective,
  fetchHeldLists,
  fetchKinds,
  fetchPersonRoles,
  setList,
} from "../api/entitlements";
import { PersonEntitlements } from "../components/entitlements/PersonEntitlements";
import { SERVER_KINDS } from "../test-utils/entitlementKinds";

const mockHeld = vi.mocked(fetchHeldLists);
const mockEffective = vi.mocked(fetchEffective);
const mockSet = vi.mocked(setList);
const mockEnd = vi.mocked(endList);
const mockKinds = vi.mocked(fetchKinds);
const mockRoles = vi.mocked(fetchPersonRoles);

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
  mockKinds.mockResolvedValue(SERVER_KINDS);
  mockRoles.mockResolvedValue({
    principal: "u|1",
    roles: [
      { role_id: "auditor", origins: ["idp"], domains: [] },
      { role_id: "viewer", origins: ["provisa"], domains: ["*"] },
    ],
  });
});

/** The options of the select `testId`, from its own listbox (others stay rendered, hidden). */
async function optionsOf(testId: string): Promise<string[]> {
  const input = await screen.findByTestId(testId);
  await userEvent.click(input);
  const listbox = await waitFor(() => {
    const el = document.getElementById(input.getAttribute("aria-controls") ?? "");
    if (el === null) throw new Error(`${testId} has no listbox`);
    return el;
  });
  return within(listbox)
    .getAllByRole("option", { hidden: true })
    .map((o) => o.textContent ?? "");
}

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

  it("explains an ended entry and a superseded list with the shared hint, not a native title", async () => {
    render(<PersonEntitlements principal="u|1" roleIds={["viewer"]} />);
    const ended = await screen.findByTestId("person-list-tables-ended-finance.costs");
    const superseded = screen.getByTestId("person-list-columns-superseded");
    expect(ended).not.toHaveAttribute("title");
    expect(superseded).not.toHaveAttribute("title");
    await userEvent.hover(ended);
    expect(await screen.findByRole("tooltip")).toHaveTextContent("Ended by a passed end date");
    await userEvent.unhover(ended);
    await waitFor(() => expect(screen.queryByRole("tooltip")).toBeNull());
    await userEvent.hover(superseded);
    expect(await screen.findByRole("tooltip")).toHaveTextContent("The list this one replaced");
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

  it("edits a list with the cascading pickers and a reason, through the write API", async () => {
    mockSet.mockResolvedValue(MIXED);
    render(<PersonEntitlements principal="u|1" roleIds={["viewer"]} />);
    await userEvent.click(await screen.findByTestId("person-list-tables-edit"));
    const save = screen.getByTestId("person-entitlements-form-save");
    expect(save).toBeDisabled();
    // The listed table shows its domain chosen, so the tables offered are that domain's.
    await userEvent.click(
      await screen.findByTestId("person-entitlements-form-picker-tables-search"),
    );
    await userEvent.click(
      await screen.findByTestId("person-entitlements-form-picker-tables-option-finance.costs"),
    );
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

  it("offers the roles the person holds, each with its origin, and no other role", async () => {
    render(<PersonEntitlements principal="u|1" roleIds={["viewer", "analyst", "auditor"]} />);
    await screen.findByTestId("person-list-tables");
    expect(mockRoles).toHaveBeenCalledWith("u|1");
    expect(await optionsOf("person-entitlements-role")).toEqual([
      "viewer · Assigned in Provisa (lists set)",
      "auditor · Identity provider",
    ]);
  });

  it("opens on the first role held when Provisa holds no lists for the person", async () => {
    mockHeld.mockResolvedValue({ principal: "u|1", lists: [] });
    render(<PersonEntitlements principal="u|1" roleIds={["viewer"]} />);
    await waitFor(() => expect(mockEffective).toHaveBeenCalledWith("u|1", "auditor"));
  });

  it("still offers a role Provisa holds lists for that the person no longer holds", async () => {
    mockRoles.mockResolvedValue({
      principal: "u|1",
      roles: [{ role_id: "auditor", origins: ["idp"], domains: [] }],
    });
    render(<PersonEntitlements principal="u|1" roleIds={["viewer", "auditor"]} />);
    await waitFor(() => expect(mockEffective).toHaveBeenCalledWith("u|1", "auditor"));
    expect(await optionsOf("person-entitlements-role")).toEqual([
      "auditor · Identity provider",
      "viewer (no longer held; lists set)",
    ]);
  });

  it("says so when the person holds no role the caller administers", async () => {
    mockHeld.mockResolvedValue({ principal: "u|1", lists: [] });
    mockRoles.mockResolvedValue({ principal: "u|1", roles: [] });
    render(<PersonEntitlements principal="u|1" roleIds={["viewer"]} />);
    expect(await screen.findByTestId("person-entitlements-no-roles")).toHaveTextContent(
      "This person holds no role you administer.",
    );
    expect(mockEffective).not.toHaveBeenCalled();
  });

  it("adds a list of a kind the server defines, and only those", async () => {
    mockKinds.mockResolvedValue({
      kinds: [
        { kind: "tables", semantic: true, tree_depth: 2 },
        { kind: "sources", semantic: false, tree_depth: null },
      ],
      names: SERVER_KINDS.names,
    });
    mockEffective.mockResolvedValue({ ...MIXED, lists: [], expiries: [] });
    render(<PersonEntitlements principal="u|1" roleIds={["viewer"]} />);
    await userEvent.click(await screen.findByTestId("person-entitlements-add"));
    expect(await optionsOf("person-entitlements-form-kind")).toEqual(["Tables", "Sources"]);
  });
});
