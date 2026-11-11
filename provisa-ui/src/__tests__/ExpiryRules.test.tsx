// Copyright (c) 2026 Kenneth Stott
// Canary: f70c350c-ce48-4457-9951-54dd40680f7c
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2219: a grant's end dates -- every rule with its date, what it ends in words and its reason,
// the ones to come soonest first, then the passed ones; a rule a Provisa list was set with is
// extended by setting the list again with the new date, through the one write API.

import { describe, it, expect, vi, beforeEach } from "vitest";
import userEvent from "@testing-library/user-event";
import { render, screen, waitFor, within } from "../test-utils/render";
import type { AccessView, ExpiryRuleView } from "../api/entitlements";

vi.mock("../api/entitlements", async (orig) => ({
  ...(await orig<typeof import("../api/entitlements")>()),
  setList: vi.fn(),
}));

import { setList } from "../api/entitlements";
import { ExpiryRules } from "../components/entitlements/ExpiryRules";
import { untilEndOf } from "../lib/grantUntil";

const mockSet = vi.mocked(setList);

const rule = (over: Partial<ExpiryRuleView>): ExpiryRuleView => ({
  rule_key: "r",
  ends_at: "2027-01-31T23:59:59+00:00",
  ends: {},
  list_kind: null,
  reason: null,
  passed: false,
  ...over,
});

const VIEW: AccessView = {
  principal: "u|1",
  role_id: "viewer",
  lists: [
    {
      kind: "tables",
      identifiers: ["finance.revenue", "sales.orders"],
      origin: "provisa",
      superseded: null,
      ended: [],
      set_by: "admin|9",
      reason: "quarter close",
    },
    {
      kind: "columns",
      identifiers: ["finance.revenue.amount"],
      origin: "idp",
      superseded: null,
      ended: [],
      set_by: null,
      reason: null,
    },
  ],
  expiries: [
    rule({
      rule_key: "late",
      ends_at: "2027-06-30T23:59:59+00:00",
      ends: { "*": ["finance.revenue", "sales.orders"] },
      list_kind: "tables",
      reason: "quarter close",
    }),
    rule({ rule_key: "gone", ends_at: "2026-01-01T00:00:00+00:00", passed: true, reason: "left" }),
    rule({
      rule_key: "soon",
      ends_at: "2026-12-01T23:59:59+00:00",
      ends: { export: ["*"] },
      reason: "audit",
    }),
    rule({
      rule_key: "idp",
      ends_at: "2027-03-01T23:59:59+00:00",
      ends: { "*": ["finance.revenue.amount"] },
      list_kind: "columns",
    }),
  ],
  grant_ended: false,
  findings: [],
};

function renderRules(view: AccessView = VIEW, editable = true) {
  const onSaved = vi.fn();
  const onError = vi.fn();
  render(
    <ExpiryRules
      principal="u|1"
      view={view}
      editable={editable}
      onSaved={onSaved}
      onError={onError}
    />,
  );
  return { onSaved, onError };
}

beforeEach(() => vi.clearAllMocks());

describe("a grant's end dates", () => {
  it("lists every rule, the ones to come soonest first, then the passed ones", () => {
    renderRules();
    const rows = within(screen.getByTestId("expiry-rules-table"))
      .getAllByRole("row")
      .slice(1)
      .map((r) => r.getAttribute("data-testid"));
    expect(rows).toEqual([
      "expiry-rule-soon",
      "expiry-rule-idp",
      "expiry-rule-late",
      "expiry-rule-gone",
    ]);
    expect(screen.getByTestId("expiry-rule-gone")).toHaveTextContent("Passed");
    expect(screen.getByTestId("expiry-rule-soon")).not.toHaveTextContent("Passed");
  });

  it("says what each rule ends in words, with its date and reason", () => {
    renderRules();
    const late = screen.getByTestId("expiry-rule-late");
    expect(late).toHaveTextContent("2027-06-30");
    expect(late).toHaveTextContent("Every capability on finance.revenue, sales.orders");
    expect(late).toHaveTextContent("quarter close");
    expect(screen.getByTestId("expiry-rule-soon")).toHaveTextContent("export everywhere");
    expect(screen.getByTestId("expiry-rule-gone")).toHaveTextContent("The whole grant");
  });

  it("offers extend only on a rule to come that a Provisa list was set with", () => {
    renderRules();
    expect(screen.getByTestId("expiry-rule-late-extend")).toBeInTheDocument();
    // Set by a rule of its own, on the identity provider's list, or passed: not moved here.
    expect(screen.queryByTestId("expiry-rule-soon-extend")).toBeNull();
    expect(screen.queryByTestId("expiry-rule-idp-extend")).toBeNull();
    expect(screen.queryByTestId("expiry-rule-gone-extend")).toBeNull();
  });

  it("offers nothing to change where the grant is read-only", () => {
    renderRules(VIEW, false);
    expect(screen.getByTestId("expiry-rule-late")).toBeInTheDocument();
    expect(screen.queryByTestId("expiry-rule-late-extend")).toBeNull();
  });

  it("says so when the grant has no end dates", () => {
    renderRules({ ...VIEW, expiries: [] });
    expect(screen.getByTestId("expiry-rules-none")).toHaveTextContent(
      "No end dates: this grant lasts until it is ended.",
    );
  });

  it("extends by setting the list again, entries unchanged, with the new date and a reason", async () => {
    const saved = { ...VIEW, expiries: [] };
    mockSet.mockResolvedValue(saved);
    const { onSaved } = renderRules();
    await userEvent.click(screen.getByTestId("expiry-rule-late-extend"));
    expect(screen.getByTestId("expiry-extend")).toHaveTextContent(
      "Move the end of the Tables list, now 2027-06-30.",
    );
    const save = screen.getByTestId("expiry-extend-save");
    expect(save).toBeDisabled();
    await userEvent.type(screen.getByTestId("expiry-extend-date"), "2027-09-30");
    await userEvent.tab();
    await userEvent.type(screen.getByTestId("expiry-extend-reason"), "audit extended");
    await waitFor(() => expect(save).toBeEnabled());
    await userEvent.click(save);
    await waitFor(() =>
      expect(mockSet).toHaveBeenCalledWith({
        grantee: { principal: "u|1" },
        roleId: "viewer",
        kind: "tables",
        identifiers: ["finance.revenue", "sales.orders"],
        reason: "audit extended",
        expiresAt: untilEndOf("2027-09-30"),
      }),
    );
    expect(onSaved).toHaveBeenCalledWith(saved);
    expect(screen.queryByTestId("expiry-extend")).toBeNull();
  });

  it("hands a refused extend to the panel and keeps the form open", async () => {
    const refused = new Error("An expiry must be in the future");
    mockSet.mockRejectedValue(refused);
    const { onError, onSaved } = renderRules();
    await userEvent.click(screen.getByTestId("expiry-rule-late-extend"));
    await userEvent.type(screen.getByTestId("expiry-extend-date"), "2027-09-30");
    await userEvent.tab();
    await userEvent.type(screen.getByTestId("expiry-extend-reason"), "why");
    await userEvent.click(screen.getByTestId("expiry-extend-save"));
    await waitFor(() => expect(onError).toHaveBeenCalledWith(refused));
    expect(onSaved).not.toHaveBeenCalled();
    expect(screen.getByTestId("expiry-extend")).toBeInTheDocument();
  });
});
