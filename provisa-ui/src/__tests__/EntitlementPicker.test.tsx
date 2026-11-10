// Copyright (c) 2026 Kenneth Stott
// Canary: db7d742e-6e3e-4f06-a57d-ea7b45d802d0
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2219: the shared picker for a list's entries. * is everything of the kind including what is
// added later; a node checked by its parent shows checked; a listed value the tree does not hold is
// kept and shown, never dropped.

import { describe, it, expect, vi } from "vitest";
import { useState } from "react";
import userEvent from "@testing-library/user-event";
import { render, screen, within } from "../test-utils/render";
import type { EntitlementKind } from "../components/entitlements/kinds";

vi.mock("../hooks/useAdminQueries", () => ({
  useDomains: () => ({ domains: [{ id: "finance" }] }),
}));
vi.mock("../hooks/useTableQueries", () => ({
  useTables: () => ({
    tables: [{ domainId: "finance", tableName: "revenue", columns: [{ columnName: "amount" }] }],
  }),
}));

import { EntitlementPicker } from "../components/entitlements/EntitlementPicker";

function Harness({ kind, initial }: { kind: EntitlementKind; initial: string[] }) {
  const [value, setValue] = useState(initial);
  return (
    <>
      <EntitlementPicker kind={kind} value={value} onChange={setValue} roleIds={["dev", "ops"]} />
      <output data-testid="value">{JSON.stringify(value)}</output>
    </>
  );
}

const shown = () => JSON.parse(screen.getByTestId("value").textContent ?? "null");

describe("the entitlement picker", () => {
  it("everything replaces the entries, and hides the tree", async () => {
    render(<Harness kind="tables" initial={["finance.revenue"]} />);
    await userEvent.click(screen.getByTestId("entitlement-picker-everything"));
    expect(shown()).toEqual(["*"]);
    expect(screen.queryByTestId("entitlement-picker-filter")).toBeNull();
  });

  it("shows a child of a checked domain as checked, and keeps values the tree lacks", async () => {
    render(<Harness kind="columns" initial={["finance", "legacy.*"]} />);
    await userEvent.click(screen.getByTestId("entitlement-picker-toggle-finance"));
    await userEvent.click(screen.getByTestId("entitlement-picker-toggle-finance.revenue"));
    expect(screen.getByTestId("entitlement-picker-node-finance.revenue.amount")).toBeChecked();
    expect(
      within(screen.getByTestId("entitlement-picker-other")).getByText("legacy.*"),
    ).toBeInTheDocument();
    await userEvent.click(screen.getByRole("button", { name: "Remove legacy.*" }));
    expect(shown()).toEqual(["finance"]);
  });

  it("picks roles from the org's roles", async () => {
    render(<Harness kind="roles" initial={[]} />);
    await userEvent.click(screen.getByTestId("entitlement-picker-role-ops"));
    expect(shown()).toEqual(["ops"]);
  });
});
