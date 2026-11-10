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

import { describe, it, expect } from "vitest";
import { useState } from "react";
import userEvent from "@testing-library/user-event";
import { render, screen, within } from "../test-utils/render";
import type { KindInfo, NamesNode } from "../api/entitlements";
import { EntitlementPicker } from "../components/entitlements/EntitlementPicker";

// The server's kinds and names (GET /admin/entitlements/kinds), as far as these tests use them.
const KINDS: Record<string, KindInfo> = {
  tables: { kind: "tables", semantic: true, tree_depth: 2 },
  columns: { kind: "columns", semantic: true, tree_depth: 3 },
  roles: { kind: "roles", semantic: false, tree_depth: null },
  sources: { kind: "sources", semantic: false, tree_depth: null },
};
const NAMES: NamesNode[] = [
  {
    id: "finance",
    label: "finance",
    children: [
      {
        id: "finance.revenue",
        label: "revenue",
        children: [{ id: "finance.revenue.amount", label: "amount", children: [] }],
      },
    ],
  },
];

function Harness({
  kind,
  initial,
  names = NAMES,
}: {
  kind: string;
  initial: string[];
  names?: NamesNode[] | null;
}) {
  const [value, setValue] = useState(initial);
  return (
    <>
      <EntitlementPicker
        kind={KINDS[kind]}
        names={names}
        value={value}
        onChange={setValue}
        roleIds={["dev", "ops"]}
      />
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

  it("says a child is included by its parent in the one tooltip, not a native title", async () => {
    render(<Harness kind="columns" initial={["finance"]} />);
    await userEvent.click(screen.getByTestId("entitlement-picker-toggle-finance"));
    const child = screen.getByTestId("entitlement-picker-node-finance.revenue");
    expect(child).not.toHaveAttribute("title");
    await userEvent.hover(child);
    expect(await screen.findByText("Included by the entry above it")).toBeInTheDocument();
  });

  it("picks roles from the org's roles", async () => {
    render(<Harness kind="roles" initial={[]} />);
    await userEvent.click(screen.getByTestId("entitlement-picker-role-ops"));
    expect(shown()).toEqual(["ops"]);
  });

  it("picks from the server's names to the depth its kind goes", async () => {
    render(<Harness kind="tables" initial={[]} />);
    await userEvent.click(screen.getByTestId("entitlement-picker-toggle-finance"));
    await userEvent.click(screen.getByTestId("entitlement-picker-node-finance.revenue"));
    expect(shown()).toEqual(["finance.revenue"]);
    // tables go two levels deep: a table has no column beneath it here
    expect(screen.queryByTestId("entitlement-picker-toggle-finance.revenue")).toBeNull();
  });

  it("shows the model's own spelling, and matches a listed identifier ignoring case", async () => {
    const spelled: NamesNode[] = [
      {
        id: "Finance",
        label: "Finance",
        children: [
          {
            id: "Finance.Revenue",
            label: "Revenue",
            children: [{ id: "Finance.Revenue.AmountUSD", label: "AmountUSD", children: [] }],
          },
          { id: "Finance.budget", label: "budget", children: [] },
        ],
      },
    ];
    render(<Harness kind="columns" initial={["finance.revenue"]} names={spelled} />);
    expect(screen.getByLabelText("Finance")).toBeInTheDocument();
    await userEvent.click(screen.getByTestId("entitlement-picker-toggle-Finance"));
    expect(screen.getByLabelText("Revenue")).toBeChecked();
    expect(screen.queryByLabelText("revenue")).toBeNull();
    // A listed identifier the tree holds is not shown as one it lacks, whatever its case.
    expect(screen.queryByTestId("entitlement-picker-other")).toBeNull();
    await userEvent.click(screen.getByTestId("entitlement-picker-toggle-Finance.Revenue"));
    expect(screen.getByLabelText("AmountUSD")).toBeChecked();
    // Checking the domain replaces the listed table it now covers, spelled as the model spells it.
    await userEvent.click(screen.getByTestId("entitlement-picker-node-Finance"));
    expect(shown()).toEqual(["Finance"]);
  });

  it("says the model has nothing yet when no model is built", () => {
    render(<Harness kind="tables" initial={[]} names={null} />);
    expect(screen.getByText("The model has nothing of this kind yet.")).toBeInTheDocument();
  });

  it("types the entries of a kind the server picks from no tree", async () => {
    render(<Harness kind="sources" initial={[]} />);
    expect(screen.getByTestId("entitlement-picker-typed")).toBeInTheDocument();
    expect(screen.queryByTestId("entitlement-picker-filter")).toBeNull();
  });
});
