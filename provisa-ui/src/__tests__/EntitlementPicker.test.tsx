// Copyright (c) 2026 Kenneth Stott
// Canary: db7d742e-6e3e-4f06-a57d-ea7b45d802d0
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2219: the shared picker for a list's entries. * (everything of the kind, including what is
// added later) is ticked on the label's line and disables the pickers beneath it. Domains, tables
// and columns are picked in cascading searchable pickers, one per level the kind goes to; a domain
// or table picked with nothing chosen beneath it stands for all of it. A listed value the model
// does not hold is kept and shown flagged, never dropped.

import { describe, it, expect } from "vitest";
import { useState } from "react";
import userEvent from "@testing-library/user-event";
import { render, screen, within } from "../test-utils/render";
import type { KindInfo, NamesNode } from "../api/entitlements";
import { EntitlementPicker, PICKER_LIMIT } from "../components/entitlements/EntitlementPicker";

// The server's kinds and names (GET /admin/entitlements/kinds), as far as these tests use them.
const KINDS: Record<string, KindInfo> = {
  domains: { kind: "domains", semantic: true, tree_depth: 1 },
  tables: { kind: "tables", semantic: true, tree_depth: 2 },
  columns: { kind: "columns", semantic: true, tree_depth: 3 },
  roles: { kind: "roles", semantic: false, tree_depth: null },
  sources: { kind: "sources", semantic: false, tree_depth: null },
};
const n = (id: string, children: NamesNode[] = []): NamesNode => ({
  id,
  label: id.split(".").pop() ?? id,
  children,
});
const NAMES: NamesNode[] = [
  n("finance", [
    n("finance.costs", [n("finance.costs.amount")]),
    n("finance.revenue", [n("finance.revenue.amount"), n("finance.revenue.booked_at")]),
  ]),
  n("sales", [n("sales.orders", [n("sales.orders.total")])]),
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
        label="Entries"
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
const search = (level: string) => screen.getByTestId(`entitlement-picker-${level}-search`);
// The options of a level's open list (Mantine draws the list in a portal, hidden from role queries
// while it opens).
const options = (level: string) =>
  screen.findAllByTestId(new RegExp(`^entitlement-picker-${level}-option-`));
const optionLabels = async (level: string) => (await options(level)).map((o) => o.textContent);
/** Each option of a level's open list as "group: option". */
const grouped = async (level: string) =>
  (await options(level)).map(
    (o) =>
      `${o.closest(".mantine-Combobox-group")?.querySelector(".mantine-Combobox-groupLabel")?.textContent}: ${o.textContent}`,
  );

async function pick(level: string, id: string) {
  await userEvent.click(search(level));
  await userEvent.click(await screen.findByTestId(`entitlement-picker-${level}-option-${id}`));
  await userEvent.keyboard("{Escape}");
}

describe("the entitlement picker", () => {
  it("ticks everything on the label's line, beside the label", () => {
    render(<Harness kind="tables" initial={[]} />);
    const everything = screen.getByTestId("entitlement-picker-everything");
    const labelLine = screen.getByText("Entries").parentElement;
    expect(labelLine).toContainElement(everything);
    // Nothing stands between the label's line and the first picker.
    expect(labelLine?.nextElementSibling).toContainElement(search("domains"));
  });

  it("everything replaces the entries and disables the pickers; unticking gives them back", async () => {
    render(<Harness kind="tables" initial={["finance.revenue"]} />);
    await userEvent.click(screen.getByTestId("entitlement-picker-everything"));
    expect(shown()).toEqual(["*"]);
    expect(search("domains")).toBeDisabled();
    expect(search("tables")).toBeDisabled();
    await userEvent.click(screen.getByTestId("entitlement-picker-everything"));
    expect(shown()).toEqual(["finance.revenue"]);
    expect(search("domains")).toBeEnabled();
  });

  it("opens with everything ticked and the pickers disabled for a stored *", () => {
    render(<Harness kind="columns" initial={["*"]} />);
    expect(screen.getByTestId("entitlement-picker-everything")).toBeChecked();
    expect(search("domains")).toBeDisabled();
  });

  it("has one picker per level the kind goes to", () => {
    const { unmount } = render(<Harness kind="domains" initial={[]} />);
    expect(screen.queryByTestId("entitlement-picker-tables-search")).toBeNull();
    unmount();
    render(<Harness kind="tables" initial={[]} />);
    expect(search("tables")).toBeInTheDocument();
    expect(screen.queryByTestId("entitlement-picker-columns-search")).toBeNull();
  });

  it("a domain picked with nothing beneath it is all of it", async () => {
    render(<Harness kind="tables" initial={[]} />);
    await pick("domains", "finance");
    expect(shown()).toEqual(["finance"]);
    expect(screen.getByTestId("entitlement-picker-domains-pill-finance")).toBeInTheDocument();
  });

  it("with no domain chosen, the tables picker searches every table, grouped by domain", async () => {
    render(<Harness kind="tables" initial={[]} />);
    expect(search("tables")).toHaveAttribute("placeholder", "Search all tables");
    await userEvent.click(search("tables"));
    expect(await optionLabels("tables")).toEqual(["costs", "revenue", "orders"]);
    expect(await grouped("tables")).toEqual([
      "finance: costs",
      "finance: revenue",
      "sales: orders",
    ]);
    await userEvent.click(screen.getByTestId("entitlement-picker-tables-option-sales.orders"));
    expect(shown()).toEqual(["sales.orders"]);
    // Picking a table does not choose its domain: the tables stay searched across every domain.
    await userEvent.click(screen.getByTestId("entitlement-picker-tables-option-finance.costs"));
    expect(shown()).toEqual(["sales.orders", "finance.costs"]);
  });

  it("lists only the tables of the chosen domains, and a table chosen narrows its domain", async () => {
    render(<Harness kind="tables" initial={[]} />);
    await pick("domains", "finance");
    await userEvent.click(search("tables"));
    expect(await optionLabels("tables")).toEqual(["costs", "revenue"]);
    await userEvent.click(screen.getByTestId("entitlement-picker-tables-option-finance.revenue"));
    expect(shown()).toEqual(["finance.revenue"]);
  });

  it("lists only the columns of the chosen tables, grouped by table", async () => {
    render(<Harness kind="columns" initial={[]} />);
    expect(search("columns")).toBeDisabled();
    expect(search("columns")).toHaveAttribute("placeholder", "Choose tables to list their columns");
    await pick("tables", "finance.revenue");
    await pick("tables", "sales.orders");
    await userEvent.click(search("columns"));
    expect(await optionLabels("columns")).toEqual(["amount", "booked_at", "total"]);
    expect(await grouped("columns")).toEqual([
      "finance.revenue: amount",
      "finance.revenue: booked_at",
      "sales.orders: total",
    ]);
    await userEvent.click(
      screen.getByTestId("entitlement-picker-columns-option-sales.orders.total"),
    );
    expect(shown()).toEqual(["finance.revenue", "sales.orders.total"]);
  });

  it("claims the Escape that closes a list, so the panel around the form stays open", async () => {
    render(<Harness kind="tables" initial={[]} />);
    const seen: boolean[] = [];
    const listen = (e: KeyboardEvent) => e.key === "Escape" && seen.push(e.defaultPrevented);
    document.addEventListener("keydown", listen);
    await userEvent.click(search("tables"));
    await options("tables");
    await userEvent.keyboard("{Escape}");
    await userEvent.keyboard("{Escape}");
    document.removeEventListener("keydown", listen);
    // The first closes the list and is claimed; the second, with the list closed, is not.
    expect(seen).toEqual([true, false]);
  });

  it("searches as you type", async () => {
    render(<Harness kind="columns" initial={["finance.revenue", "sales.orders"]} />);
    await userEvent.type(search("columns"), "tot");
    expect(await optionLabels("columns")).toEqual(["total"]);
    await userEvent.clear(search("columns"));
    await userEvent.type(search("columns"), "zzz");
    expect(screen.getByText("Nothing matches")).toBeInTheDocument();
  });

  it("removing a domain removes what was chosen beneath it", async () => {
    render(<Harness kind="columns" initial={["finance.revenue.amount", "sales"]} />);
    expect(screen.getByTestId("entitlement-picker-domains-pill-finance")).toBeInTheDocument();
    expect(
      screen.getByTestId("entitlement-picker-tables-pill-finance.revenue"),
    ).toBeInTheDocument();
    await userEvent.click(screen.getByRole("button", { name: "Remove finance" }));
    expect(shown()).toEqual(["sales"]);
    expect(screen.queryByTestId("entitlement-picker-tables-pill-finance.revenue")).toBeNull();
  });

  it("an entry under another listed entry does not narrow it", () => {
    render(<Harness kind="tables" initial={["finance", "finance.revenue"]} />);
    expect(screen.getByTestId("entitlement-picker-domains-pill-finance")).toBeInTheDocument();
    expect(screen.queryByTestId("entitlement-picker-tables-pill-finance.revenue")).toBeNull();
  });

  it("keeps a value the model lacks as a flagged pill, said in the one tooltip, never dropped", async () => {
    render(<Harness kind="tables" initial={["finance.gone", "legacy"]} />);
    const gone = screen.getByTestId("entitlement-picker-tables-unknown-finance.gone");
    expect(screen.getByTestId("entitlement-picker-domains-unknown-legacy")).toBeInTheDocument();
    expect(gone).not.toHaveAttribute("title");
    await userEvent.hover(gone);
    expect(
      await screen.findByText("The model does not have this. It is kept until you remove it."),
    ).toBeInTheDocument();
    await pick("domains", "sales");
    expect(shown()).toEqual(["sales", "finance.gone", "legacy"]);
    await userEvent.click(screen.getByRole("button", { name: "Remove legacy" }));
    expect(shown()).toEqual(["sales", "finance.gone"]);
  });

  it("shows the model's own spelling, and matches a listed identifier ignoring case", async () => {
    const spelled: NamesNode[] = [
      n("Finance", [n("Finance.Revenue", [n("Finance.Revenue.AmountUSD")]), n("Finance.budget")]),
    ];
    render(<Harness kind="columns" initial={["finance.revenue"]} names={spelled} />);
    expect(
      screen.getByTestId("entitlement-picker-tables-pill-Finance.Revenue"),
    ).toBeInTheDocument();
    expect(screen.queryByTestId("entitlement-picker-tables-unknown-finance.revenue")).toBeNull();
    await pick("columns", "Finance.Revenue.AmountUSD");
    expect(shown()).toEqual(["Finance.Revenue.AmountUSD"]);
  });

  it("draws a bounded list of a long names list, and says a search narrows it", async () => {
    const many: NamesNode[] = Array.from({ length: 4 }, (_, d) =>
      n(
        `d${d}`,
        Array.from({ length: 80 }, (_, i) => n(`d${d}.t${String(i).padStart(3, "0")}`)),
      ),
    );
    render(<Harness kind="tables" initial={[]} names={many} />);
    await userEvent.click(search("tables"));
    expect(await options("tables")).toHaveLength(PICKER_LIMIT);
    expect(screen.getByTestId("entitlement-picker-tables-more")).toHaveTextContent(
      `Showing ${PICKER_LIMIT} of 320. Type to narrow.`,
    );
    await userEvent.type(search("tables"), "d3.t07");
    expect(await optionLabels("tables")).toHaveLength(10);
    expect(screen.queryByTestId("entitlement-picker-tables-more")).toBeNull();
  });

  it("says the model has nothing yet when no model is built, and keeps what is listed", () => {
    render(<Harness kind="tables" initial={["finance.revenue"]} names={null} />);
    expect(screen.getByText("The model has nothing of this kind yet.")).toBeInTheDocument();
    expect(
      within(screen.getByTestId("entitlement-picker-other")).getByText("finance.revenue"),
    ).toBeInTheDocument();
  });

  it("picks roles from the org's roles", async () => {
    render(<Harness kind="roles" initial={[]} />);
    await userEvent.click(screen.getByTestId("entitlement-picker-role-ops"));
    expect(shown()).toEqual(["ops"]);
  });

  it("types the entries of a kind the server picks from no tree", () => {
    render(<Harness kind="sources" initial={[]} />);
    expect(screen.getByTestId("entitlement-picker-typed")).toBeInTheDocument();
    expect(screen.queryByTestId("entitlement-picker-domains-search")).toBeNull();
  });
});
