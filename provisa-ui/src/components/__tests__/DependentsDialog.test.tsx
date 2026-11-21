// Copyright (c) 2026 Kenneth Stott
// Canary: d6b165ac-6936-4f26-9189-eb76f7772fb0
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// A delete is refused while anything depends on the object (REQ-1918). The server lists what
// still refers to it; the dialog shows each, grouped by kind and named as its own page names it,
// as a link into its detail panel there (REQ-2240).

import { describe, it, expect, vi } from "vitest";
import { render, screen, fireEvent, within } from "../../test-utils/render";
import { DependentsDialog } from "../DependentsDialog";
import { dependentsOf } from "../../lib/dependents";
import { commandId, detailHref } from "../list/useDetailParam";

const DEPENDENTS = [
  {
    kind: "column",
    id: 41,
    name: "orders.amount",
    label: "orders.amount",
    via: ["table_columns.visible_to"],
    table_id: 3,
  },
  {
    kind: "column",
    id: 42,
    name: "orders.note",
    label: "orders.note",
    via: ["table_columns.visible_to"],
    table_id: 3,
  },
  {
    kind: "role_assignment",
    id: 7,
    name: "alice holds seller",
    label: "alice holds seller",
    via: ["user_role_assignments.role_id"],
  },
  { kind: "role", id: "junior", name: "junior", label: "junior", via: ["roles.parents"] },
];

describe("dependentsOf", () => {
  it("reads the dependents a refused delete reported", () => {
    const refused = {
      success: false,
      message: "Role 'seller' is still referred to by: ...",
      code: "schema.role_has_dependents",
      params: { role: "seller", dependents: DEPENDENTS },
    };
    expect(dependentsOf(refused)).toEqual(DEPENDENTS);
  });

  it("is null for a delete that succeeded, failed for another reason, or reported none", () => {
    expect(dependentsOf({ success: true, message: "" })).toBeNull();
    expect(
      dependentsOf({ success: false, message: "not found", code: "schema.role_not_found" }),
    ).toBeNull();
    expect(dependentsOf({ success: false, message: "", params: { dependents: [] } })).toBeNull();
    expect(dependentsOf(undefined)).toBeNull();
  });
});

describe("DependentsDialog", () => {
  it("names the object and lists each dependent under its kind, by its page's label", () => {
    render(<DependentsDialog subject="seller" dependents={DEPENDENTS} onClose={vi.fn()} />);

    expect(screen.getByText("Cannot delete seller yet")).toBeInTheDocument();

    const grants = screen.getByTestId("dependents-column");
    expect(within(grants).getByText("Column grants")).toBeInTheDocument();
    expect(within(grants).getByText("orders.amount")).toBeInTheDocument();
    expect(within(grants).getByText("orders.note")).toBeInTheDocument();

    const holders = screen.getByTestId("dependents-role_assignment");
    expect(within(holders).getByText("alice holds seller")).toBeInTheDocument();

    const heirs = screen.getByTestId("dependents-role");
    expect(within(heirs).getByText("junior")).toBeInTheDocument();
  });

  it("offers nothing to confirm: the only action closes it", () => {
    const onClose = vi.fn();
    render(<DependentsDialog subject="seller" dependents={DEPENDENTS} onClose={onClose} />);
    expect(screen.queryByRole("button", { name: /delete/i })).toBeNull();
    fireEvent.click(screen.getByRole("button", { name: "Close" }));
    expect(onClose).toHaveBeenCalledTimes(1);
  });
});

// REQ-2240: each dependent is named as its own page names it (the server's label, never its id)
// and is itself the link into its detail panel there; there is no per-kind page link.
const dep = (kind: string, id: string | number, label: string, extra: object = {}) => ({
  kind,
  id,
  name: String(id),
  label,
  via: [],
  ...extra,
});

const LINKED: [ReturnType<typeof dep>, string][] = [
  [dep("table", 3, "orders"), detailHref("tables", 3)],
  [dep("table", 9, "open_orders"), detailHref("tables", 9)],
  [dep("column", 41, "orders.amount", { table_id: 3 }), detailHref("tables", 3, "columns")],
  [
    dep("relationship", "users-to-inquiries", "users -> inquiries"),
    detailHref("relationships", "users-to-inquiries"),
  ],
  [dep("role", "junior", "junior"), detailHref("roles", "junior")],
  [dep("row_filter", 12, "seller on sales"), detailHref("rls", 12)],
  [dep("metric", "revenue", "revenue"), detailHref("metrics", "revenue")],
  [dep("command", "notify", "notify"), detailHref("commands", commandId("function", "notify"))],
  [dep("webhook", "notify", "notify"), detailHref("commands", commandId("webhook", "notify"))],
  [dep("data_product", "dp", "Sales product"), detailHref("products", "dp")],
  [dep("source", "pg", "pg"), detailHref("sources", "pg")],
  [
    dep("remote_registration", 5, "remote_orders", { source_id: "pg" }),
    detailHref("sources", "pg"),
  ],
  [dep("domain", "finance", "finance"), detailHref("domains", "finance")],
  [dep("glossary_term", 7, "Order"), detailHref("ontology", 7)],
];

describe("DependentsDialog links each dependent to its detail panel", () => {
  it.each(LINKED.map(([d, href]) => [`${d.kind} ${d.label}`, d, href] as const))(
    "%s",
    (_, dependent, href) => {
      render(<DependentsDialog subject="x" dependents={[dependent]} onClose={vi.fn()} />);
      const group = screen.getByTestId(`dependents-${dependent.kind}`);
      expect(within(group).getByRole("link", { name: dependent.label })).toHaveAttribute(
        "href",
        href,
      );
    },
  );

  it("names a relationship by its two tables, never by its id", () => {
    render(<DependentsDialog subject="x" dependents={[LINKED[3][0]]} onClose={vi.fn()} />);
    expect(screen.getByRole("link", { name: "users -> inquiries" })).toBeInTheDocument();
    expect(screen.queryByText("users-to-inquiries")).toBeNull();
  });

  it("follows a link by closing the dialog", () => {
    const onClose = vi.fn();
    render(<DependentsDialog subject="x" dependents={[LINKED[0][0]]} onClose={onClose} />);
    fireEvent.click(screen.getByRole("link", { name: "orders" }));
    expect(onClose).toHaveBeenCalledTimes(1);
  });

  it("links a function and a webhook of one name to different panels", () => {
    const both = [dep("command", "notify", "notify"), dep("webhook", "notify", "notify")];
    render(<DependentsDialog subject="x" dependents={both} onClose={vi.fn()} />);
    const fn = within(screen.getByTestId("dependents-command")).getByRole("link");
    const hook = within(screen.getByTestId("dependents-webhook")).getByRole("link");
    expect(fn.getAttribute("href")).toBe("/commands?command=notify");
    expect(hook.getAttribute("href")).toBe("/commands?command=webhook%3Anotify");
  });

  // These kinds' pages have no detail panel to open: each is named by its label alone.
  it.each([
    dep("role_assignment", 7, "alice holds seller"),
    dep("materialized_view", "mv-daily", "mv-daily"),
    dep("region", "eu", "eu"),
    dep("synthetic_dataset", "ds1", "ds1"),
    dep("membership", "viv", "viv"),
    dep("invitation", "", "guest@example.com"),
    dep("environment", "feature-x", "feature-x"),
  ])("names a $kind by its label", (dependent) => {
    render(<DependentsDialog subject="x" dependents={[dependent]} onClose={vi.fn()} />);
    expect(screen.getByText(dependent.label)).toBeInTheDocument();
    expect(screen.queryByRole("link")).toBeNull();
  });

  it("has no per-kind page link", () => {
    render(<DependentsDialog subject="x" dependents={LINKED.map(([d]) => d)} onClose={vi.fn()} />);
    expect(screen.queryByText("Open the page")).toBeNull();
    expect(screen.getAllByRole("link")).toHaveLength(LINKED.length);
  });
});
