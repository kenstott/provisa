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
// still refers to it; the dialog shows each, grouped by kind and named as an operator knows it,
// with a link to the page where it is removed or changed.

import { describe, it, expect, vi } from "vitest";
import { render, screen, fireEvent, within } from "../../test-utils/render";
import { DependentsDialog } from "../DependentsDialog";
import { dependentsOf } from "../../lib/dependents";

const DEPENDENTS = [
  { kind: "column", id: 41, name: "orders.amount", via: ["table_columns.visible_to"] },
  { kind: "column", id: 42, name: "orders.note", via: ["table_columns.visible_to"] },
  { kind: "role_assignment", id: 7, name: "alice holds seller", via: ["user_role_assignments.role_id"] },
  { kind: "role", id: "junior", name: "junior", via: ["roles.parent_role_id"] },
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
    expect(dependentsOf({ success: false, message: "not found", code: "schema.role_not_found" })).toBeNull();
    expect(dependentsOf({ success: false, message: "", params: { dependents: [] } })).toBeNull();
    expect(dependentsOf(undefined)).toBeNull();
  });
});

describe("DependentsDialog", () => {
  it("names the object and lists each dependent under its kind, with a link to its page", () => {
    render(<DependentsDialog subject="seller" dependents={DEPENDENTS} onClose={vi.fn()} />);

    expect(screen.getByText("Cannot delete seller yet")).toBeInTheDocument();

    const grants = screen.getByTestId("dependents-column");
    expect(within(grants).getByText("Column grants")).toBeInTheDocument();
    expect(within(grants).getByText("orders.amount")).toBeInTheDocument();
    expect(within(grants).getByText("orders.note")).toBeInTheDocument();
    expect(within(grants).getByRole("link", { name: "Open the page" })).toHaveAttribute(
      "href",
      "/tables",
    );

    const holders = screen.getByTestId("dependents-role_assignment");
    expect(within(holders).getByText("alice holds seller")).toBeInTheDocument();
    expect(within(holders).getByRole("link")).toHaveAttribute("href", "/team");

    const heirs = screen.getByTestId("dependents-role");
    expect(within(heirs).getByText("junior")).toBeInTheDocument();
    expect(within(heirs).getByRole("link")).toHaveAttribute("href", "/security/roles");
  });

  it("offers nothing to confirm: the only action closes it", () => {
    const onClose = vi.fn();
    render(<DependentsDialog subject="seller" dependents={DEPENDENTS} onClose={onClose} />);
    expect(screen.queryByRole("button", { name: /delete/i })).toBeNull();
    fireEvent.click(screen.getByRole("button", { name: "Close" }));
    expect(onClose).toHaveBeenCalledTimes(1);
  });
});
