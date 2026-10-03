// Copyright (c) 2026 Kenneth Stott
// Canary: 86a88bdf-197a-4aa1-8bb7-67d686875d76
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// A role's delete is refused while a grant or an assignment names it (REQ-1918). The dialog
// offers, on each dependent it can deal with in place, the action that removes it: a table's
// column grants (once per table), a metric's, command's or webhook's assigned roles, an
// assignment. A dependent that needs a replacement chosen gets no action.

import { describe, it, expect, vi } from "vitest";
import { render, screen, fireEvent, waitFor } from "../../test-utils/render";
import { DependentsDialog } from "../DependentsDialog";
import { RoleGrantAction } from "../RoleGrantAction";
import type { Dependent } from "../../lib/dependents";

const DEPENDENTS: Dependent[] = [
  { kind: "column", id: 41, name: "orders.amount", via: ["table_columns.visible_to"], table_id: 1 },
  { kind: "column", id: 42, name: "orders.note", via: ["table_columns.writable_by"], table_id: 1 },
  { kind: "column", id: 51, name: "refunds.reason", via: ["table_columns.visible_to"], table_id: 2 },
  { kind: "metric", id: "revenue", name: "revenue", via: ["metrics.visible_to"] },
  { kind: "command", id: "refund", name: "refund", via: ["tracked_functions.visible_to"] },
  {
    kind: "role_assignment",
    id: 7,
    name: "alice holds seller",
    via: ["user_role_assignments.role_id"],
    user_id: "alice",
  },
  { kind: "data_product", id: "sales-core", name: "Sales", via: ["data_products.owner_role"] },
];

const ok = { success: true, message: "" };

function setup(overrides: Partial<Parameters<typeof RoleGrantAction>[0]> = {}) {
  const revokeFromTable = vi.fn().mockResolvedValue(ok);
  const revokeFromObject = vi.fn().mockResolvedValue(ok);
  const removeAssignment = vi.fn().mockResolvedValue(undefined);
  const handled = vi.fn();
  render(
    <DependentsDialog
      subject="seller"
      dependents={DEPENDENTS}
      onClose={vi.fn()}
      itemAction={(d) => (
        <RoleGrantAction
          roleId="seller"
          dependent={d}
          all={DEPENDENTS}
          handled={handled}
          revokeFromTable={revokeFromTable}
          revokeFromObject={revokeFromObject}
          removeAssignment={removeAssignment}
          {...overrides}
        />
      )}
    />,
  );
  return { revokeFromTable, revokeFromObject, removeAssignment, handled };
}

describe("RoleGrantAction", () => {
  it("offers one table action per table, one per object, one per assignment, none otherwise", () => {
    setup();
    expect(screen.getAllByRole("button", { name: "Remove this role from this table's grants" })).toHaveLength(2);
    expect(screen.getAllByRole("button", { name: "Remove this role's grant" })).toHaveLength(2);
    expect(screen.getAllByRole("button", { name: "Remove assignment" })).toHaveLength(1);
  });

  it("takes the role off a table's grants and settles every column grant of that table", async () => {
    const { revokeFromTable, handled } = setup();
    fireEvent.click(
      screen.getAllByRole("button", { name: "Remove this role from this table's grants" })[0],
    );
    await waitFor(() => expect(handled).toHaveBeenCalledTimes(1));
    expect(revokeFromTable).toHaveBeenCalledWith("seller", 1);
    const settles = handled.mock.calls[0][0] as (d: Dependent) => boolean;
    expect(DEPENDENTS.filter(settles).map((d) => d.id)).toEqual([41, 42]);
  });

  it("takes the role off an object's assigned roles by its kind and name", async () => {
    const { revokeFromObject, handled } = setup();
    fireEvent.click(screen.getAllByRole("button", { name: "Remove this role's grant" })[1]);
    await waitFor(() => expect(handled).toHaveBeenCalledTimes(1));
    expect(revokeFromObject).toHaveBeenCalledWith("seller", "COMMAND", "refund");
  });

  it("removes an assignment from its holder", async () => {
    const { removeAssignment } = setup();
    fireEvent.click(screen.getByRole("button", { name: "Remove assignment" }));
    await waitFor(() => expect(removeAssignment).toHaveBeenCalledWith("alice", 7));
  });

  it("shows the server's refusal and settles nothing", async () => {
    const refused = { success: false, message: "No metric named 'revenue'" };
    const { handled } = setup({ revokeFromObject: vi.fn().mockResolvedValue(refused) });
    fireEvent.click(screen.getAllByRole("button", { name: "Remove this role's grant" })[0]);
    expect(await screen.findByText("No metric named 'revenue'")).toBeInTheDocument();
    expect(handled).not.toHaveBeenCalled();
  });
});
