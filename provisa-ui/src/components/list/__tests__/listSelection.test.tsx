// Copyright (c) 2026 Kenneth Stott
// Canary: placeholder
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2240: rows of a list selected and deleted together. The shared list table draws a checkbox
// column; shift-click selects a range; the header's box selects every row the current filter
// shows; a plain row click still opens the row. Once anything is selected a bar shows the count
// with Delete and Clear; Delete asks once, naming the rows; each row refused is named with its
// reason, and one still referred to is shown with what refers to it.

import { useMemo, useState } from "react";
import { describe, it, expect, vi } from "vitest";
import { Table, TextInput } from "@mantine/core";
import userEvent from "@testing-library/user-event";
import { render, screen, waitFor, within } from "../../../test-utils/render";
import { ListHead, ListRow, ListTable } from "../ListTable";
import { useListSelection, type BulkDeleteResult } from "../listSelection";
import { ListBulkBar } from "../ListBulkBar";

const ALL = ["orders", "customers", "invoices", "payments", "refunds"];

function Page({
  onDelete = async () => ({ deleted: [], refused: [] }),
  onOpen = () => undefined,
}: {
  onDelete?: (keys: string[]) => Promise<BulkDeleteResult>;
  onOpen?: (key: string) => void;
}) {
  const [filter, setFilter] = useState("");
  const shown = useMemo(() => ALL.filter((n) => n.includes(filter)), [filter]);
  const selection = useListSelection(shown);
  return (
    <>
      <TextInput
        aria-label="filter"
        value={filter}
        onChange={(e) => setFilter(e.currentTarget.value)}
      />
      <ListBulkBar selection={selection} nameOf={(k) => `t_${k}`} onDelete={onDelete} />
      <ListTable testId="items" selection={selection}>
        <ListHead columns={["Name"]} />
        <Table.Tbody>
          {shown.map((name) => (
            <ListRow key={name} itemKey={name} testId={`row-${name}`} onClick={() => onOpen(name)}>
              <Table.Td>{name}</Table.Td>
            </ListRow>
          ))}
        </Table.Tbody>
      </ListTable>
    </>
  );
}

const box = (key: string) => screen.getByTestId(`list-select-${key}`) as HTMLInputElement;
const checked = () =>
  ALL.filter((k) => screen.queryByTestId(`list-select-${k}`)).filter((k) => box(k).checked);

describe("list selection (REQ-2240)", () => {
  it("draws a box on every row; pressing it selects the row without opening it", async () => {
    const onOpen = vi.fn();
    render(<Page onOpen={onOpen} />);
    expect(screen.getByTestId("list-select-all")).toBeInTheDocument();
    await userEvent.click(box("customers"));
    expect(checked()).toEqual(["customers"]);
    expect(onOpen).not.toHaveBeenCalled();
    // A plain click on the row still opens it.
    await userEvent.click(within(screen.getByTestId("row-orders")).getByText("orders"));
    expect(onOpen).toHaveBeenCalledWith("orders");
    expect(checked()).toEqual(["customers"]);
  });

  it("shift-click selects every row from the last one pressed", async () => {
    render(<Page />);
    const user = userEvent.setup();
    await user.click(box("customers"));
    await user.keyboard("{Shift>}");
    await user.click(box("refunds"));
    await user.keyboard("{/Shift}");
    expect(checked()).toEqual(["customers", "invoices", "payments", "refunds"]);
  });

  it("the header's box selects every row the current filter shows, and only those", async () => {
    render(<Page />);
    const user = userEvent.setup();
    await user.type(screen.getByLabelText("filter"), "r");
    // orders, customers, refunds show; invoices and payments are filtered out.
    await user.click(screen.getByTestId("list-select-all"));
    expect(checked()).toEqual(["orders", "customers", "refunds"]);
    expect(screen.getByTestId("list-bulk-count")).toHaveTextContent("3");
    // The filter cleared: the rows it hid were never selected.
    await user.clear(screen.getByLabelText("filter"));
    expect(checked()).toEqual(["orders", "customers", "refunds"]);
    await user.type(screen.getByLabelText("filter"), "o");
    expect(screen.getByTestId("list-bulk-count")).toHaveTextContent("2");
  });

  it("shows the bar only once something is selected; Clear empties the selection", async () => {
    render(<Page />);
    expect(screen.queryByTestId("list-bulk")).toBeNull();
    await userEvent.click(box("orders"));
    await userEvent.click(box("invoices"));
    expect(screen.getByTestId("list-bulk-count")).toHaveTextContent("2");
    await userEvent.click(screen.getByTestId("list-bulk-clear"));
    expect(screen.queryByTestId("list-bulk")).toBeNull();
    expect(checked()).toEqual([]);
  });

  it("Delete asks once, naming the rows, then makes the one bulk call", async () => {
    const onDelete = vi.fn(async (keys: string[]) => ({
      deleted: keys.map((k) => `t_${k}`),
      refused: [],
    }));
    render(<Page onDelete={onDelete} />);
    await userEvent.click(box("orders"));
    await userEvent.click(box("payments"));
    await userEvent.click(screen.getByTestId("list-bulk-delete"));
    const dialog = await screen.findByRole("dialog");
    expect(dialog).toHaveTextContent("t_orders");
    expect(dialog).toHaveTextContent("t_payments");
    expect(dialog).not.toHaveTextContent("t_customers");
    expect(onDelete).not.toHaveBeenCalled();
    await userEvent.click(screen.getByTestId("list-bulk-confirm-dialog-confirm"));
    await waitFor(() => expect(onDelete).toHaveBeenCalledTimes(1));
    expect(onDelete).toHaveBeenCalledWith(["orders", "payments"]);
    await waitFor(() => expect(screen.queryByTestId("list-bulk")).toBeNull());
    expect(screen.queryByTestId("list-bulk-refused")).toBeNull();
  });

  it("names each refused row with its reason, and shows what still refers to one", async () => {
    const onDelete = vi.fn(async () => ({
      deleted: ["t_payments"],
      refused: [
        {
          name: "t_orders",
          reason: "Cannot delete orders: view open_orders reads it",
          dependents: [
            { kind: "table", id: 7, name: "open_orders", via: ["registered_tables.view_sql"] },
          ],
        },
        { name: "t_invoices", reason: "Table 3 not found", dependents: [] },
      ],
    }));
    render(<Page onDelete={onDelete} />);
    await userEvent.click(box("orders"));
    await userEvent.click(box("invoices"));
    await userEvent.click(box("payments"));
    await userEvent.click(screen.getByTestId("list-bulk-delete"));
    await userEvent.click(await screen.findByTestId("list-bulk-confirm-dialog-confirm"));
    const refused = await screen.findByTestId("list-bulk-refused");
    const rows = within(refused).getAllByTestId("list-bulk-refused-row");
    expect(rows.map((r) => r.textContent)).toEqual([
      "t_orders: Cannot delete orders: view open_orders reads it",
      "t_invoices: Table 3 not found",
    ]);
    // The row still referred to: the dependents dialog names it and what refers to it.
    const dependents = await screen.findByTestId("dependents-table");
    expect(dependents).toHaveTextContent("open_orders");
    expect(screen.getByRole("dialog")).toHaveTextContent("t_orders");
  });

  it("marks the selected rows as being deleted while the call is out", async () => {
    let finish: (r: BulkDeleteResult) => void = () => undefined;
    const onDelete = vi.fn(
      () =>
        new Promise<BulkDeleteResult>((resolve) => {
          finish = resolve;
        }),
    );
    render(<Page onDelete={onDelete} />);
    await userEvent.click(box("orders"));
    await userEvent.click(screen.getByTestId("list-bulk-delete"));
    await userEvent.click(await screen.findByTestId("list-bulk-confirm-dialog-confirm"));
    await waitFor(() =>
      expect(screen.getByTestId("row-orders")).toHaveAttribute("data-deleting", "true"),
    );
    expect(screen.getByTestId("row-customers")).not.toHaveAttribute("data-deleting");
    finish({ deleted: ["t_orders"], refused: [] });
    await waitFor(() =>
      expect(screen.getByTestId("row-orders")).not.toHaveAttribute("data-deleting"),
    );
  });
});
