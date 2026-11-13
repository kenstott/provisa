// Copyright (c) 2026 Kenneth Stott
// Canary: b9dd9959-fa15-44f2-bc01-c224a2a0cfca
// Canary: placeholder
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2198: a detail panel's Delete works like its Save. From the confirmation until the delete
// settles the control shows a spinner in place of its icon, the panel's fields and actions are
// disabled, and the item's row is marked as being deleted and is not clickable. A delete that
// went through closes the panel on a list read again without the item; one that failed unlocks
// the panel and says why; one the page refused itself unlocks it and says nothing.

import { useState } from "react";
import { describe, it, expect, vi } from "vitest";
import { Table, TextInput } from "@mantine/core";
import userEvent from "@testing-library/user-event";
import { act, render, screen, waitFor, within } from "../../../test-utils/render";
import { ListRow, ListTable } from "../ListTable";
import { usePanelDeleting } from "../panelActions";
import { DeletingRows, PanelDelete, PanelDeleteLock } from "../PanelDelete";
import { PanelActions, SplitPane } from "../SplitPane";

const LABELS = {
  panel: "Item details",
  resize: "Resize",
  expand: "Expand",
  restore: "Restore",
  close: "Close",
  backToList: "Back",
};

/** A delete that settles when the test says. */
function slowDelete() {
  let settle: { resolve: (v?: boolean) => void; reject: (e: unknown) => void } = {
    resolve: () => {},
    reject: () => {},
  };
  const run = vi.fn(
    () =>
      new Promise<boolean | void>((resolve, reject) => {
        settle = { resolve, reject };
      }),
  );
  return { run, settle: () => settle };
}

function Page({ onDelete }: { onDelete: () => Promise<boolean | void> }) {
  const [items, setItems] = useState(["orders", "customers"]);
  const [open, setOpen] = useState<string | null>(null);
  return (
    <SplitPane
      testId="split"
      storageKey="provisa.test.panelDelete"
      labels={LABELS}
      title={open}
      onClose={() => setOpen(null)}
      list={
        <ListTable testId="items">
          <Table.Tbody>
            {items.map((name) => (
              <ListRow
                key={name}
                itemKey={name}
                testId={`row-${name}`}
                onClick={() => setOpen(name)}
              >
                <Table.Td>{name}</Table.Td>
              </ListRow>
            ))}
          </Table.Tbody>
        </ListTable>
      }
    >
      {open === null ? null : (
        <div>
          <TextInput label="Description" data-testid="description" />
          <PanelActions>
            <button data-testid="edit">Edit</button>
            <PanelDelete
              itemKey={open}
              title={`Delete ${open}?`}
              consequence="It is deleted."
              ariaLabel={`Delete ${open}`}
              testId="delete"
              onDelete={async () => {
                const outcome = await onDelete();
                // The list read again without it, as a page's reload does.
                if (outcome !== false) setItems((prev) => prev.filter((x) => x !== open));
                return outcome;
              }}
            />
          </PanelActions>
        </div>
      )}
    </SplitPane>
  );
}

async function startDelete(user: ReturnType<typeof userEvent.setup>) {
  await user.click(screen.getByTestId("row-orders"));
  await user.click(screen.getByTestId("delete"));
  const dialog = await screen.findByRole("dialog");
  await user.click(within(dialog).getByRole("button", { name: "Delete" }));
  await waitFor(() => expect(screen.queryByRole("dialog")).not.toBeInTheDocument());
}

describe("PanelDelete (REQ-2198)", () => {
  it("confirms with Delete, a filled red button the size of the Cancel beside it", async () => {
    const user = userEvent.setup();
    render(<Page onDelete={vi.fn(async () => undefined)} />);
    await user.click(screen.getByTestId("row-orders"));
    await user.click(screen.getByTestId("delete"));
    const dialog = await screen.findByRole("dialog");
    const confirm = within(dialog).getByRole("button", { name: "Delete" });
    expect(confirm).toHaveAttribute("data-variant", "filled");
    expect(confirm).toHaveAttribute("data-size", "sm");
    expect(within(dialog).getByRole("button", { name: "Cancel" })).toHaveAttribute(
      "data-size",
      "sm",
    );
  });

  it("while the delete runs: spinner, panel locked, row marked and not clickable", async () => {
    const user = userEvent.setup();
    const del = slowDelete();
    render(<Page onDelete={del.run} />);
    await startDelete(user);

    expect(del.run).toHaveBeenCalledTimes(1);
    const control = screen.getByTestId("delete");
    expect(control).toBeDisabled();
    expect(control).toHaveAttribute("data-deleting", "true");
    expect(control.querySelector(".mantine-Loader-root")).not.toBeNull();

    // The panel's fields and its head's actions are disabled.
    expect(screen.getByTestId("description")).toBeDisabled();
    expect(screen.getByTestId("edit")).toBeDisabled();
    const body = screen.getByTestId("split-body");
    expect(body).toHaveAttribute("aria-busy", "true");
    expect(body).toHaveAttribute("inert");

    // The row is marked as being deleted and offers the item for nothing.
    const row = screen.getByTestId("row-orders");
    expect(row).toHaveAttribute("data-deleting", "true");
    expect(row).toHaveAttribute("aria-disabled", "true");
    expect(row).toHaveAttribute("inert");
    expect(row).toHaveClass("row-deleting");
    expect(row).not.toHaveClass("clickable");
    expect(row).toHaveAttribute("title", "Deleting…");
    // Another row is not marked.
    expect(screen.getByTestId("row-customers")).not.toHaveAttribute("data-deleting");

    await act(async () => del.settle().resolve());
  });

  it("a delete that went through closes the panel on a list without the item", async () => {
    const user = userEvent.setup();
    const del = slowDelete();
    render(<Page onDelete={del.run} />);
    await startDelete(user);
    await act(async () => del.settle().resolve());

    await waitFor(() => expect(screen.queryByTestId("split-panel")).not.toBeInTheDocument());
    expect(screen.queryByTestId("row-orders")).not.toBeInTheDocument();
    expect(screen.getByTestId("row-customers")).not.toHaveAttribute("data-deleting");
  });

  it("a failed delete unlocks the panel and names the reason", async () => {
    const user = userEvent.setup();
    const del = slowDelete();
    render(<Page onDelete={del.run} />);
    await startDelete(user);
    await act(async () => del.settle().reject(new Error("view v_orders reads it")));

    const problem = await screen.findByTestId("delete-problem");
    expect(problem).toHaveTextContent("Not deleted: view v_orders reads it");
    expect(problem).toHaveAttribute("role", "alert");
    // Under the panel's head, where a refused save is said.
    expect(problem.parentElement).toHaveClass("split-pane__notice");
    expect(screen.getByTestId("split-panel")).toBeInTheDocument();
    expect(screen.getByTestId("description")).not.toBeDisabled();
    expect(screen.getByTestId("edit")).not.toBeDisabled();
    const control = screen.getByTestId("delete");
    expect(control).not.toBeDisabled();
    expect(control).not.toHaveAttribute("data-deleting");
    expect(screen.getByTestId("split-body")).not.toHaveAttribute("aria-busy");
    const row = screen.getByTestId("row-orders");
    expect(row).not.toHaveAttribute("data-deleting");
    expect(row).toHaveClass("clickable");
  });

  it("a delete the page refused itself unlocks the panel and says nothing", async () => {
    const user = userEvent.setup();
    const del = slowDelete();
    render(<Page onDelete={del.run} />);
    await startDelete(user);
    await act(async () => del.settle().resolve(false));

    await waitFor(() => expect(screen.getByTestId("description")).not.toBeDisabled());
    expect(screen.getByTestId("split-panel")).toBeInTheDocument();
    expect(screen.queryByTestId("delete-problem")).not.toBeInTheDocument();
    expect(screen.getByTestId("row-orders")).not.toHaveAttribute("data-deleting");
  });

  it("closing the panel mid-delete unlocks it; the row stays marked until the delete settles", async () => {
    const user = userEvent.setup();
    const del = slowDelete();
    render(<Page onDelete={del.run} />);
    await startDelete(user);
    await user.click(screen.getByTestId("split-close"));
    expect(screen.queryByTestId("split-panel")).not.toBeInTheDocument();
    expect(screen.getByTestId("row-orders")).toHaveAttribute("data-deleting", "true");

    // Another item opened meanwhile is not locked, and the settled delete does not close it.
    await user.click(screen.getByTestId("row-customers"));
    expect(screen.getByTestId("description")).not.toBeDisabled();
    await act(async () => del.settle().resolve());
    expect(screen.queryByTestId("row-orders")).not.toBeInTheDocument();
    expect(screen.getByTestId("split-panel")).toBeInTheDocument();
  });
});

function ExpandPage({ onDelete }: { onDelete: () => Promise<boolean | void> }) {
  const [open, setOpen] = useState<string | null>("orders");
  const host = usePanelDeleting(open !== null, () => setOpen(null));
  return (
    <DeletingRows host={host}>
      <ListTable testId="items">
        <Table.Tbody>
          <ListRow itemKey="orders" testId="row-orders" onClick={() => setOpen("orders")}>
            <Table.Td>orders</Table.Td>
          </ListRow>
          {open !== null && (
            <Table.Tr>
              <Table.Td>
                <PanelDeleteLock host={host}>
                  <TextInput label="Description" data-testid="description" />
                  <PanelDelete
                    itemKey="orders"
                    title="Delete orders?"
                    consequence="It is deleted."
                    ariaLabel="Delete orders"
                    testId="delete"
                    onDelete={onDelete}
                  />
                </PanelDeleteLock>
              </Table.Td>
            </Table.Tr>
          )}
        </Table.Tbody>
      </ListTable>
    </DeletingRows>
  );
}

describe("PanelDelete beneath its row (REQ-2198)", () => {
  it("locks the detail, marks the row, closes on success", async () => {
    const user = userEvent.setup();
    const del = slowDelete();
    render(<ExpandPage onDelete={del.run} />);
    await user.click(screen.getByTestId("delete"));
    await user.click(
      within(await screen.findByRole("dialog")).getByRole("button", { name: "Delete" }),
    );

    await waitFor(() => expect(screen.getByTestId("description")).toBeDisabled());
    expect(screen.getByTestId("panel-delete-lock")).toHaveAttribute("aria-busy", "true");
    expect(screen.getByTestId("row-orders")).toHaveAttribute("data-deleting", "true");

    await act(async () => del.settle().resolve());
    await waitFor(() => expect(screen.queryByTestId("description")).not.toBeInTheDocument());
    expect(screen.getByTestId("row-orders")).not.toHaveAttribute("data-deleting");
  });

  it("a failed delete names the reason beside the control and unlocks the detail", async () => {
    const user = userEvent.setup();
    const del = slowDelete();
    render(<ExpandPage onDelete={del.run} />);
    await user.click(screen.getByTestId("delete"));
    await user.click(
      within(await screen.findByRole("dialog")).getByRole("button", { name: "Delete" }),
    );
    await act(async () => del.settle().reject("refused by the server"));

    expect(await screen.findByTestId("delete-problem")).toHaveTextContent(
      "Not deleted: refused by the server",
    );
    expect(screen.getByTestId("description")).not.toBeDisabled();
    expect(screen.getByTestId("row-orders")).not.toHaveAttribute("data-deleting");
  });

  it("as a text button among worded actions: its word, held while it cannot delete, a spinner before the word mid-delete", async () => {
    const user = userEvent.setup();
    const del = slowDelete();
    function Toolbar({ blocked }: { blocked: boolean }) {
      return (
        <PanelDelete
          itemKey="orders"
          title="Delete term"
          consequence="Delete term orders?"
          ariaLabel="Delete"
          text="Delete"
          disabled={blocked}
          tip={blocked ? "It has refs." : null}
          testId="text-delete"
          onDelete={del.run}
        />
      );
    }
    const { rerender } = render(<Toolbar blocked />);
    expect(screen.getByRole("button", { name: "Delete" })).toHaveTextContent("Delete");
    expect(screen.getByTestId("text-delete")).toBeDisabled();

    rerender(<Toolbar blocked={false} />);
    const control = screen.getByTestId("text-delete");
    expect(control).toBeEnabled();
    expect(control).toHaveTextContent("Delete");
    await user.click(control);
    const dialog = await screen.findByRole("dialog");
    await user.click(within(dialog).getByRole("button", { name: "Delete" }));
    await waitFor(() => expect(del.run).toHaveBeenCalledTimes(1));
    expect(control).toBeDisabled();
    expect(control).toHaveAttribute("aria-busy", "true");
    expect(control).toHaveTextContent("Delete");
    expect(control.querySelector(".mantine-Loader-root")).not.toBeNull();

    await act(async () => del.settle().reject(new Error("still referenced")));
    expect(await screen.findByTestId("text-delete-problem")).toHaveTextContent(
      "Not deleted: still referenced",
    );
    expect(control).toBeEnabled();
  });
});
