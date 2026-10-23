// Copyright (c) 2026 Kenneth Stott
// Canary: 561c87e8-4176-4cba-8474-39dcd7a18de3
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// A list with a detail panel on its right: the panel is there only while it has content; it
// closes by its button, by Escape and by its caller; its left edge resizes it by pointer and by
// keyboard and the width is kept; it expands to the whole row and back; on a row too narrow for
// both, the list gives way; what the content puts in PanelActions sits in the panel's head.

import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { useState } from "react";
import { createPortal } from "react-dom";
import { describe, it, expect, beforeEach, vi } from "vitest";
import { ActionIcon, Button, Table } from "@mantine/core";
import userEvent from "@testing-library/user-event";
import { act, fireEvent, render, screen, waitFor, within } from "../../../test-utils/render";
import { FormFooter } from "../../form/FormLayout";
import { ListRow } from "../ListTable";
import { usePanelDirty, usePanelLeave } from "../panelLeave";
import { PanelActions, SplitPane } from "../SplitPane";
import { HEAD_ACTION_PX } from "../PanelHeadStyle";
import { storageKey as scopedKey } from "../../../lib/browserStorage";

const KEY = "provisa.test.splitWidth";
const LABELS = {
  panel: "Table details",
  resize: "Resize the detail panel",
  expand: "Expand the panel",
  restore: "Restore the panel",
  close: "Close the detail panel",
  backToList: "Back to the list",
};

function Page({ closeOnEscape }: { closeOnEscape?: boolean }) {
  const [open, setOpen] = useState<string | null>(null);
  return (
    <SplitPane
      testId="split"
      storageKey={KEY}
      labels={LABELS}
      title={open}
      onClose={() => setOpen(null)}
      closeOnEscape={closeOnEscape}
      list={
        <>
          <button onClick={() => setOpen("orders")}>orders</button>
          <button onClick={() => setOpen("customers")}>customers</button>
        </>
      }
    >
      {open === null ? null : (
        <div>
          detail of {open}
          <PanelActions>
            <button>Preview {open}</button>
          </PanelActions>
        </div>
      )}
    </SplitPane>
  );
}

/** jsdom lays nothing out: give an element the size a browser would report. */
function size(el: Element, props: Record<string, number>) {
  for (const [name, value] of Object.entries(props)) {
    Object.defineProperty(el, name, { configurable: true, value });
  }
}

beforeEach(() => {
  localStorage.clear();
  Object.defineProperty(window, "innerWidth", { configurable: true, value: 1500 });
});

describe("SplitPane", () => {
  it("opens on a row, swaps to another, and shows which is open in its head", () => {
    render(<Page />);
    expect(screen.queryByTestId("split-panel")).toBeNull();
    expect(screen.queryByRole("separator")).toBeNull();

    fireEvent.click(screen.getByText("orders"));
    const panel = screen.getByRole("complementary", { name: "Table details" });
    expect(panel).toHaveTextContent("detail of orders");
    expect(screen.getByTestId("split-title")).toHaveTextContent("orders");
    expect(screen.getByRole("separator", { name: "Resize the detail panel" })).toBeInTheDocument();

    fireEvent.click(screen.getByText("customers"));
    expect(screen.getByTestId("split-panel")).toHaveTextContent("detail of customers");
    expect(screen.getAllByRole("complementary")).toHaveLength(1);
  });

  it("puts the content's actions in the pinned head", () => {
    render(<Page />);
    fireEvent.click(screen.getByText("orders"));
    const head = screen.getByTestId("split-title").parentElement as HTMLElement;
    expect(within(head).getByRole("button", { name: "Preview orders" })).toBeInTheDocument();
  });

  it("closes by its button and by Escape", () => {
    render(<Page />);
    fireEvent.click(screen.getByText("orders"));
    fireEvent.click(screen.getByTestId("split-close"));
    expect(screen.queryByTestId("split-panel")).toBeNull();

    fireEvent.click(screen.getByText("orders"));
    fireEvent.keyDown(screen.getByTestId("split-panel"), { key: "Escape" });
    expect(screen.queryByTestId("split-panel")).toBeNull();
  });

  it("leaves Escape to a popover or menu drawn in a portal from inside the panel", () => {
    // A tag picker's dropdown, portaled to the body: its Escape closes it, not the panel.
    function WithPortal() {
      const [open, setOpen] = useState<string | null>("orders");
      return (
        <SplitPane
          testId="split"
          storageKey={KEY}
          labels={LABELS}
          title={open}
          onClose={() => setOpen(null)}
          list={<span>list</span>}
        >
          {open === null ? null : createPortal(<input aria-label="in a dropdown" />, document.body)}
        </SplitPane>
      );
    }
    render(<WithPortal />);
    fireEvent.keyDown(screen.getByLabelText("in a dropdown"), { key: "Escape" });
    expect(screen.getByTestId("split-panel")).toBeInTheDocument();
  });

  it("leaves Escape alone while leaving would drop what is being typed", () => {
    render(<Page closeOnEscape={false} />);
    fireEvent.click(screen.getByText("orders"));
    fireEvent.keyDown(screen.getByTestId("split-panel"), { key: "Escape" });
    expect(screen.getByTestId("split-panel")).toBeInTheDocument();
  });

  it("takes focus when it opens and gives it back when it closes", () => {
    render(<Page />);
    const row = screen.getByText("orders");
    row.focus();
    fireEvent.click(row);
    expect(screen.getByTestId("split-panel")).toHaveFocus();
    fireEvent.click(screen.getByTestId("split-close"));
    expect(row).toHaveFocus();
  });

  it("leaves the focus in a dialog open over the page when it opens beneath it", () => {
    // A linked item's panel opens when its data arrives, which on a first visit is after the
    // welcome dialog is up: the reader's Escape is the dialog's, not the panel's.
    function Linked() {
      const [open, setOpen] = useState<string | null>(null);
      return (
        <>
          <div role="dialog" aria-modal="true">
            <button onClick={() => setOpen("orders")}>data arrives</button>
          </div>
          <SplitPane
            testId="split"
            storageKey={KEY}
            labels={LABELS}
            title={open}
            onClose={() => setOpen(null)}
            list={<span>list</span>}
          >
            {open === null ? null : <div>detail of {open}</div>}
          </SplitPane>
        </>
      );
    }
    render(<Linked />);
    const inDialog = screen.getByText("data arrives");
    inDialog.focus();
    fireEvent.click(inDialog);
    expect(screen.getByTestId("split-panel")).toBeInTheDocument();
    expect(inDialog).toHaveFocus();
  });

  it("opens at about 60% of its row and keeps the width it is dragged to", () => {
    render(<Page />);
    fireEvent.click(screen.getByText("orders"));
    const panel = screen.getByTestId("split-panel");
    expect(panel.style.width).toBe("900px");
    size(screen.getByTestId("split"), { clientWidth: 1452 });
    panel.getBoundingClientRect = vi.fn(() => ({ right: 1476 }) as DOMRect);
    // The panel's right edge stays put; its left edge follows the pointer.
    fireEvent.pointerDown(screen.getByTestId("split-handle"), { clientX: 576 });
    fireEvent.pointerMove(window, { clientX: 776 });
    fireEvent.pointerUp(window);
    expect(panel.style.width).toBe("700px");
    expect(localStorage.getItem(scopedKey(KEY))).toBe("700");
  });

  it("never squeezes the list or the panel below what each needs", () => {
    render(<Page />);
    fireEvent.click(screen.getByText("orders"));
    const panel = screen.getByTestId("split-panel");
    size(screen.getByTestId("split"), { clientWidth: 1452 });
    panel.getBoundingClientRect = vi.fn(() => ({ right: 1476 }) as DOMRect);
    fireEvent.pointerDown(screen.getByTestId("split-handle"), { clientX: 576 });
    fireEvent.pointerMove(window, { clientX: -4000 });
    expect(panel.style.width).toBe("1172px"); // the row less the list's 280
    fireEvent.pointerMove(window, { clientX: 4000 });
    expect(panel.style.width).toBe("360px");
    fireEvent.pointerUp(window);
  });

  it("is resized from the keyboard with the arrow keys", () => {
    localStorage.setItem(scopedKey(KEY), "700");
    render(<Page />);
    fireEvent.click(screen.getByText("orders"));
    size(screen.getByTestId("split"), { clientWidth: 1452 });
    const handle = screen.getByTestId("split-handle");
    const panel = screen.getByTestId("split-panel");
    size(panel, { offsetWidth: 700 });
    fireEvent.keyDown(handle, { key: "ArrowLeft" });
    expect(panel.style.width).toBe("732px");
    size(panel, { offsetWidth: 732 });
    fireEvent.keyDown(handle, { key: "ArrowRight" });
    expect(panel.style.width).toBe("700px");
  });

  it("expands to the whole row, putting the list away, and comes back", () => {
    render(<Page />);
    fireEvent.click(screen.getByText("orders"));
    const list = screen.getByText("customers").parentElement as HTMLElement;
    fireEvent.click(screen.getByRole("button", { name: "Expand the panel" }));
    expect(list).not.toBeVisible();
    expect(screen.queryByRole("separator")).toBeNull();
    expect(screen.getByTestId("split-panel").style.width).toBe("");
    fireEvent.click(screen.getByRole("button", { name: "Restore the panel" }));
    expect(list).toBeVisible();
    expect(screen.getByRole("separator")).toBeInTheDocument();
  });

  it("gives the row to the panel, with a way back, where both do not fit", () => {
    const observed: (() => void)[] = [];
    const original = globalThis.ResizeObserver;
    globalThis.ResizeObserver = class {
      constructor(cb: () => void) {
        observed.push(cb);
      }
      observe() {}
      unobserve() {}
      disconnect() {}
    } as unknown as typeof ResizeObserver;
    try {
      render(<Page />);
      size(screen.getByTestId("split"), { clientWidth: 872 }); // a 1280 window with Polly open
      fireEvent.click(screen.getByText("orders"));
      // The row reports its width once it has been measured.
      act(() => observed.forEach((cb) => cb()));
      const back = screen.getByRole("button", { name: "Back to the list" });
      expect(screen.getByText("customers").parentElement).not.toBeVisible();
      expect(screen.queryByRole("separator")).toBeNull();
      expect(screen.queryByRole("button", { name: "Expand the panel" })).toBeNull();
      fireEvent.click(back);
      expect(screen.queryByTestId("split-panel")).toBeNull();
      expect(screen.getByText("customers").parentElement).toBeVisible();
    } finally {
      globalThis.ResizeObserver = original;
    }
  });
});

// Leaving a form in the panel with changes that are not saved asks first.
describe("SplitPane — leaving a form that has unsaved changes", () => {
  function Form({ name, onSaved }: { name: string; onSaved: () => void }) {
    const [value, setValue] = useState(name);
    // The form's one line: changed since it opened, and what it is editing.
    usePanelDirty(value !== name, name);
    return (
      <>
        <input aria-label="alias" value={value} onChange={(e) => setValue(e.target.value)} />
        <FormFooter panel onSave={onSaved} onCancel={onSaved} />
      </>
    );
  }

  function Tables({ withOpener = false }: { withOpener?: boolean }) {
    const [open, setOpen] = useState<string | null>(null);
    const [creating, setCreating] = useState(false);
    const leave = usePanelLeave();
    return (
      <>
        {withOpener && (
          <button onClick={() => leave.guard(() => setCreating(true))}>+ Table</button>
        )}
        <SplitPane
          testId="split"
          storageKey={KEY}
          labels={LABELS}
          title={creating ? "New table" : open}
          onClose={() => setOpen(null)}
          leave={withOpener ? leave : undefined}
          onDiscard={() => undefined}
          list={
            <Table>
              <Table.Tbody>
                {["orders", "customers"].map((name) => (
                  <ListRow key={name} testId={`row-${name}`} onClick={() => setOpen(name)}>
                    <Table.Td>{name}</Table.Td>
                  </ListRow>
                ))}
              </Table.Tbody>
            </Table>
          }
        >
          {creating ? (
            <p>creating</p>
          ) : open === null ? null : (
            <Form key={open} name={open} onSaved={() => setOpen(null)} />
          )}
        </SplitPane>
      </>
    );
  }

  const row = (name: string) => screen.getByTestId(`row-${name}`);
  const title = () => screen.getByTestId("split-title");
  const edit = async () => {
    await userEvent.click(row("orders"));
    await userEvent.type(screen.getByLabelText("alias"), "_v2");
  };
  const asked = () => screen.queryByRole("dialog");
  // The dialog's own root in the page: a pane that asks nothing must hold none (a page may
  // rightly assert that what it shows is not a dialog).
  const modalRoot = () => document.querySelector(".mantine-Modal-root");

  it("holds no dialog in the page until a leave is attempted, and none after the answer", async () => {
    render(<Tables />);
    // Nothing open, then a clean form, then a changed one: no leave attempted, no dialog.
    expect(modalRoot()).toBeNull();
    await userEvent.click(row("orders"));
    expect(modalRoot()).toBeNull();
    await userEvent.type(screen.getByLabelText("alias"), "_v2");
    expect(modalRoot()).toBeNull();
    // A leave attempt on the changed form: the dialog is there.
    await userEvent.click(row("customers"));
    await screen.findByRole("dialog");
    expect(modalRoot()).not.toBeNull();
    // Cancel: gone again. The pane unmounts the dialog with the answer, so there is no exit
    // transition to wait out; waitFor only covers the state update.
    await userEvent.click(within(screen.getByRole("dialog")).getByText("Cancel"));
    await waitFor(() => expect(modalRoot()).toBeNull());
    // And after Discard.
    await userEvent.click(row("customers"));
    await userEvent.click(await screen.findByTestId("discard-changes-confirm"));
    await waitFor(() => expect(modalRoot()).toBeNull());
    expect(title()).toHaveTextContent("customers");
  });

  it("asks when another row is chosen, naming what is being edited", async () => {
    render(<Tables />);
    await edit();
    await userEvent.click(row("customers"));
    const dialog = await screen.findByRole("dialog");
    expect(dialog).toHaveTextContent("Discard your changes?");
    expect(dialog).toHaveTextContent("Your changes to orders are not saved.");
    // Discard first, then the shared Cancel, at the start of the row.
    const buttons = within(dialog)
      .getAllByRole("button")
      .map((b) => b.textContent);
    expect(buttons).toEqual(["Discard", "Cancel"]);
    // Nothing has moved yet.
    expect(title()).toHaveTextContent("orders");
  });

  it("Discard leaves, and the edit is gone", async () => {
    render(<Tables />);
    await edit();
    await userEvent.click(row("customers"));
    await userEvent.click(await screen.findByTestId("discard-changes-confirm"));
    await waitFor(() => expect(title()).toHaveTextContent("customers"));
    expect(screen.getByLabelText("alias")).toHaveValue("customers");
    await waitFor(() => expect(asked()).toBeNull());
    // Back on the first table its form opens as it was saved.
    await userEvent.click(row("orders"));
    expect(screen.getByLabelText("alias")).toHaveValue("orders");
  });

  it("Cancel stays in the form with the edit intact", async () => {
    render(<Tables />);
    await edit();
    await userEvent.click(row("customers"));
    const dialog = await screen.findByRole("dialog");
    await userEvent.click(within(dialog).getByRole("button", { name: "Cancel" }));
    await waitFor(() => expect(asked()).toBeNull());
    expect(title()).toHaveTextContent("orders");
    expect(screen.getByLabelText("alias")).toHaveValue("orders_v2");
  });

  it("asks for the panel's own close, and for the page's opener of another form", async () => {
    render(<Tables withOpener />);
    await edit();
    await userEvent.click(screen.getByTestId("split-close"));
    expect(await screen.findByRole("dialog")).toHaveTextContent("Discard your changes?");
    await userEvent.click(
      within(screen.getByRole("dialog")).getByRole("button", { name: "Cancel" }),
    );
    await waitFor(() => expect(asked()).toBeNull());
    await userEvent.click(screen.getByText("+ Table"));
    await userEvent.click(await screen.findByTestId("discard-changes-confirm"));
    expect(await screen.findByText("creating")).toBeInTheDocument();
  });

  it("does not ask when nothing was changed", async () => {
    render(<Tables />);
    await userEvent.click(row("orders"));
    await userEvent.click(row("customers"));
    expect(title()).toHaveTextContent("customers");
    expect(asked()).toBeNull();
    await userEvent.click(screen.getByTestId("split-close"));
    expect(screen.queryByTestId("split-panel")).toBeNull();
    expect(asked()).toBeNull();
  });

  it("does not ask on Save, nor on the form's own Cancel", async () => {
    render(<Tables />);
    await edit();
    await userEvent.click(screen.getByRole("button", { name: "Save" }));
    expect(asked()).toBeNull();
    expect(screen.queryByTestId("split-panel")).toBeNull();
    // What was dropped by Save does not leave the next form "changed".
    await userEvent.click(row("customers"));
    await userEvent.click(row("orders"));
    expect(asked()).toBeNull();
    await userEvent.type(screen.getByLabelText("alias"), "x");
    await userEvent.click(screen.getByRole("button", { name: "Cancel" }));
    expect(asked()).toBeNull();
  });

  it("a row outside any panel is clicked as before", async () => {
    const onClick = vi.fn();
    render(
      <Table>
        <Table.Tbody>
          <ListRow testId="lone" onClick={onClick}>
            <Table.Td>lone</Table.Td>
          </ListRow>
        </Table.Tbody>
      </Table>,
    );
    await userEvent.click(screen.getByTestId("lone"));
    expect(onClick).toHaveBeenCalledTimes(1);
  });
});

// Every action in the head has one style, defined once; a thin line parts them from the name.
describe("SplitPane — the head's actions", () => {
  function Head({ actions = true }: { actions?: boolean }) {
    return (
      <SplitPane
        testId="split"
        storageKey={KEY}
        labels={LABELS}
        title="orders"
        onClose={() => undefined}
        list={<div>list</div>}
      >
        <>
          {actions && (
            <PanelActions>
              <Button data-testid="preview">Preview</Button>
              <ActionIcon data-testid="edit" aria-label="Edit">
                <span>e</span>
              </ActionIcon>
              <ActionIcon data-testid="delete" aria-label="Delete" color="red">
                <span>d</span>
              </ActionIcon>
            </PanelActions>
          )}
          <p>body</p>
        </>
      </SplitPane>
    );
  }

  it("gives a text action and an icon action the same borderless look and height", async () => {
    render(<Head />);
    const preview = await screen.findByTestId("preview");
    for (const id of ["preview", "edit", "delete", "split-close", "split-expand"]) {
      expect(screen.getByTestId(id)).toHaveAttribute("data-variant", "subtle");
    }
    expect(preview).toHaveAttribute("data-size", "compact-sm");
    // An icon action is as tall as a text one.
    const px = `calc(${HEAD_ACTION_PX / 16}rem * var(--mantine-scale))`;
    expect(screen.getByTestId("edit").style.getPropertyValue("--ai-size")).toBe(px);
    expect(screen.getByTestId("split-close").style.getPropertyValue("--ai-size")).toBe(px);
  });

  it("keeps a destructive action's colour, as text or icon only", async () => {
    render(<Head />);
    const remove = await screen.findByTestId("delete");
    expect(remove).toHaveAttribute("data-variant", "subtle");
    // The readable red, not the pale tint a subtle action has by default.
    expect(remove.style.getPropertyValue("--ai-color")).toBe("var(--mantine-color-red-text)");
    expect(remove.style.getPropertyValue("--ai-bg")).toBe("transparent");
  });

  it("keeps the name a direct child of the head, before the actions", async () => {
    render(<Head />);
    const title = screen.getByTestId("split-title");
    const head = title.parentElement as HTMLElement;
    expect(head).toHaveClass("split-pane__head");
    expect(head).toContainElement(await screen.findByTestId("preview"));
    expect(title.nextElementSibling).toHaveClass("split-pane__actions");
  });

  it("puts the actions a page and its view each give in the one row of the slot", async () => {
    render(
      <SplitPane
        testId="split"
        storageKey={KEY}
        labels={LABELS}
        title="orders"
        onClose={() => undefined}
        list={<div>list</div>}
      >
        <>
          <PanelActions>
            <ActionIcon data-testid="page-action" aria-label="Refresh" />
          </PanelActions>
          <PanelActions>
            <ActionIcon data-testid="view-action" aria-label="Edit" />
          </PanelActions>
        </>
      </SplitPane>,
    );
    const slot = screen.getByTestId("split-title").nextElementSibling as HTMLElement;
    expect(slot).toContainElement(await screen.findByTestId("page-action"));
    expect(slot).toContainElement(screen.getByTestId("view-action"));
    // jsdom lays nothing out: the slot's own rule is what keeps the two on one line.
    const css = readFileSync(resolve(__dirname, "../../../App.css"), "utf8");
    const at = css.indexOf(".split-pane__actions {");
    const rule = css.slice(at, css.indexOf("}", at));
    expect(rule).toContain("display: flex");
    expect(rule).toContain("align-items: center");
  });

  it("has an empty actions slot when the view gives none: nothing for a line to part", () => {
    render(<Head actions={false} />);
    const slot = screen.getByTestId("split-title").nextElementSibling as HTMLElement;
    expect(slot).toHaveClass("split-pane__actions");
    expect(slot).toBeEmptyDOMElement();
  });
});
