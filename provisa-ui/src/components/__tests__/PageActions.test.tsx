// Copyright (c) 2026 Kenneth Stott
// Canary: 94fe3bfc-aa85-4807-9cb3-cfa861cfdc80
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// A page header's command buttons stand in one row while the header fits them; when it is too
// narrow they move from the end, in order, into a (...) menu (PageActions). jsdom draws nothing, so
// the header's widths are stubbed: each button is 100px wide, the (...) 40px, and the header
// overflows when its row is wider than the header. Each button is a PageCommand: in the row it
// opens a card with its description and a "Read the documentation" link on hover, and in the menu
// the same text and link stand beneath it (maintainer 2026-11-28).

import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { act, fireEvent, screen, waitFor, within } from "@testing-library/react";
import { Button } from "@mantine/core";
import { renderWithProviders } from "../../test-utils/render";
import { PageActions } from "../PageActions";
import { PageCommand } from "../PageCommand";

const header = { width: 0 };
const observers: ResizeObserverCallback[] = [];

class ResizeObserverMock {
  private readonly callback: ResizeObserverCallback;
  constructor(callback: ResizeObserverCallback) {
    this.callback = callback;
    observers.push(callback);
  }
  observe() {}
  unobserve() {}
  disconnect() {
    observers.splice(observers.indexOf(this.callback), 1);
  }
}

/** The width the row's content takes: 100px a button, 40px the (...). */
function rowWidth(head: HTMLElement): number {
  const row = head.querySelector<HTMLElement>(".page-actions");
  if (row === null) return 0;
  return [...row.children].reduce(
    (sum, c) => sum + ((c as HTMLElement).dataset.testid === "page-actions-more" ? 40 : 100),
    0,
  );
}

beforeEach(() => {
  observers.length = 0;
  vi.stubGlobal("ResizeObserver", ResizeObserverMock);
  vi.spyOn(HTMLElement.prototype, "clientWidth", "get").mockImplementation(function (
    this: HTMLElement,
  ) {
    return this.classList.contains("page-header") ? header.width : 0;
  });
  vi.spyOn(HTMLElement.prototype, "scrollWidth", "get").mockImplementation(function (
    this: HTMLElement,
  ) {
    return this.classList.contains("page-header") ? Math.max(header.width, rowWidth(this)) : 0;
  });
});

afterEach(() => {
  vi.restoreAllMocks();
  vi.unstubAllGlobals();
});

function resized(width: number) {
  header.width = width;
  act(() => {
    for (const callback of [...observers]) callback([], {} as ResizeObserver);
  });
}

const onPick = vi.fn();

function renderHeader(width: number) {
  header.width = width;
  renderWithProviders(
    <div className="page-header">
      <PageActions>
        {["A", "B", "C", "D", "E"].map((name) => (
          <PageCommand key={name} description={`Does ${name}.`} docs="configuration/#tables">
            <Button data-testid={`btn-${name}`} onClick={() => onPick(name)}>
              {name}
            </Button>
          </PageCommand>
        ))}
      </PageActions>
    </div>,
  );
}

/** The buttons standing in the row, in order. */
function inRow(): string[] {
  return [...screen.getByTestId("page-actions").children]
    .map((c) => c.querySelector<HTMLElement>("[data-testid^='btn-']")?.dataset.testid)
    .filter((id): id is string => id !== undefined);
}

/** The buttons moved into the (...) menu, in order. */
function inMenu(): string[] {
  const menu = screen.queryByTestId("page-actions-menu");
  if (menu === null) return [];
  return within(menu)
    .queryAllByRole("button", { hidden: true })
    .map((b) => b.dataset.testid ?? "")
    .filter((id) => id.startsWith("btn-"));
}

describe("PageActions", () => {
  it("stands every button in the row when the header is wide enough", () => {
    renderHeader(1000);
    expect(inRow()).toEqual(["btn-A", "btn-B", "btn-C", "btn-D", "btn-E"]);
    expect(screen.queryByTestId("page-actions-more")).toBeNull();
  });

  it("moves the buttons from the end, in order, into a (...) menu when it is narrow", () => {
    renderHeader(260);
    // 2 buttons (200px) and the (...) (40px) fit 260px; a third would not.
    expect(inRow()).toEqual(["btn-A", "btn-B"]);
    expect(inMenu()).toEqual(["btn-C", "btn-D", "btn-E"]);
    expect(screen.getByTestId("page-actions-more")).toHaveAccessibleName("More actions");
  });

  it("takes the buttons back into the row as the header widens, and out as it narrows", () => {
    renderHeader(260);
    resized(1000);
    expect(inRow()).toEqual(["btn-A", "btn-B", "btn-C", "btn-D", "btn-E"]);
    expect(screen.queryByTestId("page-actions-more")).toBeNull();
    resized(370);
    expect(inRow()).toEqual(["btn-A", "btn-B", "btn-C"]);
    expect(inMenu()).toEqual(["btn-D", "btn-E"]);
    resized(30);
    expect(inRow()).toEqual([]);
    expect(inMenu()).toEqual(["btn-A", "btn-B", "btn-C", "btn-D", "btn-E"]);
  });

  it("runs a button picked from the menu and closes the menu", () => {
    onPick.mockClear();
    renderHeader(260);
    fireEvent.click(screen.getByTestId("page-actions-more"));
    expect(screen.getByTestId("page-actions-menu")).toBeVisible();
    fireEvent.click(screen.getByTestId("btn-D"));
    expect(onPick).toHaveBeenCalledWith("D");
    expect(screen.getByTestId("page-actions-menu")).not.toBeVisible();
  });
});

describe("a page command's description and documentation", () => {
  it("opens on hover in the row, with a link the pointer can reach", async () => {
    renderHeader(1000);
    const command = screen.getByTestId("btn-B").closest<HTMLElement>(".page-command")!;
    fireEvent.mouseEnter(command);
    const card = await screen.findByTestId("page-command-card");
    expect(card).toHaveTextContent("Does B.");
    const link = within(card).getByTestId("page-command-docs");
    expect(link).toHaveTextContent("Read the documentation");
    expect(link).toHaveAttribute("href", "/docs?page=configuration/#tables");
    expect(link).toHaveAttribute("target", "_blank");
    expect(link).toHaveAttribute("rel", "noopener noreferrer");
    // The card stays open while the pointer crosses onto it, so the link can be clicked; leaving
    // both closes it. (jsdom positions nothing, so Mantine keeps the card display:none: whether it
    // is mounted is what tells here.)
    fireEvent.mouseLeave(command);
    fireEvent.mouseEnter(card);
    await act(() => new Promise((r) => setTimeout(r, 400)));
    expect(screen.getByTestId("page-command-card")).toBeInTheDocument();
    fireEvent.mouseLeave(screen.getByTestId("page-command-card"));
    await waitFor(() => expect(screen.queryByTestId("page-command-card")).toBeNull());
  });

  it("stands beneath each command moved into the (...) menu", () => {
    renderHeader(260);
    fireEvent.click(screen.getByTestId("page-actions-more"));
    const menu = screen.getByTestId("page-actions-menu");
    const moved = within(menu).getAllByTestId("page-command");
    expect(moved.map((m) => within(m).getByTestId("page-command-description").textContent)).toEqual(
      ["Does C.", "Does D.", "Does E."],
    );
    for (const m of moved)
      expect(within(m).getByTestId("page-command-docs")).toHaveAttribute(
        "href",
        "/docs?page=configuration/#tables",
      );
  });

  it("follows the documentation link from the menu and closes the menu", () => {
    renderHeader(260);
    fireEvent.click(screen.getByTestId("page-actions-more"));
    const menu = screen.getByTestId("page-actions-menu");
    fireEvent.click(within(menu).getAllByTestId("page-command-docs")[0]);
    expect(screen.getByTestId("page-actions-menu")).not.toBeVisible();
  });

  it("refuses a command that is not a PageCommand", () => {
    vi.spyOn(console, "error").mockImplementation(() => {});
    expect(() =>
      renderWithProviders(
        <PageActions>
          <Button>Bare</Button>
        </PageActions>,
      ),
    ).toThrow(/only PageCommand children/);
  });
});

describe("the page header's filter", () => {
  const css = readFileSync(resolve(__dirname, "../../App.css"), "utf8");
  const rem = (name: string, block: RegExp): number => {
    const m = block.exec(css);
    if (m === null) throw new Error(`App.css has no ${name}`);
    return Number(m[1]);
  };

  it("shrinks below the filter's own minimum width before the actions give", () => {
    const filterMin = rem("--page-filter-min", /--page-filter-min:\s*([\d.]+)rem/);
    const searchMin = rem(
      ".search-wrap min-width",
      /\.search-wrap \{[^}]*min-width:\s*([\d.]+)rem/,
    );
    expect(filterMin).toBeLessThan(searchMin);
    expect(css).toMatch(/grid-template-columns:\s*1fr minmax\(var\(--page-filter-min\), 1fr\) 1fr/);
    expect(css).toMatch(/\.page-header \.search-wrap \{[^}]*min-width:\s*0;/);
  });
});
