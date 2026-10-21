// Copyright (c) 2026 Kenneth Stott
// Canary: 5435f4a9-86f0-4dd0-ad94-0c34325b25c1
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// A cell of a page list is held to its column's cap and its row's maximum height. jsdom lays
// nothing out, so "does not fit" is given to it here by the sizes an element reports; that the
// rules produce those sizes in a browser is held by the rules themselves (the last block) and
// by the rendered measurements.

import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { Badge, Button, Code, Group, Stack, Table, Text } from "@mantine/core";
import userEvent from "@testing-library/user-event";
import { render, screen, within } from "../../../test-utils/render";
import { ListCell, ListPills } from "../ListCell";
import {
  LIST_COLUMN_CAP_PX,
  LIST_CUT_MIN_CHARS,
  LIST_ROW_MAX_LINES,
  LIST_WRAP_MIN_CHARS,
} from "../listCellLimits";
import { ListHead, ListRow, ListTable } from "../ListTable";

const TOKEN = "warehouse-eu-west-1.internal.example.com/exports/2026/q4/customers";
const WORDS = "Orders placed by customers in the European region since the start of the year";

/** Make every `.list-cell` report that it holds more than it shows, sideways or downwards. */
function cutCells(how: "side" | "foot") {
  const spy = (prop: "scrollWidth" | "scrollHeight") =>
    vi.spyOn(HTMLElement.prototype, prop, "get").mockImplementation(function (this: HTMLElement) {
      return this.classList.contains("list-cell") ? 900 : 0;
    });
  return how === "side" ? spy("scrollWidth") : spy("scrollHeight");
}

afterEach(() => vi.restoreAllMocks());

describe("a cell of text", () => {
  it("(a) fits: shown as it is, no hint, not a tab stop", () => {
    render(<ListCell text="sales-pg" />);
    const cell = screen.getByText("sales-pg");
    expect(cell).toHaveClass("list-cell");
    expect(cell).not.toHaveAttribute("data-cut");
    expect(cell).not.toHaveAttribute("tabindex");
  });

  it("(b) a single value too long for its column: one line, cut, whole in the hint", async () => {
    cutCells("side");
    render(<ListCell text={TOKEN} />);
    const cell = screen.getByText(TOKEN);
    expect(cell).toHaveClass("list-cell--token");
    expect(cell).toHaveAttribute("data-cut", "true");
    await userEvent.hover(cell);
    expect(await screen.findByRole("tooltip")).toHaveTextContent(TOKEN);
  });

  it("a one-line column's text, words and all: one line, cut, whole in the hint", async () => {
    cutCells("side");
    const error = "refresh failed: the source did not answer in time";
    render(<ListCell text={error} oneLine />);
    const cell = screen.getByText(error);
    expect(cell).toHaveClass("list-cell--token", "list-cell--one-line");
    expect(cell).not.toHaveClass("list-cell--words");
    await userEvent.hover(cell);
    expect(await screen.findByRole("tooltip")).toHaveTextContent(error);
  });

  it("a few short words are words: they wrap at their spaces, with no minimum width", () => {
    render(<ListCell text="10/9/2026, 6:00:00 AM" />);
    const cell = screen.getByText("10/9/2026, 6:00:00 AM");
    expect(cell).toHaveClass("list-cell--words");
    expect(cell).not.toHaveClass("list-cell--wide");
  });

  it("words longer than a wrapped column's minimum keep that minimum", () => {
    render(<ListCell text={WORDS} />);
    expect(screen.getByText(WORDS)).toHaveClass("list-cell--words", "list-cell--wide");
  });

  it("a single value no longer than its squeezed minimum keeps its whole width", () => {
    const short = "x".repeat(LIST_CUT_MIN_CHARS);
    render(<ListCell text={short} />);
    expect(screen.getByText(short)).toHaveClass("list-cell--value");
  });

  it("(c) several words: the kind that wraps, and uncut while its lines fit", () => {
    render(<ListCell text={WORDS} />);
    const cell = screen.getByText(WORDS);
    expect(cell).toHaveClass("list-cell--words");
    expect(cell).not.toHaveAttribute("data-cut");
  });

  it("(d) several words past the row's maximum: cut at the foot, whole in the hint", async () => {
    cutCells("foot");
    render(<ListCell text={WORDS} />);
    const cell = screen.getByText(WORDS);
    expect(cell).toHaveClass("list-cell--words");
    expect(cell).toHaveAttribute("data-cut", "true");
    await userEvent.hover(cell);
    expect(await screen.findByRole("tooltip")).toHaveTextContent(WORDS);
  });

  it("a cut cell can be reached by keyboard, and shows the same on focus", async () => {
    cutCells("side");
    // jsdom does not know which focus the keyboard made; the hint opens only for that one.
    const matches = Element.prototype.matches;
    vi.spyOn(Element.prototype, "matches").mockImplementation(function (this: Element, sel) {
      return sel === ":focus-visible" ? true : matches.call(this, sel);
    });
    render(<ListCell text={TOKEN} />);
    await userEvent.tab();
    expect(document.querySelector(".list-cell")).toHaveFocus();
    expect(await screen.findByRole("tooltip")).toHaveTextContent(TOKEN);
  });
});

describe("a cell of pills", () => {
  const VALUES = ["read", "write", "approve", "export", "admin"];

  it("draws them all while they fit", () => {
    render(<ListPills values={VALUES} />);
    for (const value of VALUES) expect(screen.getByText(value)).toBeInTheDocument();
    expect(screen.queryByTestId("list-pills-more")).toBeNull();
  });

  it("counts the ones past the row's maximum in a last pill whose hint lists them all", async () => {
    // Three pills to a line of 20px, in a cell 40px high: the fourth and fifth are below the foot.
    vi.spyOn(HTMLElement.prototype, "clientHeight", "get").mockImplementation(function (
      this: HTMLElement,
    ) {
      return this.classList.contains("list-cell--pills") ? 40 : 0;
    });
    vi.spyOn(HTMLElement.prototype, "offsetHeight", "get").mockReturnValue(20);
    vi.spyOn(HTMLElement.prototype, "offsetTop", "get").mockImplementation(function (
      this: HTMLElement,
    ) {
      const pills = [...(this.parentElement?.querySelectorAll("[data-pill]") ?? [])];
      return Math.floor(pills.indexOf(this) / 2) * 20;
    });
    render(<ListPills values={VALUES} />);
    // Four fit whole (two lines of two); one fewer is drawn so the count has its place.
    const more = await screen.findByTestId("list-pills-more");
    expect(more).toHaveTextContent("+2 more");
    expect(screen.getByText("approve")).toBeInTheDocument();
    expect(screen.queryByText("export")).toBeNull();
    await userEvent.hover(more);
    expect(await screen.findByRole("tooltip")).toHaveTextContent(VALUES.join(", "));
  });
});

describe("a list row", () => {
  const list = (cells: React.ReactNode, columns: Parameters<typeof ListHead>[0]["columns"]) =>
    render(
      <ListTable testId="list">
        <ListHead columns={columns} />
        <Table.Tbody>
          <ListRow testId="row">{cells}</ListRow>
        </Table.Tbody>
      </ListTable>,
    );

  it("holds every cell of text to the cap, whatever page drew it", () => {
    list(
      <>
        <Table.Td>{TOKEN}</Table.Td>
        <Table.Td>
          <Text component="code">{WORDS}</Text>
        </Table.Td>
        <Table.Td>{5000}ms</Table.Td>
      </>,
      ["Host", "Filter", "Timeout"],
    );
    const cells = within(screen.getByTestId("row")).getAllByRole("cell");
    for (const cell of cells) expect(cell).toHaveClass("list-capped");
    expect(cells[0].querySelector(".list-cell--token")).toHaveTextContent(TOKEN);
    // The page's own look for the text is kept inside the capped cell.
    expect(cells[1].querySelector(".list-cell--words code")).toHaveTextContent(WORDS);
    expect(cells[2].querySelector(".list-cell")).toHaveTextContent("5000ms");
  });

  it("never cuts a cell that holds a control", () => {
    list(
      <>
        <Table.Td>orders</Table.Td>
        <Table.Td>
          <Button>Approve</Button>
        </Table.Td>
      </>,
      ["Name", "Actions"],
    );
    const [, actions] = within(screen.getByTestId("row")).getAllByRole("cell");
    expect(actions).not.toHaveClass("list-capped");
    expect(actions.querySelector(".list-cell")).toBeNull();
    expect(actions.querySelector(".list-cell-drawn button")).toHaveTextContent("Approve");
  });

  it("holds a name set in a row with its badges like a cell of text, and keeps the badges beside it", () => {
    list(
      <>
        <Table.Td>
          <Group gap="xs">
            <Text size="sm">{TOKEN}</Text>
            <Badge>drifted</Badge>
          </Group>
        </Table.Td>
      </>,
      ["Name"],
    );
    const [name] = within(screen.getByTestId("row")).getAllByRole("cell");
    expect(name.querySelector(".list-cell-drawn .list-cell--token")).toHaveTextContent(TOKEN);
    // The badge is drawn as the page drew it, not cut.
    expect(name.querySelector(".list-cell--token")).not.toHaveTextContent("drifted");
    expect(name).toHaveTextContent("drifted");
  });

  it("holds a statement set above a dimmed line like a cell of text", () => {
    list(
      <>
        <Table.Td>
          <Stack gap={2}>
            <Code>{WORDS}</Code>
            <Text size="xs" c="dimmed">
              runs as admin
            </Text>
          </Stack>
        </Table.Td>
      </>,
      ["Target"],
    );
    const [target] = within(screen.getByTestId("row")).getAllByRole("cell");
    expect(target.querySelector(".list-cell-drawn .list-cell--words code")).toHaveTextContent(
      WORDS,
    );
    const [, runsAs] = target.querySelectorAll(".list-cell-drawn .list-cell--words");
    expect(runsAs).toHaveTextContent("runs as admin");
  });

  it("gives the list its cap and its row maximum once, and a column its own where it states one", async () => {
    list(
      <>
        <Table.Td>orders</Table.Td>
        <Table.Td>{WORDS}</Table.Td>
      </>,
      ["Name", { label: "Description", cap: 480 }],
    );
    const table = screen.getByTestId("list");
    expect(table.style.getPropertyValue("--list-col-cap")).toBe(`${LIST_COLUMN_CAP_PX}px`);
    expect(table.style.getPropertyValue("--list-row-lines")).toBe(String(LIST_ROW_MAX_LINES));
    expect(table.style.getPropertyValue("--list-wrap-min")).toBe(`${LIST_WRAP_MIN_CHARS}ch`);
    expect(table.style.getPropertyValue("--list-cut-min")).toBe(`${LIST_CUT_MIN_CHARS}ch`);
    const [name, description] = within(screen.getByTestId("row")).getAllByRole("cell");
    expect(name.style.getPropertyValue("--list-cell-cap")).toBe("");
    await vi.waitFor(() =>
      expect(description.style.getPropertyValue("--list-cell-cap")).toBe("480px"),
    );
  });
});

describe("the rules that make it so in a browser", () => {
  const css = readFileSync(resolve(__dirname, "../../../App.css"), "utf8");
  const rule = (selector: string) => {
    const at = css.indexOf(`${selector} {`);
    expect(at, selector).toBeGreaterThan(-1);
    return css.slice(at, css.indexOf("}", at));
  };

  it("a column asks for no more than its cap", () => {
    expect(rule(".data-table td.list-capped")).toContain(
      "max-width: var(--list-cell-cap, var(--list-col-cap))",
    );
  });

  it("a short value stays on one line at its whole width", () => {
    expect(rule(".list-cell--value")).toContain("white-space: nowrap");
  });

  it("a long single value shows one line ending with an ellipsis, and may break to give up width", () => {
    expect(rule(".list-cell--token")).toContain("-webkit-line-clamp: 1");
    expect(rule(".list-cell--token")).toContain("word-break: break-all");
  });

  it("several words wrap up to the row's maximum of lines", () => {
    expect(rule(".list-cell--words")).toContain("-webkit-line-clamp: var(--list-row-lines)");
    // A page that set nowrap on the cell does not stop it.
    expect(rule(".list-cell--words")).toContain("white-space: normal");
  });

  it("a list is as wide as its container, and its columns give way inside it", () => {
    const beside = rule(".split-pane__list > .table-scroll > table");
    expect(beside).toContain("width: 100%");
    expect(beside).not.toContain("max-content");
  });

  it("a pill or a control in a cell the page drew keeps its own width", () => {
    const kept = rule(
      ".list-cell-drawn .mantine-Badge-root,\n.list-cell-drawn .mantine-Button-root,\n.list-cell-drawn .mantine-ActionIcon-root",
    );
    expect(kept).toContain("flex-shrink: 0");
    expect(kept).toContain("min-width: max-content");
  });

  it("wrapped text and a long single value give up width down to their minimums", () => {
    expect(rule(".list-cell--wide")).toContain("min-width: var(--list-wrap-min)");
    expect(rule(".list-cell--token")).toContain("min-width: var(--list-cut-min)");
  });

  it("a one-line column gives up width down to its header", () => {
    expect(rule(".list-cell--token.list-cell--one-line")).toContain("min-width: 0");
  });

  it("a header is shown whole", () => {
    expect(rule(".data-table th")).toContain("white-space: nowrap");
  });
});
