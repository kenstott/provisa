// Copyright (c) 2026 Kenneth Stott
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2260: a list page reopens the item whose detail was open when the viewer comes back to it,
// with the address showing it; closing forgets it; an address naming an item wins; an item no
// longer listed is not reopened and is forgotten; each viewer remembers their own.

import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { Table } from "@mantine/core";
import userEvent from "@testing-library/user-event";
import { MantineProvider } from "@mantine/core";
import { Link, MemoryRouter, Route, Routes, useLocation } from "react-router-dom";
import { act, render, screen } from "@testing-library/react";
import { theme } from "../../../theme/theme";
import { bindViewer, getStored } from "../../../lib/browserStorage";
import { TEST_VIEWER } from "../../../test-setup";
import { ListRow } from "../ListTable";
import { forgetDetailForEveryone, useRememberedDetail } from "../useDetailParam";

const ROWS = ["1", "2", "3"];

function Where() {
  const location = useLocation();
  return <output data-testid="where">{location.pathname + location.search}</output>;
}

function RelationshipsList({ listed }: { listed: readonly string[] | null }) {
  const detail = useRememberedDetail("relationships", listed);
  return (
    <>
      <Table>
        <Table.Tbody>
          {(listed ?? []).map((id) => (
            <ListRow
              key={id}
              itemKey={id}
              testId={`row-${id}`}
              selected={detail.id === id}
              onClick={() => detail.open(id)}
            >
              <Table.Td>{id}</Table.Td>
            </ListRow>
          ))}
        </Table.Tbody>
      </Table>
      {detail.id !== null && (
        <section data-testid="panel">
          {detail.id}
          <button onClick={() => detail.close()}>close</button>
        </section>
      )}
      <Link to="/tables">to-tables</Link>
    </>
  );
}

function App({ start, listed = ROWS }: { start: string; listed?: readonly string[] | null }) {
  return (
    <MantineProvider theme={theme}>
      <MemoryRouter initialEntries={[start]}>
        <Routes>
          <Route path="/relationships" element={<RelationshipsList listed={listed} />} />
          <Route path="/tables" element={<Link to="/relationships">to-relationships</Link>} />
        </Routes>
        <Where />
      </MemoryRouter>
    </MantineProvider>
  );
}

const MEMORY = `detail.${TEST_VIEWER}.relationships`;
const where = () => screen.getByTestId("where").textContent;

let scrolled: Element[];
beforeEach(() => {
  scrolled = [];
  Element.prototype.scrollIntoView = vi.fn(function (this: Element) {
    scrolled.push(this);
  });
});
afterEach(() => {
  forgetDetailForEveryone("relationships");
});

describe("a list page reopens the detail that was open (REQ-2260)", () => {
  it("remembers the item opened", async () => {
    render(<App start="/relationships" />);
    await userEvent.click(screen.getByTestId("row-2"));
    expect(where()).toBe("/relationships?relationship=2");
    expect(getStored(MEMORY)).toBe("2");
  });

  it("reopens it on coming back without a target, in the address, scrolled to its row", async () => {
    render(<App start="/relationships" />);
    await userEvent.click(screen.getByTestId("row-3"));
    await userEvent.click(screen.getByText("to-tables"));
    expect(where()).toBe("/tables");
    scrolled = [];
    await userEvent.click(screen.getByText("to-relationships"));
    expect(where()).toBe("/relationships?relationship=3");
    expect(screen.getByTestId("panel")).toHaveTextContent("3");
    expect(scrolled).toEqual([screen.getByTestId("row-3")]);
  });

  it("reopens it on a fresh mount (a reload) with no item named", () => {
    const first = render(<App start="/relationships?relationship=1" />);
    expect(getStored(MEMORY)).toBe("1");
    first.unmount();
    render(<App start="/relationships" />);
    expect(where()).toBe("/relationships?relationship=1");
  });

  it("waits for the list before reopening", () => {
    render(<App start="/relationships?relationship=2" />).unmount();
    const { rerender } = render(<App start="/relationships" listed={null} />);
    expect(where()).toBe("/relationships");
    rerender(<App start="/relationships" listed={ROWS} />);
    expect(where()).toBe("/relationships?relationship=2");
  });

  it("forgets it when the detail is closed", async () => {
    render(<App start="/relationships" />);
    await userEvent.click(screen.getByTestId("row-2"));
    await userEvent.click(screen.getByText("close"));
    expect(where()).toBe("/relationships");
    expect(getStored(MEMORY)).toBeNull();
    await userEvent.click(screen.getByText("to-tables"));
    await userEvent.click(screen.getByText("to-relationships"));
    expect(where()).toBe("/relationships");
  });

  it("opens the item an address names, and remembers that one instead", () => {
    render(<App start="/relationships?relationship=1" />).unmount();
    render(<App start="/relationships?relationship=3" />);
    expect(where()).toBe("/relationships?relationship=3");
    expect(getStored(MEMORY)).toBe("3");
  });

  it("does not reopen an item the list no longer holds, and forgets it", () => {
    render(<App start="/relationships?relationship=2" />).unmount();
    render(<App start="/relationships" listed={["1", "3"]} />);
    expect(where()).toBe("/relationships");
    expect(screen.queryByTestId("panel")).toBeNull();
    expect(getStored(MEMORY)).toBeNull();
  });

  it("keeps each viewer's item apart", () => {
    render(<App start="/relationships?relationship=1" />).unmount();
    act(() => bindViewer("someone-else"));
    const other = render(<App start="/relationships" />);
    expect(where()).toBe("/relationships");
    other.unmount();
    render(<App start="/relationships?relationship=3" />).unmount();
    act(() => bindViewer(TEST_VIEWER));
    render(<App start="/relationships" />);
    expect(where()).toBe("/relationships?relationship=1");
    act(() => bindViewer("someone-else"));
    expect(getStored("detail.someone-else.relationships")).toBe("3");
  });
});
