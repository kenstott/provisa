// Copyright (c) 2026 Kenneth Stott
// Canary: bce89d52-572f-4f9d-bea0-cd6749810b96
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2277 rule 9: a detail panel shows an item only while it is a row of the list the page shows.
// A link (or a remembered item) naming one the list leaves out opens no detail, and the page says
// once above the list which item and why, with the action bringing it into view. A search, a filter
// or the domains in view removing the open item close its detail and take it out of the address.

import { afterEach, describe, expect, it } from "vitest";
import { useState } from "react";
import userEvent from "@testing-library/user-event";
import { MantineProvider, Table } from "@mantine/core";
import { MemoryRouter, Route, Routes, useLocation } from "react-router-dom";
import { render, screen } from "@testing-library/react";
import { useTranslation } from "react-i18next";
import "../../../i18n";
import { theme } from "../../../theme/theme";
import { getStored } from "../../../lib/browserStorage";
import { TEST_VIEWER } from "../../../test-setup";
import { ListRow } from "../ListTable";
import { SplitPane } from "../SplitPane";
import { domainNarrowing, filterNarrowing, leftOutOf, searchNarrowing } from "../leftOut";
import { forgetDetailForEveryone, useRememberedDetail } from "../useDetailParam";

interface Item {
  id: string;
  name: string;
  domain: string;
  draft: boolean;
}
const ITEMS: Item[] = [
  { id: "1", name: "orders", domain: "sales", draft: false },
  { id: "2", name: "clinic", domain: "pet-store", draft: false },
  { id: "3", name: "invoices", domain: "sales", draft: true },
];

function Where() {
  const location = useLocation();
  return <output data-testid="where">{location.pathname + location.search}</output>;
}

interface Start {
  loading?: boolean;
  search?: string;
  inView?: string[];
  draftsOnly?: boolean;
}

function Page({ loading = false, ...start }: Start) {
  const { t } = useTranslation();
  const [search, setSearch] = useState(start.search ?? "");
  const [inView, setInView] = useState<ReadonlySet<string> | null>(
    start.inView ? new Set(start.inView) : null,
  );
  const [draftsOnly, setDraftsOnly] = useState(start.draftsOnly ?? false);
  const narrowings = [
    domainNarrowing<Item>(
      t,
      inView,
      (d) => setInView((prev) => new Set([...(prev ?? []), d])),
      (i) => [i.domain],
    ),
    searchNarrowing<Item>(
      t,
      search,
      () => setSearch(""),
      (i) => [i.name],
    ),
    filterNarrowing<Item>(
      t,
      draftsOnly,
      "Drafts",
      (i) => i.draft,
      () => setDraftsOnly(false),
    ),
  ];
  const shownItems = ITEMS.filter((i) => !narrowings.some((n) => n !== null && n.hides(i)));
  const detail = useRememberedDetail("relationships", {
    shown: loading ? null : shownItems.map((i) => i.id),
    leftOut: leftOutOf(
      t,
      loading ? null : ITEMS,
      (i) => i.id,
      (i) => i.name,
      narrowings,
    ),
  });
  const open = ITEMS.find((i) => i.id === detail.id);
  return (
    <>
      <input aria-label="search" value={search} onChange={(e) => setSearch(e.target.value)} />
      <button onClick={() => setInView(new Set(["sales"]))}>sales only</button>
      <button onClick={() => setDraftsOnly(true)}>drafts only</button>
      <SplitPane
        list={
          <Table>
            <Table.Tbody>
              {shownItems.map((i) => (
                <ListRow
                  key={i.id}
                  itemKey={i.id}
                  testId={`row-${i.id}`}
                  selected={detail.id === i.id}
                  onClick={() => detail.open(i.id)}
                >
                  <Table.Td>{i.name}</Table.Td>
                </ListRow>
              ))}
            </Table.Tbody>
          </Table>
        }
        title={open?.name}
        onClose={() => detail.close()}
        storageKey="test.detailInList"
        labels={{
          panel: "panel",
          resize: "resize",
          expand: "expand",
          restore: "restore",
          close: "close",
          backToList: "back",
        }}
        testId="pane"
        outOfList={detail.outOfList}
      >
        {open ? <p data-testid="detail">{open.name}</p> : null}
      </SplitPane>
    </>
  );
}

function App({ start, ...page }: { start: string } & Start) {
  return (
    <MantineProvider theme={theme}>
      <MemoryRouter initialEntries={[start]}>
        <Routes>
          <Route path="/relationships" element={<Page {...page} />} />
        </Routes>
        <Where />
      </MemoryRouter>
    </MantineProvider>
  );
}

const where = () => screen.getByTestId("where").textContent;
const MEMORY = `detail.${TEST_VIEWER}.relationships`;

afterEach(() => {
  forgetDetailForEveryone("relationships");
});

describe("a detail shows only an item the list shows (REQ-2277 rule 9)", () => {
  it("opens no detail for a link to an item out of the domains in view, and says why", async () => {
    render(<App start="/relationships?relationship=2" inView={["sales"]} />);
    expect(screen.queryByTestId("detail")).toBeNull();
    expect(screen.queryByTestId("pane-panel")).toBeNull();
    const notice = screen.getByTestId("detail-out-of-list");
    expect(notice).toHaveTextContent("clinic is not in the list");
    expect(notice).toHaveTextContent("its domain (pet-store) is not in view");
    // The link stays in the address while its item is out of view.
    expect(where()).toBe("/relationships?relationship=2");
    await userEvent.click(screen.getByTestId("detail-out-of-list-show"));
    expect(screen.getByTestId("detail")).toHaveTextContent("clinic");
    expect(screen.getByTestId("row-2")).toBeInTheDocument();
    expect(screen.queryByTestId("detail-out-of-list")).toBeNull();
  });

  it("names the search that leaves a linked item out, and clears it", async () => {
    render(<App start="/relationships?relationship=1" search="clinic" />);
    expect(screen.queryByTestId("detail")).toBeNull();
    expect(screen.getByTestId("detail-out-of-list")).toHaveTextContent(
      "the search “clinic” leaves it out",
    );
    expect(screen.getByTestId("detail-out-of-list-show")).toHaveTextContent("Clear the search");
    await userEvent.click(screen.getByTestId("detail-out-of-list-show"));
    expect(screen.getByLabelText("search")).toHaveValue("");
    expect(screen.getByTestId("detail")).toHaveTextContent("orders");
  });

  it("names every narrowing hiding it, and one action undoes them all", async () => {
    render(<App start="/relationships?relationship=2" inView={["sales"]} draftsOnly />);
    const notice = screen.getByTestId("detail-out-of-list");
    expect(notice).toHaveTextContent("its domain (pet-store) is not in view");
    expect(notice).toHaveTextContent("the Drafts filter leaves it out");
    await userEvent.click(screen.getByTestId("detail-out-of-list-show"));
    expect(screen.getByTestId("detail")).toHaveTextContent("clinic");
  });

  it("dismissing the notice takes the item out of the address", async () => {
    render(<App start="/relationships?relationship=2" inView={["sales"]} />);
    await userEvent.click(screen.getByRole("button", { name: "Dismiss" }));
    expect(where()).toBe("/relationships");
    expect(screen.queryByTestId("detail-out-of-list")).toBeNull();
    expect(getStored(MEMORY)).toBeNull();
  });

  it("does not show a remembered item the list leaves out, and says why", () => {
    render(<App start="/relationships?relationship=2" />).unmount();
    render(<App start="/relationships" inView={["sales"]} />);
    expect(screen.queryByTestId("detail")).toBeNull();
    expect(screen.getByTestId("detail-out-of-list")).toHaveTextContent("clinic");
  });

  it("says nothing of a linked item the page does not hold", () => {
    render(<App start="/relationships?relationship=9" />);
    expect(screen.queryByTestId("detail")).toBeNull();
    expect(screen.queryByTestId("detail-out-of-list")).toBeNull();
  });

  it("closes the detail when a filter removes the open item, clearing it from the address", async () => {
    render(<App start="/relationships" />);
    await userEvent.click(screen.getByTestId("row-1"));
    expect(screen.getByTestId("detail")).toHaveTextContent("orders");
    expect(where()).toBe("/relationships?relationship=1");
    await userEvent.click(screen.getByText("drafts only"));
    expect(screen.queryByTestId("detail")).toBeNull();
    expect(where()).toBe("/relationships");
    expect(screen.queryByTestId("detail-out-of-list")).toBeNull();
    expect(getStored(MEMORY)).toBeNull();
  });

  it("closes the detail when the search removes the open item", async () => {
    render(<App start="/relationships?relationship=1" />);
    expect(screen.getByTestId("detail")).toHaveTextContent("orders");
    await userEvent.type(screen.getByLabelText("search"), "clinic");
    expect(screen.queryByTestId("detail")).toBeNull();
    expect(where()).toBe("/relationships");
  });

  it("closes the detail when the domains in view leave the open item out", async () => {
    render(<App start="/relationships" />);
    await userEvent.click(screen.getByTestId("row-2"));
    expect(screen.getByTestId("detail")).toHaveTextContent("clinic");
    await userEvent.click(screen.getByText("sales only"));
    expect(screen.queryByTestId("detail")).toBeNull();
    expect(where()).toBe("/relationships");
  });

  it("keeps an open item that stays a row when the domains in view change", async () => {
    render(<App start="/relationships?relationship=3" />);
    await userEvent.click(screen.getByText("sales only"));
    expect(screen.getByTestId("detail")).toHaveTextContent("invoices");
  });

  it("keeps the open item's detail while the list is read again", () => {
    const { rerender } = render(<App start="/relationships?relationship=2" />);
    expect(screen.getByTestId("detail")).toHaveTextContent("clinic");
    rerender(<App start="/relationships?relationship=2" loading />);
    expect(screen.getByTestId("detail")).toHaveTextContent("clinic");
    expect(where()).toBe("/relationships?relationship=2");
  });

  it("opens nothing while the list loads", () => {
    render(<App start="/relationships?relationship=2" loading />);
    expect(screen.queryByTestId("detail")).toBeNull();
    expect(screen.queryByTestId("detail-out-of-list")).toBeNull();
  });
});
