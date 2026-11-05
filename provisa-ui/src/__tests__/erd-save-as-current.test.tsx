// Copyright (c) 2026 Kenneth Stott
// Canary: 5c2e9a47-1b86-4d03-8f7a-d3e61b09c258
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2189: Save as, as in Microsoft Office, makes the layout it wrote the layout shown. The menu
// names it, the link carries it, a later Save writes to it, and the layout it began from is left as
// it was. ErdView is real; the panel (the canvas) and the layout store are stood in for.

import { fireEvent, screen, waitFor } from "@testing-library/react";
import { Route, Routes, useLocation } from "react-router-dom";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { render } from "../test-utils/render";
import type { LayoutArrangement, SavedLayout } from "../api/erdLayouts";

const OPTIONS = { columnDetail: "all", edgeRouting: "taxi", gridSnap: 10, showOrphans: true } as const;

const store = vi.hoisted(() => ({ layouts: new Map<string, unknown>(), puts: [] as string[] }));
const canvas = vi.hoisted(() => ({ x: 10 }));

vi.mock("../api/erdLayouts", () => {
  const stamp = (name: string, a: LayoutArrangement) => ({
    ...a,
    name,
    updatedBy: "ada",
    updatedAt: "2026-11-04T10:00:00Z",
  });
  return {
    fetchErdLayouts: async () => ({
      layouts: [...store.layouts.keys()].sort().map((name) => ({
        name,
        updatedBy: "ada",
        updatedAt: "2026-11-04T10:00:00Z",
      })),
    }),
    fetchErdLayout: async (name: string) => structuredClone(store.layouts.get(name)),
    saveErdLayout: async (name: string, a: LayoutArrangement) => {
      store.puts.push(name);
      const saved = stamp(name, structuredClone(a));
      store.layouts.set(name, saved);
      return structuredClone(saved);
    },
    deleteErdLayout: async (name: string) => {
      store.layouts.delete(name);
      return { deleted: name };
    },
  };
});

const TABLES = {
  tables: [{ id: 1, tableName: "orders", domainId: "sales", columns: [] }],
  loading: false,
  refetch: vi.fn(),
};
vi.mock("../hooks/useAdminQueries", () => ({
  useTables: () => TABLES,
  useAllRelationships: () => ({ relationships: [], loading: false, refetch: vi.fn() }),
  useDomains: () => ({ domains: [{ id: "sales", description: "" }], loading: false, refetch: vi.fn() }),
}));
vi.mock("../context/DomainFilterContext", () => ({
  useDomainFilter: () => ({ checkedDomains: new Set<string>(), domains: ["sales"] }),
}));
vi.mock("../mcp/screenState", () => ({ useScreenInputs: () => undefined }));
vi.mock("../components/list/panelLeave", () => ({
  useLeaveGuard: () => (go: () => void) => go(),
}));

// The canvas: its menu slot, the layout it was handed, and the places it reports when saved.
vi.mock("../components/erd/ErdPanel", () => ({
  ErdPanel: ({
    layoutMenu,
    savedLayout,
    placesRef,
    onArrange,
  }: {
    layoutMenu: React.ReactNode;
    savedLayout: SavedLayout | null;
    placesRef: { current: (() => unknown) | null };
    onArrange: () => void;
  }) => {
    placesRef.current = () => ({
      options: OPTIONS,
      collapsed: new Set<string>(),
      tables: new Map([[1, { x: canvas.x, y: 20 }]]),
      domains: new Map([["sales", { x: 0, y: 0, w: 300, h: 200 }]]),
    });
    return (
      <div>
        {layoutMenu}
        <button data-testid="arrange" onClick={onArrange} />
        <output data-testid="panel-layout">{savedLayout?.name ?? ""}</output>
      </div>
    );
  },
}));

import { ErdView } from "../components/erd/ErdView";

const place = (x: number): SavedLayout => ({
  name: "Sales overview",
  updatedBy: "ada",
  updatedAt: "2026-11-04T10:00:00Z",
  options: OPTIONS,
  domains: [{ domainId: "sales", selected: false, collapsed: false, x: 0, y: 0, w: 300, h: 200 }],
  tables: [{ tableId: 1, selected: false, x, y: 20 }],
});

function Where() {
  const { search } = useLocation();
  return <output data-testid="where">{search}</output>;
}
const layoutParam = () =>
  new URLSearchParams(screen.getByTestId("where").textContent ?? "").get("layout");
const tableX = (name: string) =>
  (store.layouts.get(name) as SavedLayout).tables.find((t) => t.tableId === 1)?.x;

async function saveAs(name: string) {
  fireEvent.click(screen.getByTestId("erd-layout-menu"));
  fireEvent.click(await screen.findByTestId("erd-layout-save-as"));
  fireEvent.change(await screen.findByTestId("erd-layout-name"), { target: { value: name } });
  fireEvent.click(screen.getByTestId("erd-layout-save-as-ok"));
}

beforeEach(() => {
  store.layouts = new Map([["Sales overview", place(10)]]);
  store.puts = [];
  canvas.x = 10;
});

describe("Save as makes the new layout the current one", () => {
  it("shows it, follows it in the link, saves to it next, and leaves the original alone", async () => {
    render(
      <>
        <Routes>
          <Route path="/tables" element={<ErdView page="tables" onOpen={() => undefined} />} />
        </Routes>
        <Where />
      </>,
      { initialEntries: ["/tables?view=erd&layout=Sales%20overview"] },
    );
    await waitFor(() =>
      expect(screen.getByTestId("panel-layout").textContent).toBe("Sales overview"),
    );

    canvas.x = 99; // the arrangement is edited, then saved as a new name
    await saveAs("Sales review");

    await waitFor(() =>
      expect(screen.getByTestId("erd-layout-menu").textContent).toContain("Sales review"),
    );
    expect(layoutParam()).toBe("Sales review");
    await waitFor(() => expect(screen.getByTestId("panel-layout").textContent).toBe("Sales review"));
    expect(tableX("Sales review")).toBe(99);
    expect(tableX("Sales overview")).toBe(10);

    // Edited again, a plain Save goes to the new layout.
    canvas.x = 150;
    fireEvent.click(screen.getByTestId("arrange"));
    fireEvent.click(screen.getByTestId("erd-layout-menu"));
    fireEvent.click(await screen.findByTestId("erd-layout-save"));
    await waitFor(() => expect(store.puts).toEqual(["Sales review", "Sales review"]));
    expect(tableX("Sales review")).toBe(150);
    expect(tableX("Sales overview")).toBe(10);
    expect(layoutParam()).toBe("Sales review");
  });
});
