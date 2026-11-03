// Copyright (c) 2026 Kenneth Stott
// Canary: 8b3e0d27-5f61-4c9a-a2d4-71c9e6f5b038
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2189: a saved ERD layout against the live model, and the bar's Layout menu. A layout holds
// tables by id, so a renamed table keeps its place and its selection; a table it does not hold, in
// a domain it shows, is new since the save. Automatic always exists and cannot be deleted.

import { fireEvent, screen, waitFor } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import { render } from "../test-utils/render";
import type { ListedTable } from "../types/admin";
import type { SavedLayout } from "../api/erdLayouts";
import {
  arrangementOf,
  layoutFocus,
  newSinceSaved,
  sameFocus,
  type ErdPlaces,
} from "../components/erd/erd-saved-layout";
import { ErdLayoutMenu } from "../components/erd/ErdLayoutMenu";

const listed = (id: number, tableName: string, domainId: string) =>
  ({ id, tableName, domainId, columns: [] }) as unknown as ListedTable;

const OPTIONS = {
  columnDetail: "all",
  edgeRouting: "taxi",
  gridSnap: 10,
  showOrphans: true,
} as const;

const SAVED: SavedLayout = {
  name: "Sales overview",
  updatedBy: "ada",
  updatedAt: "2026-11-03T10:00:00Z",
  options: OPTIONS,
  domains: [
    { domainId: "sales", selected: true, collapsed: false, x: 0, y: 0, w: 400, h: 300 },
    { domainId: "finance", selected: false, collapsed: true, x: 600, y: 0, w: 130, h: 90 },
  ],
  tables: [
    { tableId: 1, selected: true, x: 10, y: 20 },
    { tableId: 2, selected: false, x: 200, y: 20 },
    // finance was collapsed when it was saved: its table has a place from before.
    { tableId: 3, selected: false, x: 640, y: 40 },
  ],
};

describe("a saved layout against the live model", () => {
  it("names its selection by the tables' names now: a renamed table stays selected", () => {
    const tables = [listed(1, "clients", "sales"), listed(2, "orders", "sales")];
    expect(layoutFocus(SAVED, tables)).toEqual({ domains: ["sales"], objects: ["sales.clients"] });
  });

  it("does not name a table gone from the model", () => {
    expect(layoutFocus(SAVED, [listed(2, "orders", "sales")])).toEqual({
      domains: ["sales"],
      objects: [],
    });
  });

  it("holds a table it does not know, in a domain shown, as new since the save", () => {
    const tables = [
      listed(1, "customers", "sales"),
      listed(9, "returns", "sales"),
      listed(8, "budgets", "planning"),
    ];
    expect(newSinceSaved(SAVED, tables, new Set(["sales"])).map((t) => t.id)).toEqual([9]);
    expect(newSinceSaved(SAVED, tables, null).map((t) => t.id)).toEqual([9, 8]);
  });

  it("compares focuses as sets", () => {
    expect(
      sameFocus({ domains: ["a", "b"], objects: [] }, { domains: ["b", "a"], objects: [] }),
    ).toBe(true);
    expect(sameFocus({ domains: ["a"], objects: [] }, { domains: ["a", "b"], objects: [] })).toBe(
      false,
    );
  });

  it("saves what is drawn, and keeps the place of a table its collapsed domain hides", () => {
    const places: ErdPlaces = {
      options: OPTIONS,
      collapsed: new Set(["finance"]),
      tables: new Map([
        [1, { x: 15, y: 25 }],
        [2, { x: 210, y: 25 }],
      ]),
      domains: new Map([
        ["sales", { x: 100, y: 10, w: 420, h: 310 }],
        ["finance", { x: 650, y: 5, w: 130, h: 90 }],
      ]),
    };
    const members = [
      listed(1, "customers", "sales"),
      listed(2, "orders", "sales"),
      listed(3, "ledger", "finance"),
    ];
    const saved = arrangementOf(
      places,
      { domains: ["sales"], objects: ["sales.orders"] },
      members,
      SAVED,
    );
    expect(saved.options).toEqual(OPTIONS);
    expect(saved.tables).toEqual([
      { tableId: 1, selected: false, x: 15, y: 25 },
      { tableId: 2, selected: true, x: 210, y: 25 },
      { tableId: 3, selected: false, x: 640, y: 40 },
    ]);
    expect(saved.domains).toEqual([
      { domainId: "finance", selected: false, collapsed: true, x: 650, y: 5, w: 130, h: 90 },
      { domainId: "sales", selected: true, collapsed: false, x: 100, y: 10, w: 420, h: 310 },
    ]);
  });

  it("saves no place for a table never drawn", () => {
    const places: ErdPlaces = {
      options: OPTIONS,
      collapsed: new Set(["finance"]),
      tables: new Map(),
      domains: new Map(),
    };
    const saved = arrangementOf(
      places,
      { domains: [], objects: [] },
      [listed(3, "ledger", "finance")],
      null,
    );
    expect(saved.tables).toEqual([{ tableId: 3, selected: false, x: null, y: null }]);
  });
});

describe("the Layout menu", () => {
  const LAYOUTS = [{ name: "Sales overview", updatedBy: "ada", updatedAt: SAVED.updatedAt }];

  function menu(current: string | null, unsaved: boolean) {
    const props = {
      onChoose: vi.fn(),
      onSave: vi.fn().mockResolvedValue(true),
      onDelete: vi.fn().mockResolvedValue(true),
    };
    render(<ErdLayoutMenu layouts={LAYOUTS} current={current} unsaved={unsaved} {...props} />);
    return props;
  }

  it("names Automatic, and on it offers neither Save nor Delete", async () => {
    const props = menu(null, true);
    expect(screen.getByTestId("erd-layout-menu").textContent).toContain("Automatic");
    expect(screen.getByTestId("erd-layout-unsaved")).toBeTruthy();
    fireEvent.click(screen.getByTestId("erd-layout-menu"));
    await screen.findByTestId("erd-layout-dropdown");
    expect(screen.getByTestId("erd-layout-save")).toHaveProperty("disabled", true);
    expect(screen.getByTestId("erd-layout-delete")).toHaveProperty("disabled", true);
    fireEvent.click(screen.getByTestId("erd-layout-choose-Sales overview"));
    expect(props.onChoose).toHaveBeenCalledWith("Sales overview");
  });

  it("saves an edited layout under its own name", async () => {
    const props = menu("Sales overview", true);
    fireEvent.click(screen.getByTestId("erd-layout-menu"));
    fireEvent.click(await screen.findByTestId("erd-layout-save"));
    expect(props.onSave).toHaveBeenCalledWith("Sales overview");
  });

  it("offers Save only once the layout is edited, and shows no unsaved mark before", async () => {
    menu("Sales overview", false);
    expect(screen.queryByTestId("erd-layout-unsaved")).toBeNull();
    fireEvent.click(screen.getByTestId("erd-layout-menu"));
    expect(await screen.findByTestId("erd-layout-save")).toHaveProperty("disabled", true);
  });

  it("saves as a new name, and refuses a name already taken", async () => {
    const props = menu("Sales overview", true);
    fireEvent.click(screen.getByTestId("erd-layout-menu"));
    fireEvent.click(await screen.findByTestId("erd-layout-save-as"));
    const name = await screen.findByTestId("erd-layout-name");
    fireEvent.change(name, { target: { value: "Sales overview" } });
    expect(screen.getByTestId("erd-layout-save-as-ok")).toHaveProperty("disabled", true);
    fireEvent.change(name, { target: { value: "Sales review" } });
    fireEvent.click(screen.getByTestId("erd-layout-save-as-ok"));
    await waitFor(() => expect(props.onSave).toHaveBeenCalledWith("Sales review"));
  });

  it("deletes the layout shown once it is confirmed", async () => {
    const props = menu("Sales overview", false);
    fireEvent.click(screen.getByTestId("erd-layout-menu"));
    fireEvent.click(await screen.findByTestId("erd-layout-delete"));
    expect(props.onDelete).not.toHaveBeenCalled();
    await screen.findByTestId("erd-layout-delete-confirm");
    fireEvent.click(screen.getByRole("button", { name: /Delete/ }));
    await waitFor(() => expect(props.onDelete).toHaveBeenCalledWith("Sales overview"));
  });
});
