// Copyright (c) 2026 Kenneth Stott
// Canary: a4c81e06-72d5-4b39-8f1a-3e9b5d2c7f64
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2249: a landed view calling a code command is kept incrementally only by its resolver's
// @change function. Its incremental setting is disabled with the reason where the source has
// none, and asks how often the view is rebuilt whole while it is on.

import { describe, expect, it, vi } from "vitest";
import { fireEvent, renderInForm, screen } from "../../../test-utils/render";
import type { IncrementalOption, RegisteredTable } from "../../../types/admin";
import { draftFromRegistration } from "../draftTable";
import { MaterializedViewPanels } from "../MaterializedViewPanels";

const NO_CHANGE: IncrementalOption = {
  available: false,
  code: "schema.incremental_no_change",
  reason: "server english text",
};

function landedView(more: Partial<RegisteredTable>): RegisteredTable {
  return {
    ...draftFromRegistration({
      input: {
        sourceId: "__derived__",
        domainId: "retail",
        schemaName: "views",
        tableName: "store_regions",
        columns: [{ name: "store_id" }, { name: "region_id" }],
      },
      pagination: null,
      pagingKind: null,
      pagingCeilingRows: null,
      rowRules: [],
    }),
    viewSql: "SELECT * FROM match_stores(stores, regions)",
    materialize: true,
    ...more,
  };
}

function panels(table: RegisteredTable, setEditingTable = vi.fn()) {
  renderInForm(
    <MaterializedViewPanels editingTable={table} setEditingTable={setEditingTable} saved={table} />,
  );
  return setEditingTable;
}

describe("a landed view's incremental setting", () => {
  it("can be turned on when the option is available", () => {
    const set = panels(landedView({}));
    const tick = screen.getByTestId("mv-incremental");
    expect(tick).toBeEnabled();
    expect(screen.queryByTestId("mv-rebuild-every")).toBeNull();
    fireEvent.click(tick);
    expect(set).toHaveBeenCalledWith(expect.objectContaining({ mvIncremental: true }));
  });

  it("is disabled, with the catalog's reason at its (?), when the source has no @change", () => {
    panels(landedView({ incrementalOption: NO_CHANGE }));
    const tick = screen.getByTestId("mv-incremental");
    expect(tick).toBeDisabled();
    expect(tick).toHaveAccessibleDescription(
      expect.stringContaining("The code source has no @change function for store_regions."),
    );
    expect(screen.queryByText("server english text", { exact: false })).toBeNull();
  });

  it("asks how often the view is rebuilt whole while it is on", () => {
    const set = panels(landedView({ mvIncremental: true }));
    const every = screen.getByTestId("mv-rebuild-every");
    fireEvent.change(every, { target: { value: "3600" } });
    expect(set).toHaveBeenCalledWith(expect.objectContaining({ mvRebuildEvery: 3600 }));
  });

  it("stays reachable when it is on and the source lost its @change function", () => {
    panels(landedView({ mvIncremental: true, mvRebuildEvery: 3600, incrementalOption: NO_CHANGE }));
    expect(screen.getByTestId("mv-incremental")).toBeEnabled();
    expect(screen.getByTestId("mv-rebuild-every")).toHaveValue("3600");
  });
});
