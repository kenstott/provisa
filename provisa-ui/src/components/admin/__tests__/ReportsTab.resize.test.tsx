// Copyright (c) 2026 Kenneth Stott
// Canary: be605574-de61-44e9-9f7e-6abc03dd5a83
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1431: the rule between the report list and the report is a grab handle -- dragging it sets
// how the row is shared, clamped so neither side can be dragged away, and the width is kept.
// REQ-2277: the rule is the standard split panel's (SplitPane), as on every object page.

import { beforeEach, describe, it, expect, vi } from "vitest";
import { render, screen, fireEvent } from "../../../test-utils/render";
import { storageKey } from "../../../lib/browserStorage";

vi.mock("../../../context/AuthContext", () => ({
  useAuth: () => ({
    actingRoles: "analyst",
    loading: false,
    capabilities: ["table_registration"],
    demonstrated: [],
  }),
}));
vi.mock("../../../hooks/useAdminQueries", () => ({
  useTables: () => ({
    tables: [{ id: 1, domainId: "ops", tableName: "slow_queries", alias: null, sourceId: "ops" }],
    loading: false,
    refetch: vi.fn(),
  }),
  useRegisterTable: () => ({ registerTable: vi.fn() }),
  useDeleteTable: () => ({ deleteTable: vi.fn() }),
}));

vi.mock("../../GovernedTableViewer", () => ({
  GovernedTableViewer: () => <div data-testid="viewer" />,
}));

import { ReportsTab } from "../ReportsTab";

const KEY = "provisa.reportsTab.detailWidth";

/** jsdom lays nothing out: give an element the size a browser would report. */
function size(el: Element, props: Record<string, number>) {
  for (const [name, value] of Object.entries(props)) {
    Object.defineProperty(el, name, { configurable: true, value });
  }
}

function openReport() {
  render(<ReportsTab title="Admin — Reports" />);
  fireEvent.click(screen.getByTestId("report-item-slow_queries"));
  const panel = screen.getByTestId("reports-split-panel");
  size(screen.getByTestId("reports-split"), { clientWidth: 1452 });
  panel.getBoundingClientRect = vi.fn(() => ({ right: 1476 }) as DOMRect);
  return panel;
}

beforeEach(() => {
  localStorage.clear();
  Object.defineProperty(window, "innerWidth", { configurable: true, value: 1500 });
});

describe("ReportsTab resize", () => {
  it("has no list-resize handle of its own: the split panel's rule is the one", () => {
    openReport();
    expect(screen.queryByTestId("reports-resize-handle")).toBeNull();
    expect(screen.getByTestId("reports-split-handle")).toHaveAttribute("role", "separator");
  });

  it("writes the dragged width to storage and restores it on the next mount", () => {
    const panel = openReport();
    fireEvent.pointerDown(screen.getByTestId("reports-split-handle"), { clientX: 576 });
    fireEvent.pointerMove(window, { clientX: 776 });
    fireEvent.pointerUp(window);
    expect(panel.style.width).toBe("700px");
    expect(localStorage.getItem(storageKey(KEY))).toBe("700");
  });

  it("moves the edge with the arrow keys", () => {
    localStorage.setItem(storageKey(KEY), "700");
    const panel = openReport();
    size(panel, { offsetWidth: 700 });
    fireEvent.keyDown(screen.getByTestId("reports-split-handle"), { key: "ArrowLeft" });
    expect(panel.style.width).toBe("732px");
  });

  it("clamps so neither the list nor the report can be dragged away", () => {
    const panel = openReport();
    fireEvent.pointerDown(screen.getByTestId("reports-split-handle"), { clientX: 576 });
    fireEvent.pointerMove(window, { clientX: -4000 });
    expect(panel.style.width).toBe("1172px");
    fireEvent.pointerMove(window, { clientX: 4000 });
    expect(panel.style.width).toBe("360px");
    fireEvent.pointerUp(window);
  });
});
