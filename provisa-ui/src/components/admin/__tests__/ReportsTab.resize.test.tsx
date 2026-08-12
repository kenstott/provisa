// Copyright (c) 2026 Kenneth Stott
// Canary: be605574-de61-44e9-9f7e-6abc03dd5a83
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1431: the rule between the report list and the report is a grab handle — dragging it sets the
// list width, clamped so neither pane can be dragged away.

import { describe, it, expect, vi } from "vitest";
import { render, screen, fireEvent } from "../../../test-utils/render";

vi.mock("../../../hooks/useAdminQueries", () => ({
  useTables: () => ({ tables: [], loading: false, refetch: vi.fn() }),
  useRegisterTable: () => ({ registerTable: vi.fn() }),
  useDeleteTable: () => ({ deleteTable: vi.fn() }),
}));

vi.mock("../../GovernedTableViewer", () => ({
  GovernedTableViewer: () => <div data-testid="viewer" />,
}));

import { ReportsTab } from "../ReportsTab";

function drag(dx: number) {
  fireEvent.mouseDown(screen.getByTestId("reports-resize-handle"), { clientX: 500 });
  fireEvent.mouseMove(window, { clientX: 500 + dx });
  fireEvent.mouseUp(window);
}

describe("ReportsTab resize handle", () => {
  it("widens the list as the handle is dragged right", () => {
    render(<ReportsTab />);
    expect(screen.getByTestId("reports-list").style.width).toBe("240px");

    drag(80);
    expect(screen.getByTestId("reports-list").style.width).toBe("320px");
  });

  it("clamps the list between its minimum and maximum width", () => {
    render(<ReportsTab />);

    drag(-900);
    expect(screen.getByTestId("reports-list").style.width).toBe("160px");

    drag(900);
    expect(screen.getByTestId("reports-list").style.width).toBe("560px");
  });
});
