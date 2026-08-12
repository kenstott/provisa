// Copyright (c) 2026 Kenneth Stott
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1386: typing in a column filter. The onChange handler must read the value off the event
// before handing a functional updater to setState: React runs that updater on a later render
// pass, when the synthetic event has been pooled and currentTarget is null — reading it there
// threw "Cannot read properties of null (reading 'value')" and blanked the Reports tab.

import { describe, it, expect } from "vitest";
import { StrictMode } from "react";
import { render, screen, fireEvent } from "../../../test-utils/render";
import { renderHook, act } from "@testing-library/react";
import { ResultsGrid } from "../ResultsGrid";
import { useResultsGrid } from "../useResultsGrid";

const ROWS = [
  { domain: "ops", table: "a" },
  { domain: "sales", table: "c" },
];
const COLS = ["domain", "table"];

function Harness() {
  const grid = useResultsGrid(ROWS, COLS);
  return <ResultsGrid grid={grid} totalRowCount={ROWS.length} />;
}

describe("ResultsGrid column filter", () => {
  it("filters rows as the user types without touching the pooled event", () => {
    render(
      <StrictMode>
        <Harness />
      </StrictMode>,
    );
    const input = screen.getAllByRole("textbox")[0];
    fireEvent.change(input, { target: { value: "sales" } });
    expect(screen.queryByText("ops")).toBeNull();
    expect(screen.getByText("sales")).toBeTruthy();
  });

  it("applies the filter the state updater received", () => {
    const { result } = renderHook(() => useResultsGrid(ROWS, COLS));
    act(() => result.current.setFilters((prev) => ({ ...prev, domain: "sales" })));
    expect(result.current.pagedItems).toHaveLength(1);
  });
});
