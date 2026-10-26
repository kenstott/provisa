// Copyright (c) 2026 Kenneth Stott
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// A canvas table card's "All columns" row: one checkbox above the columns that checks them all,
// unchecks them all, and shows mixed while some are checked.

import { useState } from "react";
import { describe, expect, it } from "vitest";

import { fireEvent, render, screen } from "../../../test-utils/render";
import { CanvasTableCard } from "../CanvasTableCard";

const TABLE = {
  columns: [
    { columnName: "id", dataType: "integer" },
    { columnName: "name", dataType: "varchar" },
    { columnName: "species", dataType: "varchar" },
  ],
};

function Card({ initial = [] as string[] }) {
  const [selected, setSelected] = useState(new Set(initial));
  return (
    <CanvasTableCard
      ct={{ tableName: "pets", x: 0, y: 0 }}
      tbl={TABLE}
      onMove={() => undefined}
      onRemove={() => undefined}
      onStartConnect={() => undefined}
      selection={{
        selected,
        onToggle: (col) =>
          setSelected((prev) => {
            const next = new Set(prev);
            if (next.has(col)) next.delete(col);
            else next.add(col);
            return next;
          }),
        onSetAll: (cols) => setSelected(new Set(cols)),
      }}
    />
  );
}

const all = () => screen.getByTestId("canvas-table-card-all");
const column = (name: string) => screen.getByTestId(`canvas-table-card-col-${name}`);

describe("the All columns row", () => {
  it("checks every column when none is checked", () => {
    render(<Card />);
    expect(all().getAttribute("aria-checked")).toBe("false");
    expect(all().textContent).toContain("All columns (0 of 3)");
    fireEvent.click(all());
    for (const name of ["id", "name", "species"]) {
      expect(column(name).getAttribute("aria-checked")).toBe("true");
    }
    expect(all().getAttribute("aria-checked")).toBe("true");
    expect(all().textContent).toContain("All columns (3 of 3)");
  });

  it("unchecks every column when all are checked", () => {
    render(<Card initial={["id", "name", "species"]} />);
    fireEvent.click(all());
    for (const name of ["id", "name", "species"]) {
      expect(column(name).getAttribute("aria-checked")).toBe("false");
    }
    expect(all().getAttribute("aria-checked")).toBe("false");
  });

  it("is mixed while some are checked, and a click then checks them all", () => {
    render(<Card initial={["name"]} />);
    expect(all().getAttribute("aria-checked")).toBe("mixed");
    expect(all().textContent).toContain("All columns (1 of 3)");
    fireEvent.click(all());
    expect(all().getAttribute("aria-checked")).toBe("true");
  });

  it("is checked from the keyboard too", () => {
    render(<Card />);
    fireEvent.keyDown(all(), { key: " " });
    expect(all().getAttribute("aria-checked")).toBe("true");
  });

  it("is not shown where the canvas selects no columns", () => {
    render(
      <CanvasTableCard
        ct={{ tableName: "pets", x: 0, y: 0 }}
        tbl={TABLE}
        onMove={() => undefined}
        onRemove={() => undefined}
        onStartConnect={() => undefined}
      />,
    );
    expect(screen.queryByTestId("canvas-table-card-all")).toBeNull();
    expect(column("id").getAttribute("role")).toBeNull();
  });
});
