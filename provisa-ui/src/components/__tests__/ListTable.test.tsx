// Copyright (c) 2026 Kenneth Stott
// Canary: 0d4a7c19-5e3b-4f68-b1a2-9c7e3f50d8a4
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1940: the shared list style — one table, striped item rows, detail rows that keep the
// alternation.

import { describe, it, expect } from "vitest";
import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { render } from "../../test-utils/render";
import { Table } from "@mantine/core";
import {
  ListTable,
  ListHead,
  ListRow,
  ListExpandRow,
  ListEmpty,
} from "../list/ListTable";

function renderList(expandedIndex: number | null) {
  return render(
    <ListTable testId="demo">
      <ListHead columns={["A"]} />
      <Table.Tbody>
        {[0, 1, 2, 3].map((i) => (
          <>
            <ListRow key={i} testId={`row-${i}`} onClick={() => undefined}>
              <Table.Td>{i}</Table.Td>
            </ListRow>
            {expandedIndex === i && (
              <ListExpandRow key={`d${i}`} colSpan={1} testId="detail">
                <span>detail</span>
              </ListExpandRow>
            )}
          </>
        ))}
      </Table.Tbody>
    </ListTable>,
  );
}

describe("ListTable", () => {
  it("renders the shared data-table inside the shared scroller", () => {
    const { container } = renderList(null);
    const table = container.querySelector("[data-list-table]");
    expect(table).not.toBeNull();
    expect(table!.classList.contains("data-table")).toBe(true);
    expect(table!.parentElement!.classList.contains("table-scroll")).toBe(true);
  });

  it("marks every item row as stripeable and clickable rows as clickable", () => {
    const { container } = renderList(null);
    const rows = container.querySelectorAll("tbody tr.list-row.clickable");
    expect(rows).toHaveLength(4);
  });

  it("detail rows carry a class the stripe rule excludes from the item count", () => {
    // jsdom cannot evaluate `:nth-child(even of .list-row)`, so assert what the rule keys on: item
    // rows are .list-row, the detail row is .list-expand and is not, and it directly follows its
    // parent item row (the `+` sibling the rule uses to give it that row's color).
    const { container } = renderList(1);
    const kids = Array.from(container.querySelectorAll("tbody > tr"));
    const items = kids.filter((r) => r.classList.contains("list-row"));
    expect(items).toHaveLength(4);
    const detail = container.querySelector('[data-testid="detail"]')!;
    expect(detail.classList.contains("list-expand")).toBe(true);
    expect(detail.classList.contains("list-row")).toBe(false);
    expect(detail.previousElementSibling).toBe(items[1]);
    // Even item rows (1-based) are the striped ones: items[1] and items[3].
    const striped = items.filter((_, i) => i % 2 === 1);
    expect(striped.map((r) => r.getAttribute("data-testid"))).toEqual(["row-1", "row-3"]);
  });

  it("stripe rule is zero-specificity so hover and selected states win", () => {
    const css = readFileSync(resolve(process.cwd(), "src/App.css"), "utf8");
    expect(css).toMatch(/\.data-table tbody :where\(tr\.list-row:nth-child\(even of \.list-row\)\)/);
  });

  it("empty state spans every column", () => {
    const { getByText } = render(
      <ListTable>
        <Table.Tbody>
          <ListEmpty colSpan={3}>nothing</ListEmpty>
        </Table.Tbody>
      </ListTable>,
    );
    expect(getByText("nothing").getAttribute("colspan")).toBe("3");
  });
});
