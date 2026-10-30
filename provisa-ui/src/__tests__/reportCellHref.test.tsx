// Copyright (c) 2026 Kenneth Stott
// Canary: 6f1a9c3e-4d27-4b85-a0e6-8c2d5b7f1e93
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1386 (ONTOLOGY QUALITY IS A STANDARD REPORT): each row of the Ontology quality report links to
// its term, and a placeholder row to its RELATED_TO review.

import { describe, it, expect, vi } from "vitest";
import { render, screen, within } from "../test-utils/render";
import { ResultsGrid } from "../pages/sql/ResultsGrid";
import { useResultsGrid } from "../pages/sql/useResultsGrid";
import { reportCellHref } from "../lib/reportCellHref";

vi.mock("@mantine/notifications", () => ({ notifications: { show: vi.fn() } }));

describe("reportCellHref", () => {
  it("opens the term a term id names", () => {
    expect(reportCellHref("term_id", 7)).toBe("/admin/ontology?term=7");
    expect(reportCellHref("other_term_id", "12")).toBe("/admin/ontology?term=12");
  });

  it("opens a placeholder's term with the list filtered to RELATED_TO", () => {
    expect(reportCellHref("related_to_review", 7)).toBe("/admin/ontology?term=7&relatedTo=1");
  });

  it("leaves every other cell, and a value that is no id, as text", () => {
    expect(reportCellHref("term_name", "revenue")).toBeNull();
    expect(reportCellHref("edge_id", 3)).toBeNull();
    expect(reportCellHref("term_id", "revenue")).toBeNull();
    expect(reportCellHref("term_id", null)).toBeNull();
  });
});

const COLUMNS = ["term_id", "term_name", "related_to_review"];
const ROWS = [
  { term_id: 7, term_name: "orphan", related_to_review: 7 },
  { term_id: null, term_name: "none", related_to_review: null },
];

function Grid({ linked }: { linked: boolean }) {
  const grid = useResultsGrid(ROWS, COLUMNS);
  return (
    <ResultsGrid
      grid={grid}
      totalRowCount={ROWS.length}
      cellHref={linked ? reportCellHref : undefined}
    />
  );
}

describe("a report grid", () => {
  it("draws a term cell as a link to the term and the review cell as a link to the filter", () => {
    render(<Grid linked />);
    const table = screen.getByRole("table");
    const links = within(table).getAllByRole("link");
    expect(links.map((a) => [a.textContent, a.getAttribute("href")])).toEqual([
      ["7", "/admin/ontology?term=7"],
      ["7", "/admin/ontology?term=7&relatedTo=1"],
    ]);
    // A null stays the null marker, not a link.
    expect(within(table).getByText("orphan").closest("a")).toBeNull();
  });

  it("draws plain text without a cellHref, as every other grid does", () => {
    render(<Grid linked={false} />);
    expect(within(screen.getByRole("table")).queryAllByRole("link")).toEqual([]);
  });
});
