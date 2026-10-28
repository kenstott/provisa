// Copyright (c) 2026 Kenneth Stott
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// A files source's hints for how the Excel reader finds its workbooks' tables: each shows
// Auto-detect until it is given a number, and it is saved in the source's mapping beside what
// the mapping already holds.

import { useState } from "react";
import { describe, expect, it } from "vitest";
import { fireEvent, renderInForm, screen } from "../../../test-utils/render";
import { ExcelHintsFields } from "../ExcelHintsFields";
import {
  excelHintsFromMapping,
  excelHintsMapping,
  excelHintsOf,
  filesMappingJson,
  NO_EXCEL_HINTS,
  repeatedNames,
  withExcelHints,
} from "../excelHints";

const MAPPING = {
  crawl: { start_urls: ["https://example.test/a"] },
  excel_hints: {
    max_header_rows: 1,
    workbooks: { "watch_list.xlsx": { column_gap: 1 } },
    tables: { report__totals: { range: "summary!H3:K20", header_rows: 2 } },
  },
};

describe("what a files source's Excel hints save", () => {
  it("are read from its mapping and written back as they were", () => {
    const hints = excelHintsFromMapping(JSON.stringify(MAPPING));
    expect(hints.counts).toEqual({ max_header_rows: 1 });
    expect(hints.entries).toEqual([
      { scope: "workbook", name: "watch_list.xlsx", counts: { column_gap: 1 } },
      {
        scope: "table",
        name: "report__totals",
        counts: { header_rows: 2 },
        range: "summary!H3:K20",
      },
    ]);
    expect(excelHintsMapping(hints)).toEqual(MAPPING.excel_hints);
  });

  it("keep the rest of the mapping, and leave no hints when none is set", () => {
    const fields = withExcelHints({}, excelHintsFromMapping(JSON.stringify(MAPPING)));
    expect(JSON.parse(filesMappingJson(JSON.stringify(MAPPING), fields))).toEqual(MAPPING);
    const cleared = withExcelHints({}, NO_EXCEL_HINTS);
    expect(JSON.parse(filesMappingJson(JSON.stringify(MAPPING), cleared))).toEqual({
      crawl: MAPPING.crawl,
    });
    expect(filesMappingJson(null, {})).toBe("{}");
  });

  it("drop an entry with no name or no hint", () => {
    expect(
      excelHintsMapping({
        counts: {},
        entries: [
          { scope: "table", name: "", counts: { header_rows: 1 } },
          { scope: "table", name: "t", counts: {} },
        ],
      }),
    ).toBeNull();
  });

  it("find a workbook or table named twice", () => {
    const twice = repeatedNames({
      counts: {},
      entries: [
        { scope: "table", name: "t", counts: {} },
        { scope: "table", name: " t ", counts: {} },
        { scope: "workbook", name: "t", counts: {} },
      ],
    });
    expect([...twice]).toEqual(["table:t"]);
  });
});

function Harness({ onFields }: { onFields: (f: Record<string, string>) => void }) {
  const [fields, setFields] = useState<Record<string, string>>({});
  return (
    <ExcelHintsFields
      fields={fields}
      setFields={(f) => {
        onFields(f);
        setFields(f);
      }}
    />
  );
}

describe("the Excel hints fields", () => {
  it("show Auto-detect until a hint is given a number", () => {
    let held: Record<string, string> = {};
    renderInForm(<Harness onFields={(f) => (held = f)} />);
    fireEvent.click(screen.getByText("Excel tables"));
    const gap = screen.getByTestId("excel-hint-table_gap_rows");
    expect(gap.getAttribute("placeholder")).toBe("Auto-detect (2)");
    expect(screen.getByTestId("excel-hint-header_rows").getAttribute("placeholder")).toBe(
      "Auto-detect",
    );
    fireEvent.change(gap, { target: { value: "1" } });
    expect(excelHintsOf(held).counts).toEqual({ table_gap_rows: 1 });
    fireEvent.change(gap, { target: { value: "" } });
    expect(excelHintsOf(held).counts).toEqual({});
  });

  it("give one table a range and its own hints", () => {
    let held: Record<string, string> = {};
    renderInForm(<Harness onFields={(f) => (held = f)} />);
    fireEvent.click(screen.getByText("Excel tables"));
    fireEvent.click(screen.getByTestId("excel-hint-add"));
    fireEvent.change(screen.getByTestId("excel-hint-name-0"), {
      target: { value: "report__totals" },
    });
    fireEvent.change(screen.getByTestId("excel-hint-0-range"), {
      target: { value: "summary!H3:K20" },
    });
    fireEvent.change(screen.getByTestId("excel-hint-0-header_rows"), { target: { value: "2" } });
    expect(excelHintsMapping(excelHintsOf(held))).toEqual({
      tables: { report__totals: { header_rows: 2, range: "summary!H3:K20" } },
    });
    expect(screen.queryByTestId("excel-hint-0-column_gap")).toBeNull();
    fireEvent.click(screen.getByTestId("excel-hint-remove-0"));
    expect(excelHintsOf(held).entries).toEqual([]);
  });
});
