// Copyright (c) 2026 Kenneth Stott
// Canary: 78c1832b-b454-4c10-9d07-d0456d31da8b
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1440/REQ-1441: the workbook carries the type the engine already knew, and a second sheet
// saying how the first was produced. A CSV loses both — every value re-guessed on open, and no
// record of the statement, the page, or the filters that shaped it.

import { describe, it, expect } from "vitest";
import {
  buildXlsxBlob,
  columnType,
  dataSheet,
  provenanceSheet,
  type XlsxLabels,
} from "../exportXlsx";
import { formatTimestamp } from "../formatCell";

const LABELS: XlsxLabels = {
  dataSheet: "Data",
  provenanceSheet: "Provenance",
  fact: "Fact",
  detail: "Detail",
  column: "Column",
  writtenAs: "Written as",
  columnsHeading: "Columns",
  typeNumber: "Number",
  typeDate: "Date",
  typeText: "Text",
};

describe("columnType", () => {
  it("reads a column of numbers as a number", () => {
    expect(columnType([1, 2.5, null])).toBe("number");
  });

  it("reads the engine's instant text as a date", () => {
    expect(columnType(["2026-08-13T02:45:18.310231Z", "2026-08-13 02:45:20"])).toBe("date");
    expect(columnType(["2026-08-13"])).toBe("date");
  });

  it("falls to text when the column does not agree with itself", () => {
    // A mixed column has no single spreadsheet type; writing half of it as a date would silently
    // reinterpret the rest.
    expect(columnType([1, "n/a"])).toBe("text");
    expect(columnType([null, undefined])).toBe("text");
  });
});

describe("dataSheet", () => {
  const rows = [
    { id: 1, at: "2026-08-13T02:45:18.310231Z", day: "2026-08-13", name: "Cat 1", note: null },
    { id: 2, at: "2026-08-13T02:45:20.000000Z", day: "2026-08-13", name: "Cat 2", note: "x" },
  ];
  const cols = ["id", "at", "day", "name", "note"];

  it("writes the header bold and one row per result row", () => {
    const sheet = dataSheet(cols, rows);
    expect(sheet).toHaveLength(3);
    expect(sheet[0].map((c) => c!.value)).toEqual(cols);
    expect(sheet[0][0]).toMatchObject({ fontWeight: "bold" });
  });

  it("writes a number as a number and an instant as a formatted date", () => {
    const [, first] = dataSheet(cols, rows);
    expect(first[0]).toEqual({ value: 1, type: Number });
    expect(first[1]!.type).toBe(Date);
    expect(first[1]!.format).toBe("yyyy-mm-dd hh:mm:ss");
    // The cell holds the same wall clock the grid shows — the zoned wire value rendered in the
    // viewer's own zone. A spreadsheet serial is a day count with no zone, so a UTC-shaped Date is
    // how that clock survives the trip.
    const shown = formatTimestamp(rows[0].at)!;
    expect((first[1]!.value as Date).toISOString()).toBe(`${shown.replace(" ", "T")}.000Z`);
  });

  it("writes a date-only column without a time part", () => {
    const [, first] = dataSheet(cols, rows);
    expect(first[2]!.format).toBe("yyyy-mm-dd");
  });

  it("leaves a null cell empty rather than writing the text 'null'", () => {
    const [, first] = dataSheet(cols, rows);
    expect(first[4]).toBeNull();
  });
});

describe("buildXlsxBlob", () => {
  it("writes both sheets into a real workbook", async () => {
    // The writer rejects a Date cell with no format and an invalid sheet name, so producing the
    // file at all is the check that the two sheets are well-formed.
    const blob = await buildXlsxBlob(
      ["id", "at", "name"],
      [{ id: 1, at: "2026-08-13T02:45:18Z", name: "Cat 1" }],
      [{ label: "Statement executed", value: "SELECT 1" }],
      LABELS,
    );
    expect(blob.size).toBeGreaterThan(0);
  });
});

describe("provenanceSheet", () => {
  it("records the caller's facts and the type every column was written as", () => {
    const sheet = provenanceSheet(
      [{ label: "Statement executed", value: 'SELECT * FROM "ops"."queries" LIMIT 51' }],
      ["id", "at", "name"],
      [{ id: 1, at: "2026-08-13T02:45:18Z", name: "Cat 1" }],
      LABELS,
    );
    const flat = sheet.map((r) => r.map((c) => c?.value ?? ""));
    expect(flat[0]).toEqual(["Fact", "Detail"]);
    expect(flat[1]).toEqual(["Statement executed", 'SELECT * FROM "ops"."queries" LIMIT 51']);
    expect(flat).toContainEqual(["id", "Number"]);
    expect(flat).toContainEqual(["at", "Date"]);
    expect(flat).toContainEqual(["name", "Text"]);
  });
});
