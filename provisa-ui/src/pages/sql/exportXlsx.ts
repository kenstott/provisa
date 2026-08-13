// Copyright (c) 2026 Kenneth Stott
// Canary: 7b3522be-73ea-4bf0-8431-6950cbfdb5a9
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1440/REQ-1441: workbook export for the results grid, shared by every report, the table/view
// preview and the SQL workbench. A CSV hands the reader a wall of text and lets their spreadsheet
// re-guess every value; this writes the type the engine already told us — a number as a number, an
// instant as a date-formatted date — so a column sorts, filters and totals as itself on open.
//
// Every workbook carries a second sheet recording how the sheet next to it was produced: the
// relation, its definition, the exact statement that fetched these rows, the reader's own
// filter/sort/group choices, and the type each column was written as. An exported sheet outlives
// the screen it came from, so the construction travels with it.

import writeXlsxFile from "write-excel-file";
import type { Row, SheetData } from "write-excel-file";
import { parseInstant } from "./formatCell";

/** One recorded fact about how the exported rows were produced. */
export interface ProvenanceEntry {
  label: string;
  value: string;
}

/** Labels for the provenance sheet, supplied by the caller so they stay translated. */
export interface XlsxLabels {
  dataSheet: string;
  provenanceSheet: string;
  fact: string;
  detail: string;
  column: string;
  writtenAs: string;
  columnsHeading: string;
  typeNumber: string;
  typeDate: string;
  typeText: string;
}

export type CellType = "number" | "date" | "text";

const DATE_TIME_FORMAT = "yyyy-mm-dd hh:mm:ss";
const DATE_FORMAT = "yyyy-mm-dd";
const HEADER = { fontWeight: "bold" } as const;
const CHAR_WIDTH_CAP = 60;

/** The type every value in the column agrees on. A column of only nulls is text. */
export function columnType(values: unknown[]): CellType {
  let seen: CellType | null = null;
  for (const v of values) {
    if (v === null || v === undefined) continue;
    let here: CellType;
    if (typeof v === "number") here = "number";
    else if (typeof v === "string" && parseInstant(v) !== null) here = "date";
    else here = "text";
    if (seen === null) seen = here;
    else if (seen !== here) return "text";
  }
  return seen ?? "text";
}

/** The typed data sheet: a bold header row, then one row per result row. */
export function dataSheet(columns: string[], rows: Record<string, unknown>[]): SheetData {
  const types = new Map(columns.map((c) => [c, columnType(rows.map((r) => r[c]))]));
  const header: Row = columns.map((c) => ({ value: c, type: String, ...HEADER }));
  const body: Row[] = rows.map((row) =>
    columns.map((c) => {
      const v = row[c];
      if (v === null || v === undefined) return null;
      switch (types.get(c)) {
        case "number":
          return { value: v as number, type: Number };
        case "date": {
          // columnType only calls a column date when every value in it parses, so this cannot miss.
          const instant = parseInstant(v as string)!;
          return {
            value: instant.at,
            type: Date,
            format: instant.hasTime ? DATE_TIME_FORMAT : DATE_FORMAT,
          };
        }
        default:
          return { value: String(v), type: String };
      }
    }),
  );
  return [header, ...body];
}

/** The provenance sheet: the caller's facts, then the type each column was written as. */
export function provenanceSheet(
  entries: ProvenanceEntry[],
  columns: string[],
  rows: Record<string, unknown>[],
  labels: XlsxLabels,
): SheetData {
  const typeLabel: Record<CellType, string> = {
    number: labels.typeNumber,
    date: labels.typeDate,
    text: labels.typeText,
  };
  const sheet: SheetData = [
    [
      { value: labels.fact, type: String, ...HEADER },
      { value: labels.detail, type: String, ...HEADER },
    ],
  ];
  for (const { label, value } of entries) {
    sheet.push([
      { value: label, type: String },
      { value, type: String, wrap: true },
    ]);
  }
  sheet.push([]);
  sheet.push([{ value: labels.columnsHeading, type: String, ...HEADER }]);
  sheet.push([
    { value: labels.column, type: String, ...HEADER },
    { value: labels.writtenAs, type: String, ...HEADER },
  ]);
  for (const c of columns) {
    sheet.push([
      { value: c, type: String },
      { value: typeLabel[columnType(rows.map((r) => r[c]))], type: String },
    ]);
  }
  return sheet;
}

function widths(sheet: SheetData): { width: number }[] {
  const out: { width: number }[] = [];
  for (const row of sheet) {
    row.forEach((cell, i) => {
      const len = cell?.value === undefined || cell.value === null ? 4 : String(cell.value).length;
      out[i] = { width: Math.min(CHAR_WIDTH_CAP, Math.max(out[i]?.width ?? 8, len + 2)) };
    });
  }
  return out;
}

export async function buildXlsxBlob(
  columns: string[],
  rows: Record<string, unknown>[],
  provenance: ProvenanceEntry[],
  labels: XlsxLabels,
): Promise<Blob> {
  const data = dataSheet(columns, rows);
  const prov = provenanceSheet(provenance, columns, rows, labels);
  return writeXlsxFile([data, prov], {
    sheets: [labels.dataSheet, labels.provenanceSheet],
    columns: [widths(data), widths(prov)],
    stickyRowsCount: 1,
  });
}

export async function downloadXlsx(
  fileName: string,
  columns: string[],
  rows: Record<string, unknown>[],
  provenance: ProvenanceEntry[],
  labels: XlsxLabels,
): Promise<void> {
  const blob = await buildXlsxBlob(columns, rows, provenance, labels);
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = fileName;
  a.click();
  URL.revokeObjectURL(url);
}
