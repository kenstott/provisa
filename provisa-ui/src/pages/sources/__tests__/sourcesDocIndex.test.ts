// Copyright (c) 2026 Kenneth Stott
// Canary: dc9dd7f5-5545-4e98-91cb-f7b94d46ffc6
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.

// docs/sources.md opens with an index of every source type the Sources form offers, numbered, and
// a bold total. It drifted: the form gained types (node, python) the index never listed. This
// holds the index to SOURCE_TYPES -- the same types, numbered 1..N row by row, the total N -- and
// the nine translations to the English numbering and total.

import { describe, it, expect } from "vitest";
import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { SOURCE_TYPES } from "../constants";

const DOCS = resolve(__dirname, "../../../../../docs");
const LOCALES = ["de", "es", "fr", "he", "it", "pt", "ru", "zh", "zh-HK"];

// The index names a few types by the connector they register through, or by their product name.
const DOC_NAME_TO_TYPE: Record<string, string> = {
  graphql_remote: "graphql",
  grpc_remote: "grpc",
  github: "github",
  gitlab: "gitlab",
  stripe: "stripe",
  kaggle: "kaggle",
  wikipedia: "wikipedia",
};

interface IndexRow {
  range: string;
  first: number;
  last: number;
  items: string[];
}

/** The index table's rows: the first table after the "## All Sources" heading's paragraph. */
function indexRows(text: string): IndexRow[] {
  const rows: IndexRow[] = [];
  for (const line of text.split("\n")) {
    const m = /^\| (\d+)(?:–(\d+))? \| [^|]+ \| (.+) \|$/.exec(line);
    if (!m) {
      if (rows.length > 0) break;
      continue;
    }
    const first = Number(m[1]);
    const last = m[2] ? Number(m[2]) : first;
    const items = m[3].split(/, (?=\[?`|[A-Z])/).map((item) => {
      const tick = /`([a-z0-9_]+)`/.exec(item.split(" (")[0]);
      const name = tick ? tick[1] : item.trim().split(/[\s(]/)[0].toLowerCase();
      return DOC_NAME_TO_TYPE[name] ?? name;
    });
    rows.push({ range: m[2] ? `${m[1]}–${m[2]}` : m[1], first, last, items });
  }
  return rows;
}

/** The bold numbers of the paragraph that states the total: the last one before the index table. */
function boldTotals(text: string): number[] {
  const lines = text.split("\n");
  const table = lines.findIndex((l) => /^\| (\d+)(?:–\d+)? \| /.test(l));
  const para = lines
    .slice(0, table)
    .reverse()
    .find((l) => l.trim() !== "" && !l.startsWith("|"));
  return para ? [...para.matchAll(/\*\*(\d+)\*\*/g)].map((m) => Number(m[1])) : [];
}

const english = readFileSync(resolve(DOCS, "sources.md"), "utf8");
const types = SOURCE_TYPES.map((t) => t.value);

describe("docs/sources.md index", () => {
  const rows = indexRows(english);

  it("lists every type the Sources form offers, once each", () => {
    const listed = rows.flatMap((r) => r.items);
    expect(new Set(listed).size).toBe(listed.length);
    expect([...listed].sort()).toEqual([...types].sort());
  });

  it("numbers its rows 1..N with no gap, each row as many as it lists", () => {
    let next = 1;
    for (const row of rows) {
      expect(row.first).toBe(next);
      expect(row.last - row.first + 1).toBe(row.items.length);
      next = row.last + 1;
    }
    expect(next - 1).toBe(types.length);
  });

  it("states the total as the number of source types", () => {
    expect(boldTotals(english)).toEqual([types.length]);
  });

  it.each(LOCALES)("%s numbers its index and states its total as English does", (lng) => {
    const text = readFileSync(resolve(DOCS, lng, "sources.md"), "utf8");
    expect(indexRows(text).map((r) => r.range)).toEqual(rows.map((r) => r.range));
    expect(boldTotals(text)).toEqual([types.length]);
  });
});
