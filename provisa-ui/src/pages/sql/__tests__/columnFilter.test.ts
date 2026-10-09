// Copyright (c) 2026 Kenneth Stott
// Canary: 1a1710e8-8ccf-4b0a-8d35-407447885007
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1937: the typed filter model, its quick syntax, and the type each column is given.

import { describe, it, expect } from "vitest";
import {
  EMPTY_VALUE,
  dateBounds,
  distinctValues,
  TYPE_FAMILIES,
  kindFromType,
  matchesFilter,
  parseQuick,
} from "../columnFilter";

describe("column kind from the engine's type name", () => {
  const families: Record<string, string[]> = {
    number: [
      // DuckDB
      "BIGINT",
      "INTEGER",
      "HUGEINT",
      "UBIGINT",
      "DOUBLE",
      "FLOAT",
      "REAL",
      "DECIMAL(18,3)",
      "TINYINT",
      // PostgreSQL
      "bigint",
      "integer",
      "smallint",
      "numeric",
      "numeric(10,2)",
      "double precision",
      "real",
      "int8",
      "int4",
      "float8",
      "serial",
      // Trino
      "decimal(10,2)",
      "tinyint",
      // the pipeline's normalized names
      "DECIMAL",
    ],
    date: [
      "DATE",
      "TIMESTAMP",
      "TIMESTAMP WITH TIME ZONE",
      "TIMESTAMP_NS",
      "TIMESTAMP_MS",
      "date",
      "timestamp without time zone",
      "timestamp with time zone",
      "timestamp(3) without time zone",
      "timestamptz",
      "timestamp(3)",
      "timestamp(6) with time zone",
      "datetime",
    ],
    boolean: ["BOOLEAN", "boolean", "bool"],
    text: [
      "VARCHAR",
      "UUID",
      "BLOB",
      "INTERVAL",
      "INTEGER[]",
      "STRUCT(a INTEGER)",
      "TIME",
      "character varying",
      "character varying(25)",
      "text",
      "jsonb",
      "bytea",
      "money",
      "interval",
      "integer[]",
      "time without time zone",
      "time with time zone",
      "varchar(25)",
      "char(3)",
      "array(integer)",
      "row(a integer)",
      "json",
      "varbinary",
      "time(3)",
      "interval day to second",
      "SOMETHING_NEW",
    ],
  };

  for (const [kind, names] of Object.entries(families)) {
    it(`maps ${kind} types`, () => {
      for (const name of names) expect([name, kindFromType(name)]).toEqual([name, kind]);
    });
  }

  it("treats a column that reports no type as text", () => {
    expect(kindFromType(null)).toBe("text");
    expect(kindFromType(undefined)).toBe("text");
    expect(kindFromType("")).toBe("text");
  });

  it("has every family reachable from the one table", () => {
    expect(new Set(Object.values(TYPE_FAMILIES))).toEqual(new Set(["number", "date", "boolean"]));
  });
});

describe("quick syntax", () => {
  it("keeps plain text meaning contains", () => {
    expect(parseQuick("abc", "text")).toEqual({ op: "contains", a: "abc" });
    expect(parseQuick(">100", "text")).toEqual({ op: "contains", a: ">100" });
    expect(parseQuick("", "text")).toBeNull();
  });

  it("reads comparisons and ranges on numbers", () => {
    expect(parseQuick(">100", "number")).toEqual({ op: "gt", a: "100" });
    expect(parseQuick("<=5", "number")).toEqual({ op: "le", a: "5" });
    expect(parseQuick("10..20", "number")).toEqual({ op: "between", a: "10", b: "20" });
    expect(parseQuick("!7", "number")).toEqual({ op: "ne", a: "7" });
    expect(parseQuick("=7", "number")).toEqual({ op: "eq", a: "7" });
  });

  it("reads negation, equality and lists on text", () => {
    expect(parseQuick("!foo", "text")).toEqual({ op: "notContains", a: "foo" });
    expect(parseQuick("=foo", "text")).toEqual({ op: "equals", a: "foo" });
    expect(parseQuick("a, b, c", "text")).toEqual({ op: "in", a: "ci", values: ["a", "b", "c"] });
  });

  it("reads dates", () => {
    expect(parseQuick("<2026-01-01", "date")).toEqual({ op: "before", a: "2026-01-01" });
    expect(parseQuick("2026-01-01..2026-02-01", "date")).toEqual({
      op: "between",
      a: "2026-01-01",
      b: "2026-02-01",
    });
  });
});

describe("matchesFilter", () => {
  it("compares numbers by value, not by string", () => {
    const gt = { op: "gt" as const, a: "100" };
    expect(matchesFilter("number", gt, 250)).toBe(true);
    expect(matchesFilter("number", gt, "99")).toBe(false);
    expect(matchesFilter("number", gt, null)).toBe(false);
    expect(matchesFilter("number", { op: "between", a: "20", b: "10" }, 15)).toBe(true);
  });

  it("applies the text operators ignoring case", () => {
    expect(matchesFilter("text", { op: "startsWith", a: "ab" }, "ABC")).toBe(true);
    expect(matchesFilter("text", { op: "endsWith", a: "bc" }, "ABC")).toBe(true);
    expect(matchesFilter("text", { op: "equals", a: "abc" }, "ABC")).toBe(true);
    expect(matchesFilter("text", { op: "notContains", a: "x" }, "ABC")).toBe(true);
    expect(matchesFilter("text", { op: "notContains", a: "b" }, "ABC")).toBe(false);
  });

  it("tests empty and not empty on every kind", () => {
    for (const kind of ["text", "number", "date", "boolean"] as const) {
      expect(matchesFilter(kind, { op: "isEmpty" }, null)).toBe(true);
      expect(matchesFilter(kind, { op: "notEmpty" }, null)).toBe(false);
    }
    expect(matchesFilter("text", { op: "isEmpty" }, "")).toBe(true);
  });

  it("filters to the ticked values, with blank as its own entry", () => {
    const spec = { op: "in" as const, values: ["error", EMPTY_VALUE] };
    expect(matchesFilter("text", spec, "error")).toBe(true);
    expect(matchesFilter("text", spec, null)).toBe(true);
    expect(matchesFilter("text", spec, "ok")).toBe(false);
  });

  it("treats a number as a plain decimal only", () => {
    expect(matchesFilter("number", { op: "gt", a: "1e3" }, 5000)).toBe(true); // not a number: no filter
    expect(matchesFilter("number", { op: "gt", a: "-1.5" }, -1)).toBe(true);
    expect(matchesFilter("number", { op: "gt", a: "-1.5" }, -2)).toBe(false);
  });

  it("narrows nothing while the operand is missing", () => {
    expect(matchesFilter("number", { op: "gt", a: "" }, 1)).toBe(true);
    expect(matchesFilter("text", { op: "in", values: [] }, "x")).toBe(true);
  });
});

describe("dates", () => {
  const now = new Date(2026, 9, 8, 15, 30); // Thursday 8 Oct 2026, local

  it("resolves relative periods to absolute bounds in the local zone", () => {
    expect(dateBounds({ op: "thisMonth" }, now)).toEqual({
      from: new Date(2026, 9, 1).getTime(),
      to: new Date(2026, 10, 1).getTime(),
    });
    expect(dateBounds({ op: "lastMonth" }, now)).toEqual({
      from: new Date(2026, 8, 1).getTime(),
      to: new Date(2026, 9, 1).getTime(),
    });
    expect(dateBounds({ op: "thisWeek" }, now)?.from).toBe(new Date(2026, 9, 5).getTime());
    expect(dateBounds({ op: "lastDays", a: "7" }, now)?.from).toBe(now.getTime() - 7 * 86_400_000);
    expect(dateBounds({ op: "lastDays", a: "0" }, now)).toBeNull();
  });

  it("treats 'between' as inclusive of the last day and 'after' as from the next day", () => {
    expect(
      matchesFilter(
        "date",
        { op: "between", a: "2026-01-01", b: "2026-01-31" },
        "2026-01-31 23:59:00",
      ),
    ).toBe(true);
    expect(
      matchesFilter("date", { op: "between", a: "2026-01-01", b: "2026-01-31" }, "2026-02-01"),
    ).toBe(false);
    expect(matchesFilter("date", { op: "before", a: "2026-01-01" }, "2025-12-31")).toBe(true);
    expect(matchesFilter("date", { op: "after", a: "2026-01-01" }, "2026-01-01 10:00:00")).toBe(
      false,
    );
    expect(matchesFilter("date", { op: "after", a: "2026-01-01" }, "2026-01-02")).toBe(true);
  });
});

describe("distinctValues", () => {
  it("counts each value, most frequent first, with blanks together", () => {
    const rows = [{ s: "ok" }, { s: "error" }, { s: "ok" }, { s: null }, { s: "" }];
    expect(distinctValues(rows, "s")).toEqual([
      { value: "ok", count: 2 },
      { value: EMPTY_VALUE, count: 2 },
      { value: "error", count: 1 },
    ]);
  });
});
