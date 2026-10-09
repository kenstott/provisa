// Copyright (c) 2026 Kenneth Stott
// Canary: 3f6a1d58-b07c-4e29-8a4d-c5e2f9071b36
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1937: a typed column filter as SQL for the server-paged viewer. The statement holds
// placeholders; every value the reader typed or ticked is in the list bound beside it, and
// nothing of it is in the text.

import { describe, it, expect } from "vitest";
import { binder, filterToSql, numberValue, timestampText } from "../columnFilterSql";
import { EMPTY_VALUE, type ActiveFilter } from "../../pages/sql/columnFilter";

const f = (col: string, kind: ActiveFilter["kind"], spec: ActiveFilter["spec"]): ActiveFilter => ({
  col,
  kind,
  spec,
});
const now = new Date(2026, 9, 8, 15, 30);

/** The predicate and the values bound for it. */
function sqlOf(filter: ActiveFilter, at: Date = now) {
  const { bind, params } = binder();
  return { sql: filterToSql(filter, bind, at), params };
}

describe("filterToSql", () => {
  it("compares numbers as numbers", () => {
    expect(sqlOf(f("amount", "number", { op: "gt", a: "100" }))).toEqual({
      sql: '"amount" > $1',
      params: [100],
    });
    expect(sqlOf(f("amount", "number", { op: "ne", a: "7" }))).toEqual({
      sql: '"amount" <> $1',
      params: [7],
    });
    expect(sqlOf(f("amount", "number", { op: "between", a: "20", b: "10" }))).toEqual({
      sql: '"amount" BETWEEN $1 AND $2',
      params: [10, 20],
    });
  });

  it("matches text ignoring case without LIKE wildcards", () => {
    expect(sqlOf(f("n", "text", { op: "contains", a: "50%_" }))).toEqual({
      sql: `STRPOS(LOWER(CAST("n" AS VARCHAR)), CAST($1 AS VARCHAR)) > 0`,
      params: ["50%_"],
    });
    expect(sqlOf(f("n", "text", { op: "equals", a: "O'Brien" }))).toEqual({
      sql: `LOWER(CAST("n" AS VARCHAR)) = CAST($1 AS VARCHAR)`,
      params: ["o'brien"],
    });
    const starts = sqlOf(f("n", "text", { op: "startsWith", a: "ab" }));
    expect(starts.sql).toBe(
      `SUBSTR(LOWER(CAST("n" AS VARCHAR)), 1, LENGTH(CAST($1 AS VARCHAR))) = CAST($1 AS VARCHAR)`,
    );
    expect(starts.params).toEqual(["ab"]); // bound once, named twice
    const ends = sqlOf(f("n", "text", { op: "endsWith", a: "ab" }));
    expect(ends.sql).toContain("LENGTH(CAST($1 AS VARCHAR))");
    expect(ends.params).toEqual(["ab"]);
    expect(sqlOf(f("n", "text", { op: "notContains", a: "x" })).sql).toContain(
      '"n" IS NULL OR STRPOS',
    );
  });

  it("tests empty and not empty, binding nothing", () => {
    expect(sqlOf(f("n", "text", { op: "isEmpty" }))).toEqual({
      sql: `("n" IS NULL OR CAST("n" AS VARCHAR) = '')`,
      params: [],
    });
    expect(sqlOf(f("n", "number", { op: "notEmpty" }))).toEqual({
      sql: '"n" IS NOT NULL',
      params: [],
    });
  });

  it("lists ticked values, blank included", () => {
    expect(sqlOf(f("s", "text", { op: "in", values: ["error", "timeout"] }))).toEqual({
      sql: `CAST("s" AS VARCHAR) IN (CAST($1 AS VARCHAR), CAST($2 AS VARCHAR))`,
      params: ["error", "timeout"],
    });
    expect(sqlOf(f("s", "text", { op: "in", values: ["a", EMPTY_VALUE] }))).toEqual({
      sql: `(CAST("s" AS VARCHAR) IN (CAST($1 AS VARCHAR)) OR "s" IS NULL OR CAST("s" AS VARCHAR) = '')`,
      params: ["a"],
    });
    expect(sqlOf(f("s", "text", { op: "in", a: "ci", values: ["Ok"] }))).toEqual({
      sql: `LOWER(CAST("s" AS VARCHAR)) IN (CAST($1 AS VARCHAR))`,
      params: ["ok"],
    });
  });

  it("sends the same absolute bounds the browser resolved", () => {
    expect(sqlOf(f("d", "date", { op: "thisMonth" }))).toEqual({
      sql: `(CAST("d" AS TIMESTAMP) >= CAST($1 AS TIMESTAMP) AND CAST("d" AS TIMESTAMP) < CAST($2 AS TIMESTAMP))`,
      params: ["2026-10-01 00:00:00", "2026-11-01 00:00:00"],
    });
    expect(sqlOf(f("d", "date", { op: "before", a: "2026-01-01" }))).toEqual({
      sql: `(CAST("d" AS TIMESTAMP) < CAST($1 AS TIMESTAMP))`,
      params: ["2026-01-01 00:00:00"],
    });
  });

  it("sends nothing, and binds nothing, for a filter that is not usable yet", () => {
    expect(sqlOf(f("amount", "number", { op: "gt", a: "" }))).toEqual({ sql: null, params: [] });
    expect(sqlOf(f("d", "date", { op: "between", a: "2026-01-01" }))).toEqual({
      sql: null,
      params: [],
    });
  });

  it("numbers placeholders across several filters through one binder", () => {
    const { bind, params } = binder();
    const first = filterToSql(f("amount", "number", { op: "gt", a: "100" }), bind, now);
    const second = filterToSql(f("s", "text", { op: "in", values: ["a", "b"] }), bind, now);
    expect(first).toBe('"amount" > $1');
    expect(second).toBe(`CAST("s" AS VARCHAR) IN (CAST($2 AS VARCHAR), CAST($3 AS VARCHAR))`);
    expect(params).toEqual([100, "a", "b"]);
  });
});

describe("filterToSql — operands the reader controls", () => {
  const HOSTILE = [
    "it's",
    "a\\b",
    "50%_",
    "'; DROP TABLE users; --",
    "x'' OR ''1''=''1",
    "$1 OR 1=1",
    "日本語 émoji 😀 İstanbul",
    "line\nbreak\ttab",
    "x".repeat(10_000),
    '"quoted"',
  ];
  const OPS = ["contains", "notContains", "equals", "startsWith", "endsWith"] as const;

  it("puts no hostile operand in the statement, for any text operator: it is bound", () => {
    for (const raw of HOSTILE) {
      const benign = Object.fromEntries(
        OPS.map((op) => [op, sqlOf(f("note", "text", { op, a: "x" })).sql]),
      );
      for (const op of OPS) {
        const { sql, params } = sqlOf(f("note", "text", { op, a: raw }));
        // The statement is the same text whatever was typed; the value is the one bound.
        expect(sql).toBe(benign[op]);
        expect(params).toEqual([raw.toLowerCase()]);
      }
    }
  });

  it("binds hostile values of a checklist", () => {
    const { sql, params } = sqlOf(
      f("s", "text", { op: "in", values: ["o'k", "'; DROP TABLE t; --"] }),
    );
    expect(sql).toBe(`CAST("s" AS VARCHAR) IN (CAST($1 AS VARCHAR), CAST($2 AS VARCHAR))`);
    expect(params).toEqual(["o'k", "'; DROP TABLE t; --"]);
  });

  it("quotes a hostile column name as one identifier", () => {
    const { sql } = sqlOf(f('a"b; DROP', "text", { op: "contains", a: "x" }));
    expect(sql).toBe(`STRPOS(LOWER(CAST("a""b; DROP" AS VARCHAR)), CAST($1 AS VARCHAR)) > 0`);
  });

  it("binds a number only when it is a plain decimal, and as a number", () => {
    expect(sqlOf(f("n", "number", { op: "gt", a: "10" })).params).toEqual([10]);
    expect(sqlOf(f("n", "number", { op: "gt", a: " -2.5 " })).params).toEqual([-2.5]);
    for (const bad of [
      "1; DROP TABLE t",
      "1e3",
      "0x10",
      "NaN",
      "Infinity",
      "1 OR 1=1",
      "--1",
      "+1",
    ]) {
      expect(sqlOf(f("n", "number", { op: "gt", a: bad }))).toEqual({ sql: null, params: [] });
    }
    expect(() => numberValue("1 OR 1=1")).toThrow();
  });

  it("sends nothing for an operand SQL cannot carry", () => {
    expect(sqlOf(f("note", "text", { op: "contains", a: "a\u0000b" })).sql).toBeNull();
    expect(sqlOf(f("s", "text", { op: "in", values: ["a\u0000b"] })).sql).toBeNull();
  });

  it("builds a timestamp's text from numbers only", () => {
    expect(timestampText(new Date(2026, 0, 2, 3, 4, 5).getTime())).toBe("2026-01-02 03:04:05");
    expect(sqlOf(f("d", "date", { op: "before", a: "2026-01-01'; DROP" })).sql).toBeNull();
  });
});
