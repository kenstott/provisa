// Copyright (c) 2026 Kenneth Stott
// Canary: 4576fb2d-f7c6-459d-99ef-7073b56b6be0
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1937: the SQL form of each typed filter, for the server-paged viewer.

import { describe, it, expect } from "vitest";
import {
  filterToSql,
  numberLiteral,
  quoteIdent,
  quoteLiteral,
  timestampLiteral,
} from "../columnFilterSql";
import { EMPTY_VALUE, type ActiveFilter } from "../../pages/sql/columnFilter";

const f = (col: string, kind: ActiveFilter["kind"], spec: ActiveFilter["spec"]): ActiveFilter => ({
  col,
  kind,
  spec,
});
const now = new Date(2026, 9, 8, 15, 30);

describe("filterToSql", () => {
  it("compares numbers as numbers", () => {
    expect(filterToSql(f("amount", "number", { op: "gt", a: "100" }))).toBe('"amount" > 100');
    expect(filterToSql(f("amount", "number", { op: "ne", a: "7" }))).toBe('"amount" <> 7');
    expect(filterToSql(f("amount", "number", { op: "between", a: "20", b: "10" }))).toBe(
      '"amount" BETWEEN 10 AND 20',
    );
  });

  it("matches text ignoring case without LIKE wildcards", () => {
    expect(filterToSql(f("n", "text", { op: "contains", a: "50%_" }))).toBe(
      `STRPOS(LOWER(CAST("n" AS VARCHAR)), '50%_') > 0`,
    );
    expect(filterToSql(f("n", "text", { op: "equals", a: "O'Brien" }))).toBe(
      `LOWER(CAST("n" AS VARCHAR)) = 'o''brien'`,
    );
    expect(filterToSql(f("n", "text", { op: "startsWith", a: "ab" }))).toContain("SUBSTR(");
    expect(filterToSql(f("n", "text", { op: "endsWith", a: "ab" }))).toContain("LENGTH(");
    expect(filterToSql(f("n", "text", { op: "notContains", a: "x" }))).toContain(
      '"n" IS NULL OR STRPOS',
    );
  });

  it("tests empty and not empty", () => {
    expect(filterToSql(f("n", "text", { op: "isEmpty" }))).toBe(
      `("n" IS NULL OR CAST("n" AS VARCHAR) = '')`,
    );
    expect(filterToSql(f("n", "number", { op: "notEmpty" }))).toBe('"n" IS NOT NULL');
  });

  it("lists ticked values, blank included", () => {
    expect(filterToSql(f("s", "text", { op: "in", values: ["error", "timeout"] }))).toBe(
      `CAST("s" AS VARCHAR) IN ('error', 'timeout')`,
    );
    expect(filterToSql(f("s", "text", { op: "in", values: ["a", EMPTY_VALUE] }))).toBe(
      `(CAST("s" AS VARCHAR) IN ('a') OR "s" IS NULL OR CAST("s" AS VARCHAR) = '')`,
    );
    expect(filterToSql(f("s", "text", { op: "in", a: "ci", values: ["Ok"] }))).toBe(
      `LOWER(CAST("s" AS VARCHAR)) IN ('ok')`,
    );
  });

  it("sends the same absolute bounds the browser resolved", () => {
    expect(filterToSql(f("d", "date", { op: "thisMonth" }), now)).toBe(
      `(CAST("d" AS TIMESTAMP) >= TIMESTAMP '2026-10-01 00:00:00' AND CAST("d" AS TIMESTAMP) < TIMESTAMP '2026-11-01 00:00:00')`,
    );
    expect(filterToSql(f("d", "date", { op: "before", a: "2026-01-01" }), now)).toBe(
      `(CAST("d" AS TIMESTAMP) < TIMESTAMP '2026-01-01 00:00:00')`,
    );
  });

  it("sends nothing for a filter that is not usable yet", () => {
    expect(filterToSql(f("amount", "number", { op: "gt", a: "" }))).toBeNull();
    expect(filterToSql(f("d", "date", { op: "between", a: "2026-01-01" }))).toBeNull();
  });
});

describe("filterToSql — operands the reader controls", () => {
  const text = (op: ActiveFilter["spec"]["op"], a: string) =>
    filterToSql(f("note", "text", { op, a })) as string;
  const HOSTILE = [
    "it's",
    "a\\b",
    "50%_",
    "'; DROP TABLE users; --",
    "x'' OR ''1''=''1",
    "日本語 émoji 😀 İstanbul",
    "line\nbreak\ttab",
    "x".repeat(10_000),
    '"quoted"',
  ];

  it("writes every hostile operand as the one escaped literal, for every text operator", () => {
    for (const raw of HOSTILE) {
      const lit = `'${raw.toLowerCase().replace(/'/g, "''")}'`;
      expect(text("contains", raw)).toBe(`STRPOS(LOWER(CAST("note" AS VARCHAR)), ${lit}) > 0`);
      expect(text("equals", raw)).toBe(`LOWER(CAST("note" AS VARCHAR)) = ${lit}`);
      expect(text("startsWith", raw)).toContain(`= ${lit}`);
      expect(text("endsWith", raw)).toContain(`= ${lit}`);
      // no operand ever leaves its literal: removing every literal leaves only the fixed template
      const stripped = text("contains", raw).replace(lit, "<L>");
      expect(stripped).toBe('STRPOS(LOWER(CAST("note" AS VARCHAR)), <L>) > 0');
    }
  });

  it("writes hostile values in a checklist as escaped literals", () => {
    const sql = filterToSql(f("s", "text", { op: "in", values: ["o'k", "'; DROP TABLE t; --"] }));
    expect(sql).toBe(`CAST("s" AS VARCHAR) IN ('o''k', '''; DROP TABLE t; --')`);
  });

  it("quotes a hostile column name as one identifier", () => {
    const sql = filterToSql(f('a"b; DROP', "text", { op: "contains", a: "x" }));
    expect(sql).toBe(`STRPOS(LOWER(CAST("a""b; DROP" AS VARCHAR)), 'x') > 0`);
  });

  it("writes a number only when it is a plain decimal, and never as text", () => {
    expect(filterToSql(f("n", "number", { op: "gt", a: "10" }))).toBe('"n" > 10');
    expect(filterToSql(f("n", "number", { op: "gt", a: " -2.5 " }))).toBe('"n" > -2.5');
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
      expect(filterToSql(f("n", "number", { op: "gt", a: bad }))).toBeNull();
    }
    expect(() => numberLiteral("1 OR 1=1")).toThrow();
  });

  it("sends nothing for an operand SQL cannot carry", () => {
    expect(filterToSql(f("note", "text", { op: "contains", a: "a\u0000b" }))).toBeNull();
    expect(filterToSql(f("s", "text", { op: "in", values: ["a\u0000b"] }))).toBeNull();
  });

  it("writes dates only as typed literals built from numbers", () => {
    expect(timestampLiteral(new Date(2026, 0, 2, 3, 4, 5).getTime())).toBe(
      "TIMESTAMP '2026-01-02 03:04:05'",
    );
    expect(filterToSql(f("d", "date", { op: "before", a: "2026-01-01'; DROP" }), now)).toBeNull();
  });

  it("quotes through the two functions", () => {
    expect(quoteLiteral("a'b")).toBe("'a''b'");
    expect(quoteIdent('a"b')).toBe('"a""b"');
  });
});
