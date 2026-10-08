// Copyright (c) 2026 Kenneth Stott
// Canary: e5d98815-cf61-4a3f-9867-d2fd891df51c
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-471: a registered table has ONE SQL name, built in ONE place (src/naming.ts). In August two
// of five call sites were left on an abandoned form of the name for two months, and six files
// each carried a private copy of the domain-normalising function. This test reads every source
// file, so a new hand-built name fails the build wherever it is written. It has no allowlist:
// the naming module is the only file the patterns may appear in.

import { readdirSync, readFileSync, statSync } from "node:fs";
import { join, relative, sep } from "node:path";
import { describe, expect, it } from "vitest";
import {
  columnSqlRef,
  domainToSqlName,
  physicalRelationName,
  relationName,
  sourceToCatalog,
  tableSqlRef,
} from "../naming";

const SRC = join(__dirname, "..");
const AUTHORITY = "naming.ts";

function sourceFiles(dir: string): string[] {
  return readdirSync(dir).flatMap((name) => {
    const path = join(dir, name);
    if (statSync(path).isDirectory()) {
      return name === "__tests__" || name === "i18n" ? [] : sourceFiles(path);
    }
    return /\.tsx?$/.test(name) && !/\.test\.tsx?$/.test(name) ? [path] : [];
  });
}

const FILES = sourceFiles(SRC).filter((f) => relative(SRC, f).split(sep).join("/") !== AUTHORITY);

function offenders(pattern: RegExp): string[] {
  return FILES.flatMap((file) =>
    readFileSync(file, "utf8")
      .split("\n")
      .map((line, i) =>
        pattern.test(line) ? `${relative(SRC, file)}:${i + 1}: ${line.trim()}` : "",
      )
      .filter(Boolean),
  );
}

describe("a registered table's SQL name is built in one place", () => {
  it("reads a non-trivial number of source files", () => {
    expect(FILES.length).toBeGreaterThan(200);
  });

  it("no file but the naming module normalises a domain id into a SQL name", () => {
    // The body of domainToSqlName: any second copy is a second rule waiting to drift.
    expect(offenders(/replace\(\/\[\^a-zA-Z0-9\]\/g,\s*"_"\)/)).toEqual([]);
  });

  it("no file but the naming module writes a quoted two-part name by hand", () => {
    // `"${a}"."${b}"` -- a table name or a column reference assembled in place.
    expect(offenders(/"\$\{[^}]*\}"\."\$\{/)).toEqual([]);
  });

  it("no file qualifies a table by its physical schema or raw domain id in a template", () => {
    // `${x.schemaName}.${...}` or `${x.domainId}.${...}` inside a quoted or FROM/JOIN position:
    // the pipeline resolves the domain's SQL name, not the physical schema and not the raw id.
    expect(offenders(/(FROM|JOIN|from|join)\s+"?\$\{[^}]*(schemaName|domainId)/)).toEqual([]);
    expect(offenders(/\$\{[^}]*\bdomainId\}\.\$\{/)).toEqual([]);
  });

  it("the builders produce the name the governed pipeline resolves", () => {
    const table = { domainId: "pet-store", tableName: "inventory_raw", alias: "inventory" };
    expect(domainToSqlName("pet-store")).toBe("pet_store");
    expect(tableSqlRef(table)).toBe('"pet_store"."inventory"');
    expect(tableSqlRef({ ...table, alias: null })).toBe('"pet_store"."inventory_raw"');
    expect(columnSqlRef(table, "sku")).toBe('"inventory"."sku"');
    expect(relationName(table)).toBe("pet_store.inventory_raw");
    // A source id becomes its catalog name by the server's rule (hyphens only); the stored
    // schema and table names are not rewritten.
    expect(sourceToCatalog("test-askamerica")).toBe("test_askamerica");
    expect(
      physicalRelationName({ sourceId: "sales-pg", schemaName: "public", tableName: "orders" }),
    ).toBe("sales_pg.public.orders");
  });
});
