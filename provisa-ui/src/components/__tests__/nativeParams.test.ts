// Copyright (c) 2026 Kenneth Stott
// Canary: 54fc7a52-1f51-4d97-a8e4-55220cc0d6e7
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// Preview/Profile param gating: path_param columns are required before a
// governed SELECT * can run; values become WHERE predicates.

import { describe, it, expect } from "vitest";
import {
  buildParamWhere,
  pagedViewerSql,
  previewSql,
  requiredParamColumns,
  requiredParamsSatisfied,
} from "../nativeParams";
import type { RegisteredTable, TableColumn } from "../../types/admin";
import type { ActiveFilter } from "../../pages/sql/columnFilter";
import { binder } from "../columnFilterSql";

function col(name: string, nativeFilterType: string | null, dataType = "text"): TableColumn {
  return {
    id: 1,
    columnName: name,
    visibleTo: [],
    writableBy: [],
    unmaskedTo: [],
    maskType: null,
    maskPattern: null,
    maskReplace: null,
    maskValue: null,
    maskPrecision: null,
    alias: null,
    computedSqlAlias: name,
    computedGqlAlias: name,
    description: null,
    dataType,
    nativeFilterType,
    isPrimaryKey: false,
    isForeignKey: false,
    isAlternateKey: false,
    scope: "domain",
    isImplicitMeasure: false,
    isImplicitDimension: false,
  };
}

const TABLE = {
  domainId: "petstore",
  schemaName: "api",
  tableName: "pets",
  alias: null,
  columns: [col("status", "query_param"), col("petId", "path_param", "bigint"), col("name", null)],
} as unknown as RegisteredTable;

describe("nativeParams", () => {
  it("identifies path_param columns as required", () => {
    expect(requiredParamColumns(TABLE).map((c) => c.columnName)).toEqual(["petId"]);
  });

  it("blocks until every required param is non-blank", () => {
    expect(requiredParamsSatisfied(TABLE, {})).toBe(false);
    expect(requiredParamsSatisfied(TABLE, { petId: " " })).toBe(false);
    expect(requiredParamsSatisfied(TABLE, { petId: "7" })).toBe(true);
  });

  it("builds WHERE from provided params, each value bound", () => {
    const { bind, params } = binder();
    const where = buildParamWhere(TABLE, { petId: "7", status: "so'ld" }, bind);
    // Required parameters first, then optional ones; a numeric column's value is a number.
    expect(where).toBe(` WHERE "petId" = $1 AND "status" = $2`);
    expect(params).toEqual([7, "so'ld"]);
  });

  it("omits blank optional params", () => {
    const { bind, params } = binder();
    expect(buildParamWhere(TABLE, { petId: "7", status: "" }, bind)).toBe(` WHERE "petId" = $1`);
    expect(params).toEqual([7]);
  });

  it("previewSql targets domain-qualified relation with LIMIT", () => {
    expect(previewSql(TABLE, { petId: "7" })).toEqual({
      sql: `SELECT * FROM "petstore"."pets" WHERE "petId" = $1 LIMIT 1000`,
      params: [7],
    });
  });

  it("previewSql uses domainId (not physical schemaName) and alias (not physical tableName)", () => {
    const aliasedTable = {
      domainId: "pet-store",
      schemaName: "default",
      tableName: "get_inventory",
      alias: "inventory",
      columns: [],
    } as unknown as RegisteredTable;
    expect(previewSql(aliasedTable, {})).toEqual({
      sql: `SELECT * FROM "pet_store"."inventory" LIMIT 1000`,
      params: [],
    });
  });

  describe("pagedViewerSql — one page per query, choices pushed into SQL", () => {
    it("pages with LIMIT pageSize+1 / OFFSET, never the whole dataset", () => {
      const { sql, params } = pagedViewerSql(TABLE, { petId: "7" }, [], [], [], 3, 100);
      expect(sql).toContain("LIMIT 101 OFFSET 300");
      expect(sql).toContain(`WHERE "petId" = $1`);
      expect(params).toEqual([7]);
    });

    it("pushes column filters into WHERE against the full relation, values bound", () => {
      const f: ActiveFilter = { col: "name", kind: "text", spec: { op: "contains", a: "re'X" } };
      const { sql, params } = pagedViewerSql(TABLE, { petId: "7" }, [f], [], [], 0, 100);
      expect(sql).toContain(
        `WHERE "petId" = $1 AND STRPOS(LOWER(CAST("name" AS VARCHAR)), CAST($2 AS VARCHAR)) > 0`,
      );
      // One list numbers the native parameter and the filter; nothing typed is in the text.
      expect(params).toEqual([7, "re'x"]);
      expect(sql).not.toContain("re'");
    });

    it("orders by group columns first, then sorts, then pk tiebreaker", () => {
      const { sql } = pagedViewerSql(
        TABLE,
        {},
        [],
        [{ col: "name", dir: "desc" }],
        ["status"],
        0,
        100,
      );
      expect(sql).toContain(`ORDER BY "status" ASC, "name" DESC`);
    });

    // The defect: a sorted or grouped column was written between bare double quotes, so a column
    // whose name holds a double quote ended its own identifier and the rest of the name was read
    // as SQL. Every identifier goes through the one quoter (naming.quoteIdent).
    it("writes a column whose name holds a double quote as one identifier", () => {
      const hostile = 'a" DESC, (SELECT 1) --';
      const { sql } = pagedViewerSql(
        TABLE,
        {},
        [],
        [{ col: hostile, dir: "asc" }],
        ['g"x'],
        0,
        100,
      );
      expect(sql).toContain(`ORDER BY "g""x" ASC, "a"" DESC, (SELECT 1) --" ASC LIMIT 101`);
      // Take the quoted identifiers out and nothing of either name is left as SQL.
      const bare = sql.replace(/"(?:[^"]|"")*"/g, "<id>");
      expect(bare).toBe("SELECT * FROM <id>.<id> ORDER BY <id> ASC, <id> ASC LIMIT 101 OFFSET 0");
    });

    it("writes a sort direction only as ASC or DESC", () => {
      const { sql } = pagedViewerSql(
        TABLE,
        {},
        [],
        [{ col: "name", dir: "desc; DROP TABLE t" as "desc" }],
        [],
        0,
        100,
      );
      expect(sql).toContain(`ORDER BY "name" ASC LIMIT`);
    });

    const pkTable = {
      ...TABLE,
      columns: [
        ...TABLE.columns.map((c) => ({ ...c })),
        { ...col("id", null, "bigint"), isPrimaryKey: true },
      ],
    } as unknown as RegisteredTable;

    it("appends primary-key columns as a stable-paging tiebreaker under a chosen order", () => {
      const { sql } = pagedViewerSql(pkTable, {}, [], [{ col: "name", dir: "asc" }], [], 0, 100);
      expect(sql).toContain(`ORDER BY "name" ASC, "id" ASC`);
    });

    // A pk-only ORDER BY sorts a relation the user never asked to sort, and that
    // sort is a full scan — it stalled every telemetry report for minutes.
    it("emits no ORDER BY when no sort or grouping is chosen", () => {
      expect(pagedViewerSql(pkTable, {}, [], [], [], 0, 100)).toEqual({
        sql: `SELECT * FROM "petstore"."pets" LIMIT 101 OFFSET 0`,
        params: [],
      });
    });
  });
});
