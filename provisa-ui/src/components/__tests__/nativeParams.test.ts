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

  it("builds WHERE from provided params, numeric unquoted, strings escaped", () => {
    const where = buildParamWhere(TABLE, { petId: "7", status: "so'ld" });
    expect(where).toBe(` WHERE "petId" = 7 AND "status" = 'so''ld'`);
  });

  it("omits blank optional params", () => {
    expect(buildParamWhere(TABLE, { petId: "7", status: "" })).toBe(` WHERE "petId" = 7`);
  });

  it("previewSql targets domain-qualified relation with LIMIT", () => {
    expect(previewSql(TABLE, { petId: "7" })).toBe(
      `SELECT * FROM "petstore"."pets" WHERE "petId" = 7 LIMIT 1000`,
    );
  });

  describe("pagedViewerSql — one page per query, choices pushed into SQL", () => {
    it("pages with LIMIT pageSize+1 / OFFSET, never the whole dataset", () => {
      const sql = pagedViewerSql(TABLE, { petId: "7" }, {}, [], [], 3, 100);
      expect(sql).toContain("LIMIT 101 OFFSET 300");
      expect(sql).toContain(`WHERE "petId" = 7`);
    });

    it("pushes column filters into WHERE against the full relation", () => {
      const sql = pagedViewerSql(TABLE, {}, { name: "re'x" }, [], [], 0, 100);
      expect(sql).toContain(`LOWER(CAST("name" AS VARCHAR)) LIKE LOWER('%re''x%')`);
    });

    it("orders by group columns first, then sorts, then pk tiebreaker", () => {
      const sql = pagedViewerSql(
        TABLE,
        {},
        {},
        [{ col: "name", dir: "desc" }],
        ["status"],
        0,
        100,
      );
      expect(sql).toContain(`ORDER BY "status" ASC, "name" DESC`);
    });

    it("appends primary-key columns as a stable-paging tiebreaker", () => {
      const pkTable = {
        ...TABLE,
        columns: [...TABLE.columns.map((c) => ({ ...c })), { ...col("id", null, "bigint"), isPrimaryKey: true }],
      } as unknown as RegisteredTable;
      const sql = pagedViewerSql(pkTable, {}, {}, [], [], 0, 100);
      expect(sql).toContain(`ORDER BY "id" ASC`);
    });
  });
});
