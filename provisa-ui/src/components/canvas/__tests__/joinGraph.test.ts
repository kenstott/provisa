// Copyright (c) 2026 Kenneth Stott
// Canary: 442d5af0-5a0f-4bca-ab82-c65b520f29f0
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// The join canvas's statement, on the maintainer's canvas: pet_companions, pets, vets and
// playgroups, with playgroups.id→pets.id, pets.id→vets.id and pet_companions' two joins into pets.
// The statement it must build is __fixtures__/pets-canvas.sql, which
// tests/unit/test_canvas_join_sql.py runs on DuckDB.

import { readFileSync } from "fs";
import { resolve } from "path";
import { describe, expect, it } from "vitest";

import { approvedEdges, buildJoinSql, type ApprovedJoin } from "../joinGraph";

const table = (tableName: string) => ({ tableName, domainId: "petstore" });
const edge = (fromTable: string, fromCol: string, toTable: string, toCol: string) => ({
  id: `${fromTable}__${fromCol}__${toTable}__${toCol}`,
  fromTable,
  fromCol,
  toTable,
  toCol,
  cardinality: "many-to-one" as const,
});

const CANVAS = ["pet_companions", "pets", "vets", "playgroups"].map(table);
// Each table's key: pets, vets and playgroups are keyed on id; the junction on none.
const KEYS = new Map([
  ["pets", new Set(["id"])],
  ["vets", new Set(["id"])],
  ["playgroups", new Set(["id"])],
  ["pet_companions", new Set<string>()],
]);
const EDGES = [
  edge("playgroups", "id", "pets", "id"),
  edge("pets", "id", "vets", "id"),
  edge("pet_companions", "pet_id", "pets", "id"),
  edge("pet_companions", "companion_pet_id", "pets", "id"),
];

describe("the join canvas's statement", () => {
  it("joins every table it reaches from the first, a second join of one table under its own alias", () => {
    const expected = readFileSync(
      resolve(__dirname, "../__fixtures__/pets-canvas.sql"),
      "utf8",
    ).trim();
    expect(buildJoinSql(CANVAS, EDGES, KEYS)).toEqual({ sql: expected });
  });

  it("joins no table on a table not yet joined, whatever order the joins were drawn in", () => {
    // playgroups.id→pets.id drawn first: neither end is the first table, and pets is joined
    // before playgroups is.
    const result = buildJoinSql(CANVAS, [...EDGES].reverse(), KEYS);
    expect("sql" in result && result.sql.split("\n")[2]).toMatch(
      /^JOIN "petstore"."pets" pets ON pet_companions/,
    );
  });

  it("names a table no join reaches instead of building a statement that cannot run", () => {
    const edges = EDGES.filter((e) => e.fromTable !== "playgroups");
    expect(buildJoinSql(CANVAS, edges, KEYS)).toEqual({ unjoined: ["playgroups"] });
  });

  it("selects the chosen columns in canvas order, qualifying a name two tables share", () => {
    const result = buildJoinSql(
      CANVAS,
      EDGES,
      KEYS,
      new Map([
        ["pets", new Set(["name"])],
        ["vets", new Set(["name"])],
        ["pet_companions", new Set(["relation_type"])],
      ]),
    );
    expect("sql" in result && result.sql.split("\n")[0]).toBe(
      'SELECT pet_companions."relation_type", pets."name" pets_name, vets."name" vets_name',
    );
  });
});

// The server's approved joins for the maintainer's model (sql_validator.canvas_joins): pets is 1,
// pet_companions 2, vets 3, playgroups 4 -- a junction relationship is its two foreign-key joins.
const APPROVED: ApprovedJoin[] = [
  { fromTableId: 1, fromColumn: "id", toTableId: 2, toColumn: "companion_pet_id" },
  { fromTableId: 1, fromColumn: "id", toTableId: 2, toColumn: "pet_id" },
  { fromTableId: 1, fromColumn: "id", toTableId: 3, toColumn: "id" },
  { fromTableId: 1, fromColumn: "id", toTableId: 4, toColumn: "id" },
];
const NAMES = new Map([
  [1, "pets"],
  [2, "pet_companions"],
  [3, "vets"],
  [4, "playgroups"],
]);

describe("the lines the canvas draws", () => {
  it("are the approved joins between tables on the canvas, a junction's two included", () => {
    const edges = approvedEdges(APPROVED, NAMES, new Set(["pets", "pet_companions"]));
    expect(edges.map((e) => `${e.fromTable}.${e.fromCol}=${e.toTable}.${e.toCol}`)).toEqual([
      "pets.id=pet_companions.companion_pet_id",
      "pets.id=pet_companions.pet_id",
    ]);
  });

  it("join pets and pet_companions alone into a statement that reads pets twice", () => {
    const edges = approvedEdges(APPROVED, NAMES, new Set(["pets", "pet_companions"]));
    const expected = readFileSync(
      resolve(__dirname, "../__fixtures__/pets-companions.sql"),
      "utf8",
    ).trim();
    expect(buildJoinSql(["pets", "pet_companions"].map(table), edges, KEYS)).toEqual({
      sql: expected,
    });
  });

  it("draw nothing to a table not on the canvas", () => {
    expect(approvedEdges(APPROVED, NAMES, new Set(["pets"]))).toEqual([]);
  });
});
