// Copyright (c) 2026 Kenneth Stott
// Canary: 4f1d8b62-0a9e-4c3b-b7d5-6e2c1a8f9d07
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1163: the SQL explorer's time pickers write the relation's time-travel arguments into the
// statement, on exactly the relations that travel in time.

import { describe, expect, it } from "vitest";

import { withTimeTravel, type TimeTravelRelation } from "../timeTravelArgs";

const relation = (
  tableName: string,
  timeTravel: TimeTravelRelation["timeTravel"],
): TimeTravelRelation => ({ tableName, domainId: "banking", timeTravel });

const RELATIONS = [
  relation("view_eod_totals", "bitemporal"),
  relation("accounts", null),
  relation("events", "snapshot"),
];

const SQL =
  'SELECT * FROM "banking"."view_eod_totals" AS e JOIN "banking"."accounts" AS a ON a.id = e.id';

describe("the time pickers", () => {
  it("write the arguments on a relation that travels in time and leave the others", () => {
    expect(withTimeTravel(SQL, RELATIONS, { asOf: null, asRecorded: "2026-10-01T17:00:00" })).toBe(
      'SELECT * FROM "banking"."view_eod_totals"(as_recorded => \'2026-10-01T17:00:00\') AS e ' +
        'JOIN "banking"."accounts" AS a ON a.id = e.id',
    );
  });

  it("replace the arguments a reference already carries, and clear them when nothing is picked", () => {
    const once = withTimeTravel(SQL, RELATIONS, { asOf: "2026-09-30T00:00:00", asRecorded: null });
    const twice = withTimeTravel(once, RELATIONS, {
      asOf: "2026-09-30T00:00:00",
      asRecorded: "2026-10-12T17:00:00",
    });
    expect(twice).toContain(
      "\"view_eod_totals\"(as_of => '2026-09-30T00:00:00', as_recorded => '2026-10-12T17:00:00')",
    );
    expect(withTimeTravel(twice, RELATIONS, { asOf: null, asRecorded: null })).toBe(SQL);
  });

  it("give a snapshot table as_recorded alone", () => {
    expect(
      withTimeTravel('SELECT * FROM "banking"."events" AS x', RELATIONS, {
        asOf: "2026-09-30T00:00:00",
        asRecorded: "2026-10-01T00:00:00",
      }),
    ).toBe('SELECT * FROM "banking"."events"(as_recorded => \'2026-10-01T00:00:00\') AS x');
  });
});
