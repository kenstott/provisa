// Copyright (c) 2026 Kenneth Stott
// Canary: 9c4e2a71-5b3d-4f6e-8a1c-7d2b0e9f3c58
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1163 (amended 2026-10-25): time travel is an argument of the relation that has versions,
// written where the statement names it -- "banking"."view_eod_totals"(as_recorded => '…'). The SQL
// explorer's "as of" and "as recorded" pickers write those arguments into the statement: on each
// relation that travels in time, as_of only where it maps a valid time (a bitemporal one; an
// Iceberg or Delta table travels by its snapshots, as_recorded alone). The text stays the one
// statement that runs; the pickers only edit it.

import { tableSqlRef, type SqlNamedTable } from "../../naming";

export interface TimeTravelRelation extends SqlNamedTable {
  timeTravel: "bitemporal" | "snapshot" | null;
}

export interface TimeTravelPick {
  asOf: string | null;
  asRecorded: string | null;
}

function escapeRegExp(text: string): string {
  return text.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

/** The argument list a relation of ``kind`` is read with under ``pick``; "" when none applies. */
function argumentsFor(kind: "bitemporal" | "snapshot", pick: TimeTravelPick): string {
  const said: string[] = [];
  if (kind === "bitemporal" && pick.asOf) said.push(`as_of => '${pick.asOf}'`);
  if (pick.asRecorded) said.push(`as_recorded => '${pick.asRecorded}'`);
  return said.length > 0 ? `(${said.join(", ")})` : "";
}

/**
 * ``sql`` with every reference to a relation of ``relations`` that travels in time read at
 * ``pick``: its arguments written after its name, replacing any it had; none when the pick says
 * nothing for it. A relation that keeps no versions is left as it is.
 */
export function withTimeTravel(
  sql: string,
  relations: readonly TimeTravelRelation[],
  pick: TimeTravelPick,
): string {
  let out = sql;
  for (const relation of relations) {
    if (relation.timeTravel === null) continue;
    const ref = escapeRegExp(tableSqlRef(relation));
    // The reference, then any argument list it already carries (a time-travel call's arguments
    // are quoted times, so they hold no parenthesis).
    const reference = new RegExp(`${ref}(\\s*\\([^()]*\\))?`, "g");
    out = out.replace(
      reference,
      `${tableSqlRef(relation)}${argumentsFor(relation.timeTravel, pick)}`,
    );
  }
  return out;
}
