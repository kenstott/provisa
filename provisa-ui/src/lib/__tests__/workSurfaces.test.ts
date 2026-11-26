// Copyright (c) 2026 Kenneth Stott
// Canary: 194c6340-97d4-4121-a6fb-014c5869ea44
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2277: the UI's list of work surfaces is the requirement's list, name for name, and every file
// it names exists.

import { describe, it, expect } from "vitest";
import { existsSync, readFileSync } from "node:fs";
import { resolve } from "node:path";
import { WORK_SURFACES } from "../workSurfaces";

const SRC = resolve(__dirname, "../..");
const REQUIREMENTS = resolve(SRC, "../../docs/arch/requirements.yaml");

/** REQ-2277's description, its folded lines joined. */
function req2277(): string {
  const text = readFileSync(REQUIREMENTS, "utf8");
  const start = text.indexOf("\n- id: REQ-2277\n");
  expect(start).toBeGreaterThan(-1);
  const end = text.indexOf("\n- id: ", start + 1);
  return text
    .slice(start, end === -1 ? undefined : end)
    .replace(/\s+/g, " ")
    .replace(/''/g, "'");
}

/** The names in "The work surfaces are A, B, ... and Z;". */
function namedWorkSurfaces(description: string): string[] {
  const m = /The work surfaces are (.+?); adding a page to this list/.exec(description);
  expect(m, "REQ-2277 states its list of work surfaces").not.toBeNull();
  // A name may hold "and" itself ("the ontology and graph views"): only the last item is split, on
  // its last " and ".
  const items = m![1].split(", ");
  const last = items.pop()!;
  const cut = last.lastIndexOf(" and ");
  return [...items, last.slice(0, cut), last.slice(cut + " and ".length)].map((s) => s.trim());
}

describe("the work surfaces (REQ-2277)", () => {
  it("are exactly the requirement's list", () => {
    expect(Object.keys(WORK_SURFACES).sort()).toEqual(namedWorkSurfaces(req2277()).sort());
  });

  it("name files that exist", () => {
    const missing = Object.values(WORK_SURFACES)
      .flat()
      .filter((file) => !existsSync(resolve(SRC, file)));
    expect(missing).toEqual([]);
  });
});
