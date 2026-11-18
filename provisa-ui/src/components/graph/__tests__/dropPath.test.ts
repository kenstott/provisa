// Copyright (c) 2026 Kenneth Stott
// Canary: 5e0a7c93-1b42-4f8d-9c36-d2e8f4a1b706
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2135: the one path finder the Cypher and SPARQL views share for a drop onto a result.

import { describe, expect, it } from "vitest";
import { dropPath, noDropPath, type PathScope } from "../dropPath";
import type { SchemaEdge } from "../schemaPath";

const e = (source: string, target: string, join = `${source}-${target}`): SchemaEdge & { type: string } => ({
  source,
  target,
  join,
  type: join,
});

/** ``Domain:Table`` labels; ``meta`` is a system domain. */
function scope(labels: string[], checked: string[] = []): PathScope {
  return {
    nodeLabels: labels.map((l) => {
      const [domainLabel, tableLabel] = l.split(":");
      return { domainLabel, domainId: domainLabel, tableLabel, systemDomain: domainLabel === "Meta" };
    }),
    checkedDomains: new Set(checked),
  };
}

const USERS = "PetStore:Users";
const INQUIRIES = "PetStore:Inquiries";
const PETS = "PetStore:Pets";
const EMPLOYEES = "Shelter:Employees";
const ASSIGNMENTS = "Shelter:Assignments";
const REGISTERED = "Meta:RegisteredTables";
const BILLS = "Finance:Bills";
const ALL = [USERS, INQUIRIES, PETS, EMPLOYEES, ASSIGNMENTS, REGISTERED, BILLS];
const SELECTED = scope(ALL, ["PetStore", "Shelter"]);

/** The labels each path walks, start first. */
const walks = (found: ReturnType<typeof dropPath>) =>
  "paths" in found ? found.paths.map((p) => [p[0].from, ...p.map((s) => s.to)]) : found;

describe("dropPath: within the selected domains", () => {
  const META_WAY = [e(USERS, REGISTERED), e(EMPLOYEES, REGISTERED)];

  it("never passes through the meta domain's registered tables", () => {
    expect(walks(dropPath(META_WAY, [USERS], [EMPLOYEES], new Set(), SELECTED))).toEqual({
      hops: null,
    });
  });

  it("leaves the meta domain out even when every domain is selected", () => {
    expect(walks(dropPath(META_WAY, [USERS], [EMPLOYEES], new Set(), scope(ALL)))).toEqual({
      hops: null,
    });
  });

  it("takes a meta path when the drop itself is of the meta domain and it is selected", () => {
    const found = dropPath(META_WAY, [USERS], [REGISTERED], new Set(), scope(ALL));
    expect(walks(found)).toEqual([[USERS, REGISTERED]]);
  });

  it("never passes through a class of an unselected domain", () => {
    const rels = [e(USERS, BILLS), e(BILLS, EMPLOYEES)];
    expect(walks(dropPath(rels, [USERS], [EMPLOYEES], new Set(), SELECTED))).toEqual({
      hops: null,
    });
  });

  it("goes the selected domains' longer way round a shorter one outside them", () => {
    const rels = [
      ...META_WAY,
      e(USERS, INQUIRIES),
      e(INQUIRIES, PETS),
      e(ASSIGNMENTS, PETS),
      e(ASSIGNMENTS, EMPLOYEES),
    ];
    expect(walks(dropPath(rels, [USERS], [EMPLOYEES], new Set(), SELECTED))).toEqual([
      [USERS, INQUIRIES, PETS, ASSIGNMENTS, EMPLOYEES],
    ]);
  });

  it("never starts from or ends on a label of an unselected domain", () => {
    const rels = [e(BILLS, USERS)];
    expect(walks(dropPath(rels, [BILLS], [USERS], new Set(), SELECTED))).toEqual({ hops: null });
    expect(walks(dropPath(rels, [USERS], [BILLS], new Set(), SELECTED))).toEqual({ hops: null });
  });
});

describe("dropPath: one path", () => {
  it("takes one of several as-short paths", () => {
    const rels = [e(USERS, INQUIRIES), e(USERS, PETS), e(INQUIRIES, EMPLOYEES), e(PETS, EMPLOYEES)];
    const found = dropPath(rels, [USERS], [EMPLOYEES], new Set(), SELECTED);
    expect(walks(found)).toEqual([[USERS, INQUIRIES, EMPLOYEES]]);
  });

  it("starts from one of the held labels, not from each", () => {
    const rels = [e(USERS, ASSIGNMENTS), e(INQUIRIES, ASSIGNMENTS), e(ASSIGNMENTS, EMPLOYEES)];
    const found = dropPath(rels, [USERS, INQUIRIES], [EMPLOYEES], new Set(), SELECTED);
    expect(found).toMatchObject({ start: USERS });
    expect(walks(found)).toEqual([[USERS, ASSIGNMENTS, EMPLOYEES]]);
  });

  it("keeps each relationship of a one-hop path, all between the same two labels", () => {
    const rels = [
      e(USERS, EMPLOYEES, "KNOWS"),
      e(USERS, EMPLOYEES, "HIRED"),
      e(INQUIRIES, EMPLOYEES, "ASKED"),
    ];
    const found = dropPath(rels, [USERS, INQUIRIES], [EMPLOYEES], new Set(), SELECTED);
    expect("paths" in found && found.paths.map((p) => p.map((s) => s.edge.join))).toEqual([
      ["KNOWS"],
      ["HIRED"],
    ]);
  });

  it("never goes through a parameterized label", () => {
    const rels = [e(USERS, ASSIGNMENTS), e(ASSIGNMENTS, EMPLOYEES)];
    expect(walks(dropPath(rels, [USERS], [EMPLOYEES], new Set([ASSIGNMENTS]), SELECTED))).toEqual(
      { hops: null },
    );
  });
});

describe("dropPath: refused when none", () => {
  it("says the hops of a path past the cap", () => {
    const labels = ["PetStore:A", "PetStore:B", "PetStore:C", "PetStore:D", "PetStore:E", "PetStore:F"];
    const chain = labels.slice(1).map((l, i) => e(labels[i], l));
    expect(dropPath(chain, [labels[0]], [labels[5]], new Set(), scope(labels))).toEqual({ hops: 5 });
  });

  it("names the dropped class and the held ones", () => {
    expect(noDropPath(EMPLOYEES, `${USERS}, ${INQUIRIES}`, null)).toEqual({
      key: "graphFrame.sparqlDropNoPath",
      values: { name: EMPLOYEES, classes: `${USERS}, ${INQUIRIES}` },
    });
    expect(noDropPath(EMPLOYEES, USERS, 5)).toEqual({
      key: "graphFrame.sparqlDropTooFar",
      values: { name: EMPLOYEES, classes: USERS, hops: 5, most: 4 },
    });
  });
});
