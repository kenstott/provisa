// Copyright (c) 2026 Kenneth Stott
// Canary: 9b4f2d17-6c83-4e0a-b5d1-7a3e8c2f6d94
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2135: a drop onto a Cypher result whose query has nodes with no label labels them from the
// schema (one MATCH per label pair within the selected domains, UNION ALL, each returning its
// endpoints), then joins the dropped label with OPTIONAL MATCH; Show children uses the same rewrite
// and keeps to the selected domains. The maintainer's case (2026-11-17): Domains PetStore and
// Shelter, MATCH ()-[r:HAS_INQUIRY]->() RETURN r LIMIT 25, and a drop said the SPARQL view's "names
// no class" while Show children on an inquiry reached into another domain.
//
// The Person/Company cases write the queries tests/unit/test_cypher_drop_shapes.py runs.

import { describe, expect, it } from "vitest";
import { buildDropExpansion, type DropExpansion } from "../pages/graph-drop";
import { neighboursQuery } from "../components/graph/cypherEndpoints";
import type { PathScope } from "../components/graph/dropPath";
import type { SchemaRel } from "../components/graph/graph-schema-types";
import en from "../i18n/locales/en/graphFrame.json";

const rel = (type: string, source: string, target: string, constantSource = false): SchemaRel => ({
  type,
  source,
  target,
  properties: [],
  join: `${type} ${source} ${target}`,
  constantSource,
});

const PETS = "PetStore:Pets";
const INQUIRIES = "PetStore:Inquiries";
const USERS = "PetStore:Users";
const VETS = "PetStore:Vets";
const TICKETS = "PetStore:Tickets";
const EMPLOYEES = "Shelter:Employees";
const REGISTERED = "Meta:RegisteredTables";
const CUSTOMERS = "Finance:Customers";
const LEDGER = "Finance:Ledger";

const LABELS = [PETS, INQUIRIES, USERS, VETS, TICKETS, EMPLOYEES, REGISTERED, CUSTOMERS, LEDGER];
const scopeOf = (labels: string[], ...checked: string[]): PathScope => ({
  nodeLabels: labels.map((l) => {
    const [domainLabel, tableLabel] = l.split(":");
    return {
      domainLabel,
      domainId: domainLabel.toLowerCase(),
      tableLabel,
      systemDomain: domainLabel === "Meta",
    };
  }),
  checkedDomains: new Set(checked),
});
/** Domains (2/5): PetStore and Shelter. */
const SELECTED = scopeOf(LABELS, "petstore", "shelter");

const RELS = [
  rel("HAS_INQUIRY", PETS, INQUIRIES),
  rel("HAS_INQUIRY", CUSTOMERS, LEDGER),
  rel("SUBMITTED_BY", INQUIRIES, USERS),
  rel("TREATED_BY", PETS, VETS),
  rel("HAS_TABLE", INQUIRIES, REGISTERED, true),
  rel("ROUTED_TO", INQUIRIES, CUSTOMERS),
];
const FRAME = "MATCH ()-[r:HAS_INQUIRY]->() RETURN r LIMIT 25";

function expand(query: string, label: string, rels: SchemaRel[], scope: PathScope): DropExpansion {
  const done = buildDropExpansion(query, label, rels, new Set(), scope);
  if ("refused" in done) throw new Error(done.refused.key);
  return done;
}
const refusal = (query: string, label: string, rels: SchemaRel[], scope: PathScope) => {
  const done = buildDropExpansion(query, label, rels, new Set(), scope);
  if ("refused" in done) return done.refused;
  throw new Error(`added: ${done.query}`);
};

describe("REQ-2135 a drop onto a result with unlabelled nodes", () => {
  it("labels the endpoints of the one label pair within the selected domains, then joins", () => {
    const out = expand(FRAME, USERS, RELS, SELECTED);
    expect(out.query).toBe(
      [
        "MATCH (n1:PetStore:Pets)-[r:HAS_INQUIRY]->(n2:PetStore:Inquiries)",
        "OPTIONAL MATCH (n2)-[rUsers:SUBMITTED_BY]-(mUsers:PetStore:Users)",
        "RETURN r, n1, n2, rUsers, mUsers",
        "LIMIT 25",
      ].join("\n"),
    );
    expect(out.notices).toEqual([{ key: "graphFrame.cypherDropLabelled" }]);
    expect(out.targetVars).toEqual(["mUsers"]);
  });

  it("writes one MATCH per label pair, UNION ALL, each joined; a branch with no path returns nulls", () => {
    const rels = [...RELS, rel("HAS_INQUIRY", EMPLOYEES, TICKETS)];
    const out = expand(FRAME, VETS, rels, SELECTED);
    expect(out.query).toBe(
      [
        "MATCH (n1:PetStore:Pets)-[r:HAS_INQUIRY]->(n2:PetStore:Inquiries)",
        "OPTIONAL MATCH (n1)-[rVets:TREATED_BY]-(mVets:PetStore:Vets)",
        "RETURN r, n1, n2, rVets, mVets",
        "LIMIT 25",
        "UNION ALL",
        "MATCH (n1:Shelter:Employees)-[r:HAS_INQUIRY]->(n2:PetStore:Tickets)",
        "RETURN r, n1, n2, null AS rVets, null AS mVets",
        "LIMIT 25",
      ].join("\n"),
    );
  });

  it("names the dropped label's column alike in branches whose paths differ in length", () => {
    const rels = [rel("HAS_INQUIRY", PETS, INQUIRIES), rel("HAS_INQUIRY", USERS, INQUIRIES), rel("TREATED_BY", PETS, VETS)];
    const out = expand(FRAME, VETS, rels, SELECTED);
    const branches = out.query.split("\nUNION ALL\n");
    expect(branches).toHaveLength(2);
    expect(branches[0]).toContain("OPTIONAL MATCH (n1)-[rVets:TREATED_BY]-(mVets:PetStore:Vets)");
    expect(branches[0]).toContain("RETURN r, n1, n2, rVets, mVets, null AS rPets, null AS mPets");
    expect(branches[1]).toContain(
      "OPTIONAL MATCH (n2)-[rPets:HAS_INQUIRY]-(mPets:PetStore:Pets)-[rVets:TREATED_BY]-(mVets:PetStore:Vets)",
    );
    expect(branches[1]).toContain("RETURN r, n1, n2, rVets, mVets, rPets, mPets");
    expect(out.targetVars).toEqual(["mVets"]);
  });

  it("never labels an endpoint with a meta class or an unselected domain's", () => {
    const out = expand("MATCH (i:PetStore:Inquiries)-[r]->(x) RETURN i, r, x", USERS, RELS, SELECTED);
    expect(out.query).not.toContain(REGISTERED);
    expect(out.query).not.toContain("Finance");
    expect(out.query).toContain("MATCH (i:PetStore:Inquiries)-[r:SUBMITTED_BY]->(x:PetStore:Users)");
  });

  it("says in Cypher's words when no relationship joins labels within the selected domains", () => {
    expect(refusal("MATCH ()-[r:ROUTED_TO]->() RETURN r", USERS, RELS, SELECTED)).toEqual({
      key: "graphFrame.cypherDropNoPair",
    });
  });

  it("refuses by name a drop whose path would leave the selected domains, and adds nothing", () => {
    expect(refusal(FRAME, LEDGER, RELS, SELECTED)).toEqual({
      key: "graphFrame.sparqlDropNoPath",
      values: { name: LEDGER, classes: `${PETS}, ${INQUIRIES}` },
    });
  });

  it("shows a Cypher message, not the SPARQL view's, for a node with no label to give it", () => {
    const shown = refusal("MATCH (n) RETURN n LIMIT 25", USERS, RELS, SELECTED);
    expect(shown.key).toBe("graphFrame.cypherDropNoLabel");
    expect(shown.key).not.toBe("graphFrame.sparqlDropNoClass");
    expect(en.graphFrame.cypherDropNoLabel).not.toMatch(/\bclass\b|\bvariable\b/);
    expect(en.graphFrame.cypherDropNoLabel).toMatch(/label/);
  });

  it("drops again onto the rewritten query, every branch joined", () => {
    const once = expand(FRAME, USERS, RELS, SELECTED).query;
    const twice = expand(once, VETS, RELS, SELECTED);
    expect(twice.query).toContain("OPTIONAL MATCH (n1)-[rVets:TREATED_BY]-(mVets:PetStore:Vets)");
    expect(twice.query).toContain("RETURN r, n1, n2, rUsers, mUsers, rVets, mVets");
    expect(twice.notices).toEqual([]);
  });
});

describe("REQ-2135 Show children keeps to the selected domains", () => {
  it("labels each child from the schema, one MATCH per label pair, and none outside the domains", () => {
    const rels = [...RELS, rel("ABOUT", INQUIRIES, PETS)];
    expect(neighboursQuery(INQUIRIES, 17, "children", rels, new Set(), SELECTED)).toBe(
      [
        "MATCH (n:PetStore:Inquiries)-[r:SUBMITTED_BY]->(child:PetStore:Users)",
        "WHERE id(n) IN [17]",
        "RETURN n, r, child",
        "UNION ALL",
        "MATCH (n:PetStore:Inquiries)-[r:ABOUT]->(child:PetStore:Pets)",
        "WHERE id(n) IN [17]",
        "RETURN n, r, child",
      ].join("\n"),
    );
  });

  it("reaches another domain when that domain is selected", () => {
    const q = neighboursQuery(INQUIRIES, 17, "children", RELS, new Set(), scopeOf(LABELS));
    expect(q).toContain("(child:Finance:Customers)");
    expect(q).not.toContain(REGISTERED);
  });

  it("reads parents the same way", () => {
    expect(neighboursQuery(INQUIRIES, 3, "parents", RELS, new Set(), SELECTED)).toBe(
      [
        "MATCH (parent:PetStore:Pets)-[r:HAS_INQUIRY]->(n:PetStore:Inquiries)",
        "WHERE id(n) IN [3]",
        "RETURN n, r, parent",
      ].join("\n"),
    );
  });

  it("asks nothing when no child is within the selected domains", () => {
    expect(
      neighboursQuery(INQUIRIES, 17, "children", [RELS[4], RELS[5]], new Set(), SELECTED),
    ).toBeNull();
  });
});

describe("REQ-2135 the queries tests/unit/test_cypher_drop_shapes.py runs", () => {
  const PERSON_COMPANY: PathScope = {
    nodeLabels: ["Person", "Company"].map((tableLabel) => ({
      domainLabel: null,
      domainId: "hr",
      tableLabel,
      systemDomain: false,
    })),
    checkedDomains: new Set(),
  };
  const HR = [
    rel("KNOWS", "Person", "Person"),
    rel("WORKS_AT", "Person", "Company"),
    rel("PARENT", "Company", "Company"),
  ];

  it("labels ()-[r]->() and joins Company in every branch", () => {
    expect(expand("MATCH ()-[r]->() RETURN r LIMIT 25", "Company", HR, PERSON_COMPANY).query).toBe(
      [
        "MATCH (n1:Person)-[r:KNOWS]->(n2:Person)",
        "OPTIONAL MATCH (n1)-[rCompany:WORKS_AT]-(mCompany:Company)",
        "RETURN r, n1, n2, rCompany, mCompany",
        "LIMIT 25",
        "UNION ALL",
        "MATCH (n1:Person)-[r:WORKS_AT]->(n2:Company)",
        "OPTIONAL MATCH (n1)-[rCompany:WORKS_AT]-(mCompany:Company)",
        "RETURN r, n1, n2, rCompany, mCompany",
        "LIMIT 25",
        "UNION ALL",
        "MATCH (n1:Company)-[r:PARENT]->(n2:Company)",
        "OPTIONAL MATCH (n1)-[rCompany:PARENT]-(mCompany:Company)",
        "RETURN r, n1, n2, rCompany, mCompany",
        "LIMIT 25",
      ].join("\n"),
    );
  });

  it("asks a person's children", () => {
    expect(neighboursQuery("Person", 1, "children", HR, new Set(), PERSON_COMPANY)).toBe(
      [
        "MATCH (n:Person)-[r:KNOWS]->(child:Person)",
        "WHERE id(n) IN [1]",
        "RETURN n, r, child",
        "UNION ALL",
        "MATCH (n:Person)-[r:WORKS_AT]->(child:Company)",
        "WHERE id(n) IN [1]",
        "RETURN n, r, child",
      ].join("\n"),
    );
  });
});
