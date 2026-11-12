// Copyright (c) 2026 Kenneth Stott
// Canary: 0d7e3b51-9a2c-4f68-b1e4-63c8f0a29d75
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2237: an ontology import as the API answers for it, shared by the import, review and apply
// tests.

import type { Alignment, ImportItem, ImportReview, OntologyImport } from "../api/ontologyImport";

export const IMPORT: OntologyImport = {
  id: 7,
  title: "FIBO Accounts",
  file_name: "Accounts.ttl",
  format: "turtle",
  domains: ["sales"],
  summary: { items: 4, statements: 3, left_out: { restrictions: 2, "imported modules": 1 } },
  imported_by: "ada",
  imported_at: "2026-11-11T09:00:00Z",
  applied_by: null,
  applied_at: null,
  counts: {
    items: { pending: 3, excluded: 1 },
    alignments: { proposed: 3, approved: 1 },
  },
};

const term = (term_id: number, name: string, role: "class" | "property" | "concept") => ({
  term_id,
  name,
  definition: null,
  role,
  status: role === "class" ? ("approved" as const) : ("draft" as const),
  grounding: role === "class" ? ("grounded" as const) : ("not_grounded" as const),
  iri: null,
});

const alignment = (a: Partial<Alignment> & Pick<Alignment, "id" | "item_id">): Alignment => ({
  rel_type: "SAME_AS",
  item_is_source: true,
  basis: "name",
  status: "proposed",
  use_definition: false,
  decided_by: null,
  decided_at: null,
  term: null,
  other_item: null,
  ...a,
});

export const ACCOUNT: ImportItem = {
  id: 1,
  name: "account",
  kind: "class",
  definition: "A contractual arrangement to hold a balance.",
  iri: "https://spec.edmcouncil.org/fibo/ontology/FBC/Account",
  keys: [],
  labels: ["Account"],
  status: "pending",
  term: null,
  alignments: [
    alignment({ id: 10, item_id: 1, term: term(5, "account", "class") }),
    alignment({
      id: 11,
      item_id: 1,
      rel_type: "KIND_OF",
      basis: "ontology",
      other_item: { item_id: 2, name: "agreement" },
    }),
  ],
};

export const AGREEMENT: ImportItem = {
  id: 2,
  name: "agreement",
  kind: "class",
  definition: null,
  iri: "https://spec.edmcouncil.org/fibo/ontology/FND/Agreement",
  keys: [],
  labels: [],
  status: "pending",
  term: null,
  alignments: [],
};

export const HOLDER: ImportItem = {
  id: 3,
  name: "account holder",
  kind: "property",
  definition: "The party that holds the account.",
  iri: null,
  keys: [],
  labels: [],
  status: "pending",
  term: null,
  alignments: [
    alignment({
      id: 12,
      item_id: 3,
      rel_type: "PART_OF",
      item_is_source: false,
      basis: "qualifier",
      term: term(6, "account holder name", "property"),
    }),
  ],
};

export const CLOSED: ImportItem = {
  id: 4,
  name: "closed",
  kind: "value",
  definition: null,
  iri: null,
  keys: [],
  labels: [],
  status: "excluded",
  term: null,
  alignments: [
    alignment({
      id: 13,
      item_id: 4,
      rel_type: "VALID_VALUE_OF",
      basis: "ontology",
      status: "approved",
      other_item: { item_id: 1, name: "account" },
    }),
  ],
};

export const REVIEW: ImportReview = {
  import: IMPORT,
  items: [ACCOUNT, AGREEMENT, HOLDER, CLOSED],
};
