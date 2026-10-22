// Copyright (c) 2026 Kenneth Stott
// Canary: e4d9f93d-3afe-40fe-8914-1e201dda7f7d
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2138: a sparql source's reasoner and its OWL axioms travel in the source's mapping
// (provisa/sparql/reasoning.py: "reasoner", "axioms"); the form holds them in its fields.

/** The reasoners a sparql source may name. */
export const SPARQL_REASONERS = ["hermit"] as const;

export function sparqlMappingJson(fields: Record<string, string>): string {
  const reasoner = fields.reasoner?.trim();
  if (!reasoner) return "{}";
  return JSON.stringify({ reasoner, axioms: fields.axioms ?? "" });
}

export function sparqlFieldsFromMapping(mappingJson: string): Record<string, string> {
  const mapping = JSON.parse(mappingJson) as Record<string, unknown>;
  if (typeof mapping.reasoner !== "string") return {};
  return {
    reasoner: mapping.reasoner,
    axioms: typeof mapping.axioms === "string" ? mapping.axioms : "",
  };
}
