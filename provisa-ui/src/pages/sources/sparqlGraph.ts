// Copyright (c) 2026 Kenneth Stott
// Canary: 3a0bce79-0241-4a9f-813e-f94fc6018adb
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1740: a SPARQL source may restrict its queries to one named graph
// (provisa/sparql/source.py's SparqlSourceConfig.default_graph_uri), saved as its federation
// hints. It is orthogonal to the credential the source signs in with (./sparqlReasoner).

import { hintKey } from "./descriptor";

// REQ-2114: the hint key is the descriptor's (provisa/api/admin/source_types.py).
const DEFAULT_GRAPH_URI = hintKey("sparql", "default_graph_uri");

/** The hints a form saves: the named graph, when one is given. */
export function sparqlHints(fields: Record<string, string>): Record<string, string> {
  const graph = fields[DEFAULT_GRAPH_URI]?.trim();
  return graph ? { [DEFAULT_GRAPH_URI]: graph } : {};
}

/** A saved source's hints into the form's fields. */
export function sparqlGraphFields(hints: Record<string, string> | null): Record<string, string> {
  const graph = hints?.[DEFAULT_GRAPH_URI];
  return graph ? { [DEFAULT_GRAPH_URI]: graph } : {};
}
