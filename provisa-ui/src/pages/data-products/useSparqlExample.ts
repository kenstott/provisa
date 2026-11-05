// Copyright (c) 2026 Kenneth Stott
// Canary: 9d2f6a18-c07e-4b35-8e41-1f7ba3c6d092
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2158: a table's example on the SPARQL page: the query its class's pill runs there. The class
// is the role's own: the graph schema names the table's node label and the vocabulary the class that
// types it. A table with no class, or one read by arguments, has no example.

import { useEffect, useState } from "react";
import { pillQuery, type SparqlVocabulary } from "../../components/graph/sparql";
import type { ExampleLink } from "./tableExamples";

/** The role's documents an example is read from; a refusal leaves the table with no example. */
async function read<T>(url: string, role: string): Promise<T> {
  const resp = await fetch(url, { headers: { "X-Provisa-Role": role } });
  if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
  return (await resp.json()) as T;
}

interface GraphSchema {
  node_labels?: { label: string; table_id: number }[];
}

export function sparqlQueryOf(
  tableId: number,
  schema: GraphSchema,
  vocabulary: SparqlVocabulary,
): string | null {
  const label = schema.node_labels?.find((n) => Number(n.table_id) === tableId)?.label;
  const cls = vocabulary.classes.find(
    (c) =>
      c.kind === "table" && !c.parameterized && label !== undefined && c.labels.includes(label),
  );
  return cls ? pillQuery({ kind: "class", iri: cls.iri }, vocabulary) : null;
}

/** The SPARQL example of table ``tableId`` for ``role``; null until read, and while not ``wanted``. */
export function useSparqlExample(
  tableId: number,
  role: string | null,
  wanted: boolean,
): ExampleLink | null {
  const [query, setQuery] = useState<string | null>(null);
  useEffect(() => {
    if (!wanted || !role) return;
    let cancelled = false;
    Promise.all([
      read<GraphSchema>("/data/graph-schema", role),
      read<SparqlVocabulary>("/data/sparql/vocabulary", role),
    ]).then(
      ([schema, vocabulary]) => !cancelled && setQuery(sparqlQueryOf(tableId, schema, vocabulary)),
      () => !cancelled && setQuery(null),
    );
    return () => {
      cancelled = true;
    };
  }, [tableId, role, wanted]);
  return wanted && query ? { to: "/sparql", state: { query, autoRun: true } } : null;
}
