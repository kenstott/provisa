// Copyright (c) 2026 Kenneth Stott
// Canary: 2e7c9a15-48b3-4d6f-9c21-a6f0e3b8d472
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2252: what the acting role is served, read once per role for the page's life (it changes only
// with the model): its GraphQL schema and root fields, its graph schema, and its SPARQL vocabulary.
// Each is read on its own; a refusal leaves that one unread, and the surfaces it names no link.

import { useEffect, useState } from "react";
import { metaReader } from "../../api/metaGraph";
import type { SparqlVocabulary } from "../graph/sparql";
import type { GraphSchemaDoc, RelationshipSurfaces } from "./relationshipQueries";

async function read<T>(url: string, role: string): Promise<T> {
  const resp = await fetch(url, { headers: { "X-Provisa-Role": role } });
  if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
  return (await resp.json()) as T;
}

const graphs = new Map<string, Promise<GraphSchemaDoc>>();
const vocabularies = new Map<string, Promise<SparqlVocabulary>>();

function once<T>(cache: Map<string, Promise<T>>, role: string, load: () => Promise<T>): Promise<T> {
  let p = cache.get(role);
  if (!p) {
    p = load();
    p.catch(() => cache.delete(role));
    cache.set(role, p);
  }
  return p;
}

const NONE: RelationshipSurfaces = { reader: null, graph: null, vocabulary: null };

/** ``role``'s surfaces; each null until read, and while refused. */
export function useRelationshipSurfaces(role: string | null): RelationshipSurfaces {
  const [surfaces, setSurfaces] = useState<{ role: string; value: RelationshipSurfaces } | null>(
    null,
  );
  useEffect(() => {
    if (!role) return;
    let cancelled = false;
    const set = (part: Partial<RelationshipSurfaces>) =>
      !cancelled &&
      setSurfaces((prev) => ({
        role,
        value: { ...(prev?.role === role ? prev.value : NONE), ...part },
      }));
    const refused = () => undefined;
    metaReader(role).then((reader) => set({ reader }), refused);
    once(graphs, role, () => read<GraphSchemaDoc>("/data/graph-schema", role)).then(
      (graph) => set({ graph }),
      refused,
    );
    once(vocabularies, role, () => read<SparqlVocabulary>("/data/sparql/vocabulary", role)).then(
      (vocabulary) => set({ vocabulary }),
      refused,
    );
    return () => {
      cancelled = true;
    };
  }, [role]);
  return role && surfaces?.role === role ? surfaces.value : NONE;
}
