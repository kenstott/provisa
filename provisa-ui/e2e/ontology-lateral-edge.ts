// Copyright (c) 2026 Kenneth Stott
// Canary: f2a96122-4158-4f1e-8d3d-7760e004028d
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import type { APIRequestContext } from "@playwright/test";

/**
 * A term edge of a type the Ontology View's Taxonomy preset does not keep, for the specs that
 * prove the preset narrows the view (REQ-2130) and that its query says so (REQ-2136).
 *
 * The e2e config's terms are related by PART_OF alone since REQ-1387 retyped clinic and specialty
 * from RELATED_TO to PART_OF of referral. With no other type in the graph, Taxonomy's two types
 * (KIND_OF, PART_OF) are every type there is, and the view rightly reads that as no narrowing:
 * "All edge types", and no relType in its query. So a spec about the narrowing draws one edge of a
 * third type itself, between two of the config's terms, and takes it away after.
 */
export const LATERAL_EDGE = { from: "clinic", to: "specialty", relType: "RELATED_TO" } as const;

async function termId(request: APIRequestContext, name: string): Promise<number> {
  const res = await request.get(`/admin/glossary/terms?q=${encodeURIComponent(name)}`);
  if (!res.ok())
    throw new Error(`listing glossary terms "${name}": ${res.status()} ${await res.text()}`);
  const terms = (await res.json()) as { id: number; name: string }[];
  const term = terms.find((t) => t.name === name);
  if (!term)
    throw new Error(`no glossary term "${name}" among ${JSON.stringify(terms.map((t) => t.name))}`);
  return term.id;
}

/** Draws LATERAL_EDGE; returns what takes it away again. */
export async function addLateralEdge(request: APIRequestContext): Promise<() => Promise<void>> {
  const from = await termId(request, LATERAL_EDGE.from);
  const to = await termId(request, LATERAL_EDGE.to);
  const added = await request.post(`/admin/glossary/terms/${from}/edges`, {
    data: { to_term_id: to, rel_type: LATERAL_EDGE.relType },
  });
  if (!added.ok())
    throw new Error(
      `adding ${JSON.stringify(LATERAL_EDGE)}: ${added.status()} ${await added.text()}`,
    );
  return async () => {
    const params = new URLSearchParams({ to_term_id: String(to), rel_type: LATERAL_EDGE.relType });
    const removed = await request.delete(
      `/admin/glossary/terms/${from}/edges?${params.toString()}`,
    );
    if (!removed.ok())
      throw new Error(
        `removing ${JSON.stringify(LATERAL_EDGE)}: ${removed.status()} ${await removed.text()}`,
      );
  };
}
