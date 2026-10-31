// Copyright (c) 2026 Kenneth Stott
// Canary: 6f2a9d14-b3e7-4c58-8a01-d7c4e6b19f32
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useEffect } from "react";
import type { GNode, SparqlDescribed } from "../graph-model";

/** A class captioned or sized by a property: a Cypher node holds all its properties, a SPARQL node
 * only what its query asked, so the resources that lack the property are read from the model, once
 * each (``described`` holds what was answered, a refusal or nothing included). */
export function useSparqlStyledProperties(
  frameId: string,
  described: Record<string, SparqlDescribed> | undefined,
  nodes: Map<string, GNode>,
  labelProperty: Record<string, string>,
  sizeByProperty: Record<string, string>,
  onDescribe: ((frameId: string, iris: string[]) => void) | undefined,
): void {
  useEffect(() => {
    if (!onDescribe) return;
    const missing: string[] = [];
    nodes.forEach((n) => {
      const { iri } = n.properties;
      if (typeof iri !== "string" || (described && iri in described)) return;
      const wanted = [labelProperty[n.label], sizeByProperty[n.label]];
      if (wanted.some((k) => k && !(k in n.properties))) missing.push(iri);
    });
    if (missing.length > 0) onDescribe(frameId, missing);
  }, [frameId, described, nodes, labelProperty, sizeByProperty, onDescribe]);
}
