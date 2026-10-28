// Copyright (c) 2026 Kenneth Stott
// Canary: 6f1d3b82-9a47-4c05-b8e2-5d7a0c9e4f13
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2174 (amended 2026-10-28, ONE ONTOLOGY GRAPH; EVERY OTHER GRAPH IS A SNIPPET OF IT): the
// presets of the one graph, each named by a focus and a depth.

import { NEIGHBOURHOOD } from "./neighbourhood";
import { productNode, termNode } from "./ontologyData";

/** A snippet of the ontology: the term or product it is focused on (its node id) and how many
 * hops around it are drawn; a null focus is the whole ontology. */
export interface OntologyPreset {
  focus: string | null;
  depth: number;
}

/** A snippet's preset: it has a focus. */
export type SnippetPreset = OntologyPreset & { focus: string };

/** The whole ontology, as the Ontology page shows it. */
export const WHOLE: OntologyPreset = { focus: null, depth: NEIGHBOURHOOD.depth };

/** A term's page: the ontology focused on the term -- its ancestors and descendants, and the
 * columns, tables and products it reaches. */
export const termPreset = (term: number): SnippetPreset => ({
  focus: termNode(term),
  depth: NEIGHBOURHOOD.depth,
});

/** A product's panel: the ontology focused on the product. */
export const productPreset = (product: string): SnippetPreset => ({
  focus: productNode(product),
  depth: NEIGHBOURHOOD.depth,
});
