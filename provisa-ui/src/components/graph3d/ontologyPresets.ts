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
// presets of the one graph, each named by a focus and a depth. (Amended 2026-11-05, ONE GRAPH,
// RULES PER PRESET:) each carries its rules: the Ontology page's and a term's page's draw only what
// a shown term reaches (REQ-2130); the Product map's and a product's panel's keep the product
// layers and the member tables.

import { NEIGHBOURHOOD } from "./neighbourhood";
import { productNode, termNode } from "./ontologyData";
import {
  ONTOLOGY_SCOPE,
  PRODUCT_MAP_SCOPE,
  type GraphScope,
  type LayerId,
} from "./graphModel";

/** A snippet of the ontology: the term or product it is focused on (its node id) and how many
 * hops around it are drawn; a null focus is the whole ontology. ``products``: read and drawn by
 * the Product map's rules (QueryScope.products); false, the Ontology preset's. */
export interface OntologyPreset {
  focus: string | null;
  depth: number;
  products: boolean;
}

/** A snippet's preset: it has a focus. */
export type SnippetPreset = OntologyPreset & { focus: string };

/** The whole ontology, as the Ontology page shows it. */
export const WHOLE: OntologyPreset = { focus: null, depth: NEIGHBOURHOOD.depth, products: false };

/** The whole ontology with every product among it, as the Product map shows it. */
export const PRODUCT_MAP: OntologyPreset = { ...WHOLE, products: true };

/** A term's page: the ontology focused on the term -- its ancestors and descendants, and the
 * columns, tables and products it reaches. */
export const termPreset = (term: number): SnippetPreset => ({
  focus: termNode(term),
  depth: NEIGHBOURHOOD.depth,
  products: false,
});

/** REQ-2174 (amended 2026-11-07): a product's panel opens at its tables, matching its Related
 * Tables card: one hop, with Related tables on (snippetLayers). */
export const PRODUCT_PANEL_DEPTH = 1;

/** A product's panel: the ontology focused on the product, by the Product map's rules. */
export const productPreset = (product: string): SnippetPreset => ({
  focus: productNode(product),
  depth: PRODUCT_PANEL_DEPTH,
  products: true,
});

/** The graph scope a preset's rules draw with. */
export const scopeOf = (p: Pick<OntologyPreset, "products">): GraphScope =>
  p.products ? PRODUCT_MAP_SCOPE : ONTOLOGY_SCOPE;

/** A snippet's layers: the terms, the columns they bind, those columns' tables and the products
 * holding them; under the Product map's rules the tables related to the tables drawn too
 * (Related tables, on in a product's panel). */
export const snippetLayers = (p: Pick<OntologyPreset, "products">): LayerId[] =>
  p.products
    ? ["ontology", "columns", "ownerTables", "data", "related"]
    : ["ontology", "columns", "data"];
