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
// layers and the member tables. (Amended 2026-11-11, A TERM OPENS ON ITSELF:) an expanded snippet
// opens on its own focus -- each at the depth a product's graph opens at -- and only the
// Ontology page's graph opens whole. (Amended 2026-11-12, A HOP IS A STEP BETWEEN TERMS:) a hop is
// a step between terms; a term's grounding and a product's own tables, columns and terms come at no
// hop, so every snippet and expanded graph opens at 1.

import { NEIGHBOURHOOD } from "./neighbourhood";
import { productNode, termNode } from "./ontologyData";
import { ONTOLOGY_SCOPE, PRODUCT_MAP_SCOPE, type GraphScope, type LayerId } from "./graphModel";

/** A snippet of the ontology: the term or product it is focused on (its node id) and how many
 * hops around it are drawn; a null focus is the whole ontology. ``products``: read and drawn by
 * the Product map's rules (QueryScope.products); false, the Ontology preset's. */
export interface OntologyPreset {
  focus: string | null;
  depth: number;
  products: boolean;
  /** Drawn in 2D until the reader chooses otherwise. */
  flat?: boolean;
}

/** A snippet's preset: it has a focus. */
export type SnippetPreset = OntologyPreset & { focus: string };

/** The whole ontology, as the Ontology page shows it. */
export const WHOLE: OntologyPreset = { focus: null, depth: NEIGHBOURHOOD.depth, products: false };

/** The whole ontology with every product among it, as the Product map shows it. */
export const PRODUCT_MAP: OntologyPreset = { ...WHOLE, products: true };

/** A term's graph opens at what the term touches, its hops raised from there. */
export const TERM_GRAPH_DEPTH = 1;

/** A term's page: the ontology focused on the term -- the terms a step from it, and the columns,
 * tables and products each is grounded on -- in 2D, at one hop. */
export const termPreset = (term: number): SnippetPreset => ({
  focus: termNode(term),
  depth: TERM_GRAPH_DEPTH,
  products: false,
  flat: true,
});

/** REQ-2174 (amended 2026-11-07; 2026-11-12): a product's panel opens at one hop -- its tables,
 * their columns and terms, and the terms a step from those -- with Related tables on
 * (snippetLayers). */
export const PRODUCT_PANEL_DEPTH = 1;

/** A product's panel: the ontology focused on the product, by the Product map's rules. */
export const productPreset = (product: string): SnippetPreset => ({
  focus: productNode(product),
  depth: PRODUCT_PANEL_DEPTH,
  products: true,
});

/** A product's graph, expanded from its panel, opens at what the product delivers -- its tables,
 * their columns and the terms grounded on those columns, at no hop -- and a step past them. */
export const PRODUCT_GRAPH_DEPTH = 1;

/** The expanded term graph's preset, for a term's page preset: the term, the terms a step from it,
 * and the columns, tables and products each is grounded on, at the depth a product's graph opens
 * at -- a term opens as a product does. */
export const termGraph = (p: SnippetPreset): SnippetPreset => ({
  ...p,
  depth: PRODUCT_GRAPH_DEPTH,
});

/** The expanded product graph's preset, for a product's panel preset. */
export const productGraph = (p: SnippetPreset): SnippetPreset => ({
  ...p,
  depth: PRODUCT_GRAPH_DEPTH,
});

/** Its layers: the terms, the columns they bind and the product's tables, no domain and no
 * related table; the Layers menu adds them. */
export const PRODUCT_GRAPH_LAYERS: LayerId[] = ["ontology", "columns", "data"];

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
