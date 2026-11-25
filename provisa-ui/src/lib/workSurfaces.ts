// Copyright (c) 2026 Kenneth Stott
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2277: the work surfaces, each by the name the requirement gives it, with the components (paths
// under src/) that draw it. A work surface lays itself out for its purpose; every other page is an
// object management page and is held to the standard page anatomy (pageHeaderGuard). The names are
// exactly the requirement's list (workSurfaces.test.ts); adding a page here is an amendment of
// REQ-2277.

export const WORK_SURFACES: Readonly<Record<string, readonly string[]>> = {
  Explore: ["pages/McpExplorePage.tsx"],
  // The Ontology page (its term list, Ontology View and imports) and the lineage graph.
  "the ontology and graph views": ["components/admin/GlossaryTab.tsx", "pages/LineagePage.tsx"],
  "data products": ["pages/DataProductsPage.tsx"],
  "dashboard-like pages": ["pages/context/ContextHealthPage.tsx"],
  "the query editors": [
    "pages/QueryPage.tsx",
    "pages/GraphPage.tsx",
    "pages/SqlPage.tsx",
    "pages/NlPage.tsx",
    "pages/GrpcPage.tsx",
    "pages/JsonApiPage.tsx",
    "pages/ODataPage.tsx",
    "pages/OpenApiPage.tsx",
  ],
};

/** Every component file that draws a work surface, under src/. */
export const WORK_SURFACE_FILES: ReadonlySet<string> = new Set(Object.values(WORK_SURFACES).flat());
