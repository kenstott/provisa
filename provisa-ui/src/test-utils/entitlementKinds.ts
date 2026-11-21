// Copyright (c) 2026 Kenneth Stott
// Canary: 6e2b9d14-83a7-4c0f-b5e1-9a7d3c2f8e40
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2219: what GET /admin/entitlements/kinds answers, for the UI's tests: the server's kinds in
// its order (KIND_ORDER in provisa/entitlements/index.py) and a small names tree.

import type { EntitlementKinds } from "../api/entitlements";

export const SERVER_KINDS: EntitlementKinds = {
  kinds: [
    { kind: "domains", semantic: true, tree_depth: 1 },
    { kind: "tables", semantic: true, tree_depth: 2 },
    { kind: "columns", semantic: true, tree_depth: 3 },
    { kind: "metrics", semantic: true, tree_depth: null },
    { kind: "relationships", semantic: true, tree_depth: null },
    { kind: "commands", semantic: true, tree_depth: null },
    { kind: "data_products", semantic: true, tree_depth: null },
    { kind: "sources", semantic: false, tree_depth: null },
    { kind: "environments", semantic: false, tree_depth: null },
    { kind: "roles", semantic: false, tree_depth: null },
    { kind: "regions", semantic: false, tree_depth: null },
  ],
  names: [
    {
      id: "finance",
      label: "finance",
      children: [
        { id: "finance.costs", label: "costs", children: [] },
        { id: "finance.revenue", label: "revenue", children: [] },
      ],
    },
  ],
};
