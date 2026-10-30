// Copyright (c) 2026 Kenneth Stott
// Canary: 5d0f7c1e-2b3a-4c8e-9a41-7e6d2f0b8c13
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2130: the ERD opened on one table (`?erd=<table id>`): it is centred and selected.

import type { CyInstance } from "../graph/cytoscape-types";

/** Select the table's node and centre the diagram on it; false when the diagram has no such node. */
export function focusErdTable(cy: CyInstance, tableId: string): boolean {
  const node = cy.getElementById(`t:${tableId}`);
  if (node.empty()) return false;
  cy.$(":selected").unselect();
  node.select();
  cy.center(node);
  return true;
}
