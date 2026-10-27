// Copyright (c) 2026 Kenneth Stott
// Canary: 32859861-8f5d-4867-95a4-7a9c29b5e92d
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// Mirror of provisa.compiler.naming.domain_alias_map (REQ-471): two domains whose computed aliases
// would be the same are each named by their own name, so neither's root fields are grouped as the
// other's.

import { describe, expect, it } from "vitest";
import { domainAliasMap } from "../admin";

describe("domainAliasMap", () => {
  it("names apart two domains whose computed aliases are the same", () => {
    expect(
      domainAliasMap([
        { id: "ontology", graphqlAlias: null },
        { id: "ops", graphqlAlias: null },
        { id: "sales_analytics", graphqlAlias: null },
        { id: "hr", graphqlAlias: "HR" },
      ]),
    ).toEqual({ ontology: "ontology", ops: "ops", sales_analytics: "sa", hr: "hr" });
  });

  it("keeps a stored alias and the computed one it does not meet", () => {
    expect(
      domainAliasMap([
        { id: "ontology", graphqlAlias: "ont" },
        { id: "ops", graphqlAlias: null },
      ]),
    ).toEqual({ ontology: "ont", ops: "o" });
  });
});
