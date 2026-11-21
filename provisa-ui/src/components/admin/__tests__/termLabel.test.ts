// Copyright (c) 2026 Kenneth Stott
// Canary: 4e8a1c6d-3b27-4f90-a5d1-9c2e7b0f6a13
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1591: a term's name is unique only within a domain; listed together, two terms of one name
// are told apart by their domains, and a name no other term holds is shown alone.

import { describe, expect, it } from "vitest";
import { pipedDomains, termLabel, termOptions } from "../termLabel";

const sales = { id: 1, name: "client", domains: ["sales"] };
const pets = { id: 2, name: "client", domains: ["pet-store", "vet"] };
const party = { id: 3, name: "party", domains: [] };
const unscoped = { id: 4, name: "client", domains: [] };

describe("termLabel", () => {
  it("shows a name no other listed term holds alone", () => {
    expect(termLabel(party, [sales, pets, party])).toBe("party");
    expect(termLabel(sales, [sales, party])).toBe("client");
  });

  it("tells same-named terms apart by their domains", () => {
    const all = [sales, pets, party];
    expect(termLabel(sales, all)).toBe("client (sales)");
    expect(termLabel(pets, all)).toBe("client (pet-store, vet)");
  });

  it("marks a same-named term with no domain", () => {
    expect(termLabel(unscoped, [sales, unscoped])).toBe("client (—)");
  });
});

describe("termOptions", () => {
  it("labels each option among the shown terms and keeps its plain name and domains", () => {
    expect(termOptions([sales, pets, party])).toEqual([
      { value: "1", label: "client (sales)", name: "client", domains: ["sales"] },
      {
        value: "2",
        label: "client (pet-store, vet)",
        name: "client",
        domains: ["pet-store", "vet"],
      },
      { value: "3", label: "party", name: "party", domains: [] },
    ]);
  });
});

describe("pipedDomains", () => {
  it("reads the ontology schema's |a|b| form", () => {
    expect(pipedDomains("|sales|pet-store|")).toEqual(["sales", "pet-store"]);
    expect(pipedDomains(null)).toEqual([]);
  });
});
