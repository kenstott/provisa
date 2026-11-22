// Copyright (c) 2026 Kenneth Stott
// Canary: 4c1f7a92-3e8b-4d26-9a51-7b0e2c6d8f13
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// A table's Associations section lists each glossary term naming its columns as a link to that
// term on the Ontology page, as its relationships are links to theirs.

import { describe, expect, it, vi } from "vitest";
import { render, screen } from "../../../test-utils/render";

vi.mock("../../../api/glossary", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../../api/glossary")>()),
  fetchRelatedGlossaryTerms: vi.fn().mockResolvedValue([
    { id: 7, name: "care level", definition: null, is_abstract: false, status: "approved", grounding: "grounded", domains: [] },
    { id: 9, name: "average lifespan years", definition: null, is_abstract: false, status: "approved", grounding: "grounded", domains: [] },
    { id: 11, name: "animal", definition: null, is_abstract: true, status: "approved", grounding: "grounded", domains: [] },
  ]),
}));
vi.mock("../../../hooks/useAdminQueries", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../../hooks/useAdminQueries")>()),
  useRelationships: () => ({ relationships: [], loading: false, refetch: vi.fn() }),
  useTableMetrics: () => ({ metrics: [], loading: false, error: undefined }),
}));
vi.mock("../../../components/TagControl", () => ({ TagControl: () => null }));
vi.mock("../TableClassTerm", () => ({ TableClassTerm: () => null }));

import { TableAssociations } from "../TableAssociations";

describe("TableAssociations glossary terms", () => {
  it("links each column term to its term on the Ontology page", async () => {
    render(<TableAssociations table={{ id: 1 }} relationships={[]} />);
    const care = await screen.findByTestId("table-association-term-7");
    expect(care).toHaveTextContent("care level");
    expect(care).toHaveAttribute("href", "/context/ontology?term=7");
    expect(screen.getByTestId("table-association-term-9")).toHaveAttribute(
      "href",
      "/context/ontology?term=9",
    );
    expect(screen.queryByTestId("table-association-term-11")).toBeNull();
  });
});
