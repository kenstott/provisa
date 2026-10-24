// Copyright (c) 2026 Kenneth Stott
// Canary: ebbdcd6e-ff5a-468a-9589-f5d2df74f249
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// A data product's panel shows every value of the product under its form's labels, its ID among
// them, an empty one as "—".

import { describe, it, expect, vi } from "vitest";
import { render, screen, within } from "../../../test-utils/render";
import { DataProductDetailPanel } from "../DataProductDetailPanel";
import type { DataProduct } from "../../../types/admin";

// Who an owner role resolves to is its own fetch, held by its own tests.
vi.mock("../../../components/OwnerResolution", () => ({ OwnerResolutionInline: () => null }));
// The panel's 3D graph is its own component, held by its own tests.
vi.mock("../../../components/graph3d/KnowledgeGraphPanel", () => ({
  KnowledgeGraphPanel: () => null,
}));

const PRODUCT = {
  id: "pet_health",
  domainId: "pet-store",
  name: "Pet Health",
  version: "1.2.0",
  status: "published",
  ownerRole: "vet_lead",
  teamRole: null,
  support: "#pet-health",
  sla: "99.5%",
  purpose: "Health records of the pets in care",
  limitations: null,
  usage: "Join on pet_id",
} as unknown as DataProduct;

function fields(p: DataProduct): [string, string][] {
  render(
    <DataProductDetailPanel
      p={p}
      core={{ product: p, queries: { graphql: "", sql: "", jsonapi: "" } }}
      coreError={null}
      reader={null}
      sources={[]}
      internalRelationships={[]}
      canEdit={false}
      canSeeLineage={false}
      lineageLoading={false}
      lineageError=""
      lineageGraph={null}
      inputPorts={[]}
      onEdit={vi.fn()}
      onDelete={vi.fn()}
    />,
  );
  return within(screen.getByTestId(`data-product-read-${p.id}`))
    .getAllByRole("term")
    .map((dt) => [dt.textContent ?? "", dt.nextElementSibling?.textContent ?? ""]);
}

describe("a data product's read view", () => {
  it("shows every value of the product, under the form's labels", () => {
    expect(fields(PRODUCT)).toEqual([
      ["ID", "pet_health"],
      ["Name", "Pet Health"],
      ["Domain", "pet-store"],
      ["Version", "1.2.0"],
      ["Status", "published"],
      ["Owner", "vet_lead"],
      ["Team", "—"],
      ["Support", "#pet-health"],
      ["SLA", "99.5%"],
      ["Purpose", "Health records of the pets in care"],
      ["Limitations", "—"],
      ["Usage", "Join on pet_id"],
    ]);
  });
});
