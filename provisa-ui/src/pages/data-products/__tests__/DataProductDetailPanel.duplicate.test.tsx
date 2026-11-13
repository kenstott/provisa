// Copyright (c) 2026 Kenneth Stott
// Canary: 33232f10-20a0-4b40-a592-e52a69756f50
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// Duplicate, in a data product's panel head, beside Edit and Delete: offered to a caller who may
// create products (the page hands onDuplicate to one), and not to one who may not.

import { describe, it, expect, vi } from "vitest";
import userEvent from "@testing-library/user-event";
import { render, screen } from "../../../test-utils/render";
import { DataProductDetailPanel } from "../DataProductDetailPanel";
import type { DataProduct } from "../../../types/admin";
import { lifecycleProps } from "./lifecycleProps";

vi.mock("../../../components/OwnerResolution", () => ({ OwnerResolutionInline: () => null }));
vi.mock("../../../components/graph3d/OntologyGraph", () => ({
  OntologySnippet: () => <div data-testid="graph-panel" />,
}));

const PRODUCT = {
  id: "pet_health",
  domainId: "pet-store",
  name: "Pet Health",
  customProperties: {},
} as unknown as DataProduct;

function panel(over: { canEdit: boolean; onDuplicate?: () => void }) {
  return render(
    <DataProductDetailPanel
      p={PRODUCT}
      core={{ product: PRODUCT, queries: { graphql: "query X { x }", sql: "", jsonapi: "" } }}
      coreError={null}
      reader={null}
      canEdit={over.canEdit}
      canDelete={over.canEdit}
      owner={false}
      seesConsumers={false}
      lineageLoading={false}
      lineageError=""
      lineageGraph={null}
      onEdit={vi.fn()}
      onDuplicate={over.onDuplicate}
      onDelete={vi.fn()}
      {...lifecycleProps()}
    />,
  );
}

describe("Duplicate in a data product's head", () => {
  it("is named Duplicate and opens the create form through onDuplicate", async () => {
    const onDuplicate = vi.fn();
    panel({ canEdit: true, onDuplicate });
    const action = screen.getByTestId("data-product-detail-duplicate-pet_health");
    expect(action).toHaveAccessibleName("Duplicate");
    await userEvent.click(action);
    expect(onDuplicate).toHaveBeenCalledTimes(1);
  });

  it("is beside Edit", () => {
    panel({ canEdit: true, onDuplicate: vi.fn() });
    const edit = screen.getByTestId("data-product-detail-edit-pet_health");
    const dup = screen.getByTestId("data-product-detail-duplicate-pet_health");
    expect(edit.parentElement?.parentElement).toBe(dup.parentElement?.parentElement);
  });

  it("is not offered when the page hands no onDuplicate (a caller who may not create)", () => {
    panel({ canEdit: true });
    expect(screen.queryByTestId("data-product-detail-duplicate-pet_health")).toBeNull();
  });
});
