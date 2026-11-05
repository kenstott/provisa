// Copyright (c) 2026 Kenneth Stott
// Canary: 292b822a-31a7-493d-806d-15f2e6d69b56
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// Applying an imported OSSIE model registers each item through the existing mutations. One that
// raises (the server's GraphQL error) is that item's failure, named on its row; the items after it
// are still applied, and the apply button is free again.

import { describe, it, expect, vi } from "vitest";
import { fireEvent, screen, waitFor } from "@testing-library/react";
import { render } from "../../../test-utils/render";
import type { OssieImportProposals } from "../../../api/admin";

const registerTable = vi.fn();
vi.mock("../../../hooks/useAdminQueries", () => ({
  useRegisterTable: () => ({ registerTable }),
  useUpsertRelationship: () => ({ upsertRelationship: vi.fn() }),
  useUpsertMetric: () => ({ upsertMetric: vi.fn() }),
}));
vi.mock("../../../context/AuthContext", () => ({ useAuth: () => ({ actingRoles: "org_admin" }) }));

const table = (name: string) => ({
  name,
  table_name: name,
  schema_name: "main",
  source_id: "shop-db",
  description: null,
  columns: [],
  not_importable: [],
  primary_key: [],
  unique_keys: [],
});
const PROPOSALS: OssieImportProposals = {
  model_name: "shop",
  tables: [table("orders"), table("customers")],
  relationships: [],
  metrics: [],
} as unknown as OssieImportProposals;

vi.mock("../../../api/admin", async (orig) => ({
  ...(await orig<typeof import("../../../api/admin")>()),
  importOssie: vi.fn(async () => PROPOSALS),
}));

import { OssieInterchangePanel } from "../OssieInterchangePanel";

describe("applying an imported model", () => {
  it("names an item that raised, and applies the rest", async () => {
    registerTable
      .mockImplementationOnce(async () => {
        throw new Error("Table name 'orders' is ambiguous across domains");
      })
      .mockImplementationOnce(async () => ({ success: true, message: "ok" }));
    render(<OssieInterchangePanel />);
    fireEvent.change(screen.getByTestId("ossie-file-input"), {
      target: { files: [new File(["model: shop"], "shop.ossie.yaml")] },
    });
    fireEvent.click(await screen.findByTestId("ossie-apply"));

    expect(await screen.findByTestId("ossie-result-table:orders")).toHaveTextContent(
      "Table name 'orders' is ambiguous across domains",
    );
    expect(screen.getByTestId("ossie-result-table:customers")).toBeInTheDocument();
    expect(registerTable).toHaveBeenCalledTimes(2);
    await waitFor(() =>
      expect(screen.getByTestId("ossie-apply")).not.toHaveAttribute("data-loading"),
    );
  });
});
