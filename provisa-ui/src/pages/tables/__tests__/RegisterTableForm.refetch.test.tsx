// Copyright (c) 2026 Kenneth Stott
// Canary: 0a8d29c7-28bc-442a-8274-7201ecd7305b
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2234: a list of tables already answered stays in use while it is asked again. Every schema
// change (a registration anywhere in the org) asks each open query again; the Tables field was
// disabled while the list was asked again, which closed its open list under the person picking.

import { describe, expect, it, vi } from "vitest";
import { render, screen } from "../../../test-utils/render";
import { tablesField } from "./registerFormHelpers";

vi.mock("../../../context/DomainFilterContext", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../../context/DomainFilterContext")>()),
  useDomainFilter: () => ({ ensureDomainChecked: vi.fn() }),
}));

const list = { loading: false };
const GET_STORE = [{ name: "getStore", comment: "The store", pagingKind: null, pagination: null }];
vi.mock("../../../hooks/useAdminQueries", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../../hooks/useAdminQueries")>()),
  useAvailableSchemas: () => ({ schemas: ["openapi"], loading: false }),
  useAvailableTables: () => ({
    tables: GET_STORE,
    loading: list.loading,
    starting: false,
    startingTimedOut: false,
  }),
}));

const readColumns = vi.fn(() => new Promise<unknown[]>(() => {}));
vi.mock("../../../hooks/useAvailableColumnsMetadataLazy", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../../hooks/useAvailableColumnsMetadataLazy")>()),
  useReadColumns: () => readColumns,
}));

const { RegisterTableForm } = await import("../RegisterTableForm");

describe("RegisterTableForm — the table list asked again", () => {
  it("keeps the Tables field in use while the answered list is asked again", async () => {
    const props = {
      sources: [{ id: "petstore", type: "openapi", allowedDomains: [] } as never],
      domainHints: ["sales"],
      domainAccess: ["*"],
      checkedDomains: new Set<string>(),
      domainsEnabled: true,
      tables: [],
      roles: [{ id: "admin" } as never],
      suggestTableAlias: vi.fn().mockResolvedValue(""),
      onContinue: vi.fn(),
      onCancel: () => {},
      onProductsRegistered: vi.fn(),
      onRegisteredMany: vi.fn(),
      setError: vi.fn(),
      initialSourceId: "petstore",
    };
    const { rerender } = render(<RegisterTableForm {...props} />);
    expect(tablesField()).toBeEnabled();

    list.loading = true;
    rerender(<RegisterTableForm {...props} />);
    expect(tablesField()).toBeEnabled();
    expect(screen.queryByText("Listing the tables…")).not.toBeInTheDocument();
  });
});
