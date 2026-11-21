// Copyright (c) 2026 Kenneth Stott
// Canary: 8c1d2e4f-7a35-4b9e-a6d0-2f5b1c9e3a77
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// A source whose connector server is still starting lists no tables yet: the Tables field says
// so (not an empty list), then offers the tables once the server answers. A table whose columns
// are still being read is offered, cannot be picked yet and says so, then can be picked.

import { describe, expect, it, vi } from "vitest";
import { render, screen, waitFor } from "../../../test-utils/render";
import { expectReason, held, openTables, tablesField } from "./registerFormHelpers";

vi.mock("../../../context/DomainFilterContext", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../../context/DomainFilterContext")>()),
  useDomainFilter: () => ({ ensureDomainChecked: vi.fn() }),
}));

const listing = { starting: true };
const GET_STORE = [{ name: "getStore", comment: "The store", pagingKind: null, pagination: null }];
vi.mock("../../../hooks/useAdminQueries", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../../hooks/useAdminQueries")>()),
  useAvailableSchemas: () => ({ schemas: ["openapi"], loading: false }),
  useAvailableTables: () => ({
    tables: listing.starting ? [] : GET_STORE,
    loading: false,
    starting: listing.starting,
    startingTimedOut: false,
  }),
}));

let answer: (columns: unknown[]) => void = () => {};
const readColumns = vi.fn(
  () =>
    new Promise<unknown[]>((resolve) => {
      answer = resolve;
    }),
);
vi.mock("../../../hooks/useAvailableColumnsMetadataLazy", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../../hooks/useAvailableColumnsMetadataLazy")>()),
  useReadColumns: () => readColumns,
}));

const { RegisterTableForm } = await import("../RegisterTableForm");

describe("RegisterTableForm — tables of a starting source", () => {
  it("shows the starting state, then the tables, each pickable once its columns are read", async () => {
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
    expect(
      await screen.findByText(
        "Starting up, discovering files… this can take a while for large datasets",
      ),
    ).toBeInTheDocument();
    expect(tablesField()).toBeDisabled();

    listing.starting = false;
    rerender(<RegisterTableForm {...props} />);
    const user = await openTables();
    await waitFor(() => expect(held("getStore")).toBe(true));
    await expectReason(user, "getStore", "Reading its columns…");
    answer([
      {
        name: "id",
        dataType: "integer",
        isPrimaryKey: true,
        comment: null,
        nativeFilterType: null,
      },
    ]);
    await waitFor(() => expect(held("getStore")).toBe(false));
  });
});
