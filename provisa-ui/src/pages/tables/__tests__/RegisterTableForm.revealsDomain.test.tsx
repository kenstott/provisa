// Copyright (c) 2026 Kenneth Stott
// Canary: 7d12911b-a237-4e8a-adc4-e9b89aaeac12
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// Register Table names the domain its tables are registered into; the form reveals nothing in
// the domain filter itself: the page that hears what was registered does (REQ-2109, REQ-2234).

import { describe, expect, it, vi } from "vitest";
import { render, screen, waitFor } from "../../../test-utils/render";
import { held, openTables, submitTables, tableOption } from "./registerFormHelpers";

const ensureDomainChecked = vi.fn();

vi.mock("../../../context/DomainFilterContext", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../../context/DomainFilterContext")>()),
  useDomainFilter: () => ({ ensureDomainChecked }),
}));

vi.mock("../../../hooks/useAdminQueries", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../../hooks/useAdminQueries")>()),
  useAvailableSchemas: () => ({ schemas: ["openapi"], loading: false }),
  useAvailableTables: () => ({
    tables: [{ name: "getStore", comment: null, pagingKind: "endpoint", pagination: null }],
    loading: false,
  }),
}));
const readColumns = async () => [
  { name: "id", dataType: "integer", comment: null, nativeFilterType: null, isPrimaryKey: true },
];
vi.mock("../../../hooks/useAvailableColumnsMetadataLazy", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../../hooks/useAvailableColumnsMetadataLazy")>()),
  useReadColumns: () => readColumns,
}));
const registerTables = vi.fn();
vi.mock("../../../hooks/useTableQueries", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../../hooks/useTableQueries")>()),
  useRegisterTables: () => ({ registerTables, loading: false }),
}));
vi.mock("../../../api/admin", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../../api/admin")>()),
  fetchTableUniqueConstraints: vi.fn().mockResolvedValue([]),
}));

const { RegisterTableForm } = await import("../RegisterTableForm");

describe("RegisterTableForm names the domain it registers into", () => {
  it("registers in the one domain, revealing nothing itself", async () => {
    const outcomes = [
      { tableName: "getStore", result: { success: true, message: "", params: { id: 1 } } },
    ];
    registerTables.mockResolvedValue(outcomes);
    const onRegisteredMany = vi.fn();
    render(
      <RegisterTableForm
        sources={[{ id: "petstore", type: "openapi", allowedDomains: [] } as never]}
        domainHints={["inventory-copies"]}
        domainAccess={["*"]}
        checkedDomains={new Set<string>()}
        domainsEnabled={true}
        tables={[]}
        roles={[{ id: "admin" } as never]}
        suggestTableAlias={vi.fn().mockResolvedValue("")}
        onContinue={vi.fn()}
        onCancel={() => {}}
        onProductsRegistered={vi.fn()}
        onRegisteredMany={onRegisteredMany}
        setError={vi.fn()}
        initialSourceId="petstore"
      />,
    );
    // The one domain the caller may register in is the domain: it is not asked.
    expect(screen.queryByTestId("register-table-domain-select")).toBeNull();
    const user = await openTables();
    await waitFor(() => expect(held("getStore")).toBe(false));
    await user.click(tableOption("getStore"));
    await user.click(submitTables());
    await waitFor(() => expect(onRegisteredMany).toHaveBeenCalledWith(outcomes));
    expect(registerTables.mock.calls[0][0][0].domainId).toBe("inventory-copies");
    expect(ensureDomainChecked).not.toHaveBeenCalled();
  });
});
