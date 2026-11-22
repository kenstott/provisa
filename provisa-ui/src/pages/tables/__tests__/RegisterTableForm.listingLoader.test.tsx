// Copyright (c) 2026 Kenneth Stott
// Canary: 3c1e7f52-9a4d-4b86-a0e3-6d2f8b5c7a19
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// While a source's tables are listed, the Tables field shows the standard field loader beside its
// "Listing the tables…" hint, and the loader goes once the list answers.

import { describe, expect, it, vi } from "vitest";
import { render, screen } from "../../../test-utils/render";

vi.mock("../../../context/DomainFilterContext", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../../context/DomainFilterContext")>()),
  useDomainFilter: () => ({ ensureDomainChecked: vi.fn() }),
}));

const ORDERS = [{ name: "orders", comment: null, pagingKind: null, pagination: null }];
const list: { tables: typeof ORDERS; loading: boolean; starting: boolean } = {
  tables: [],
  loading: true,
  starting: false,
};
vi.mock("../../../hooks/useAdminQueries", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../../hooks/useAdminQueries")>()),
  useAvailableSchemas: () => ({ schemas: ["files"], loading: false }),
  useAvailableTables: () => ({
    tables: list.tables,
    loading: list.loading,
    starting: list.starting,
    startingTimedOut: false,
  }),
}));

const { RegisterTableForm } = await import("../RegisterTableForm");

const props = {
  sources: [{ id: "crawl", type: "sqlite", allowedDomains: [] } as never],
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
  initialSourceId: "crawl",
};

const tablesField = () => screen.getByTestId("register-tables-field");

describe("RegisterTableForm — the Tables field while listing", () => {
  it("shows the loader while the listing is pending and drops it once the list answers", () => {
    list.tables = [];
    list.loading = true;
    list.starting = false;
    const { rerender } = render(<RegisterTableForm {...props} />);
    expect(tablesField()).toHaveTextContent("Listing the tables…");
    expect(tablesField().querySelector('[data-testid="multi-select-loading"]')).not.toBeNull();

    list.tables = ORDERS;
    list.loading = false;
    rerender(<RegisterTableForm {...props} />);
    expect(tablesField()).not.toHaveTextContent("Listing the tables…");
    expect(screen.queryByTestId("multi-select-loading")).not.toBeInTheDocument();
  });

  it("shows the loader while the source's tables are starting", () => {
    list.tables = [];
    list.loading = true;
    list.starting = true;
    render(<RegisterTableForm {...props} />);
    expect(tablesField().querySelector('[data-testid="multi-select-loading"]')).not.toBeNull();
  });
});
