// Copyright (c) 2026 Kenneth Stott
// Canary: 7d4b2e91-5c3a-4f86-b1e0-8a6c2f9d3e57
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// A File Crawler source whose tables cannot be listed shows why on the Tables field: the listing's
// refusal (a connector bundle not staged names the bundle and the stage command), never "This
// schema has no tables". And its files, once listed, are offered as tables to pick.

import { describe, expect, it, vi } from "vitest";
import { render, screen } from "../../../test-utils/render";

vi.mock("../../../context/DomainFilterContext", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../../context/DomainFilterContext")>()),
  useDomainFilter: () => ({ ensureDomainChecked: vi.fn() }),
}));

const REFUSAL =
  "the pgwire-file bundle (engine-v0.131.1) is not staged: no launcher under " +
  "/cache/engine-v0.131.1/pgwire-file. run `python -m provisa.runtime_deps.stage_assets` once";
type Table = { name: string; comment: null; pagingKind: null; pagination: null };
const list: { tables: Table[]; error: string | null } = { tables: [], error: null };
vi.mock("../../../hooks/useAdminQueries", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../../hooks/useAdminQueries")>()),
  useAvailableSchemas: () => ({ schemas: ["test3"], loading: false, error: null }),
  useAvailableTables: () => ({
    tables: list.tables,
    loading: false,
    starting: false,
    startingTimedOut: false,
    error: list.error,
  }),
}));

const { RegisterTableForm } = await import("../RegisterTableForm");

const props = {
  sources: [{ id: "test3", type: "files", allowedDomains: [] } as never],
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
  initialSourceId: "test3",
};

const tablesField = () => screen.getByTestId("register-tables-field");

describe("RegisterTableForm — a File Crawler source's Tables field", () => {
  it("names the listing's refusal, not that the schema has no tables", () => {
    list.tables = [];
    list.error = REFUSAL;
    render(<RegisterTableForm {...props} />);
    expect(tablesField()).toHaveTextContent("pgwire-file bundle (engine-v0.131.1) is not staged");
    expect(tablesField()).toHaveTextContent("provisa.runtime_deps.stage_assets");
    expect(tablesField()).not.toHaveTextContent("This schema has no tables.");
  });

  it("offers the crawled files as tables to pick", () => {
    list.error = null;
    list.tables = ["complaints_2026_08", "orders"].map((name) => ({
      name,
      comment: null,
      pagingKind: null,
      pagination: null,
    }));
    render(<RegisterTableForm {...props} />);
    expect(tablesField()).not.toHaveTextContent("This schema has no tables.");
    expect(tablesField().querySelector("input")).not.toBeDisabled();
  });
});
