// Copyright (c) 2026 Kenneth Stott
// Canary: 1d7e3b52-9a64-4c08-b5f1-2e8a6c4d9f37
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1386: the Reports viewer hands its grid the report links, so a seeded report's term cells
// open the term.

import { describe, it, expect, vi } from "vitest";
import { render, screen } from "../test-utils/render";

vi.mock("../context/AuthContext", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../context/AuthContext")>()),
  useAuth: () => ({ actingRoles: "org_admin" }),
}));
vi.mock("../hooks/useAdminQueries", () => ({
  useTables: () => ({
    tables: [{ id: 1, domainId: "ops", tableName: "ontology_quality", alias: null, sourceId: "x" }],
    loading: false,
    refetch: vi.fn(),
  }),
  useRegisterTable: () => ({ registerTable: vi.fn() }),
  useDeleteTable: () => ({ deleteTable: vi.fn() }),
}));
vi.mock("../components/GovernedTableViewer", () => ({
  GovernedTableViewer: ({
    cellHref,
  }: {
    cellHref?: (column: string, value: unknown) => string | null;
  }) => <div data-testid="viewer" data-href={cellHref?.("term_id", 7) ?? ""} />,
}));

import { ReportsTab } from "../components/admin/ReportsTab";

describe("ReportsTab", () => {
  it("links a report's term cells to the term", () => {
    render(<ReportsTab />);
    expect(screen.getByTestId("viewer")).toHaveAttribute("data-href", "/admin/ontology?term=7");
  });
});
