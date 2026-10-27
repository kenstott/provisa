// Copyright (c) 2026 Kenneth Stott
// Canary: 05f1d1de-9705-4f92-b3c6-6d06247f118a
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1620: a report is read as the selected roles, the "Role: All" set included, never as a role
// the picker did not select.

import { describe, it, expect, vi } from "vitest";
import { render, screen } from "../../../test-utils/render";

vi.mock("../../../context/AuthContext", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../../context/AuthContext")>()),
  useAuth: () => ({ actingRoles: "analyst,steward" }),
}));
vi.mock("../../../hooks/useAdminQueries", () => ({
  useTables: () => ({
    tables: [{ id: 1, domainId: "ops", tableName: "slow_queries", alias: null, sourceId: "ops" }],
    loading: false,
    refetch: vi.fn(),
  }),
  useRegisterTable: () => ({ registerTable: vi.fn() }),
  useDeleteTable: () => ({ deleteTable: vi.fn() }),
}));
vi.mock("../../GovernedTableViewer", () => ({
  GovernedTableViewer: ({ role }: { role: string | null }) => (
    <div data-testid="viewer" data-role={role ?? ""} />
  ),
}));

import { ReportsTab } from "../ReportsTab";

describe("ReportsTab", () => {
  it("reads the report as the selected set of roles", () => {
    render(<ReportsTab />);
    expect(screen.getByTestId("viewer")).toHaveAttribute("data-role", "analyst,steward");
  });
});
