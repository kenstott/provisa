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

import { beforeEach, describe, it, expect, vi } from "vitest";
import { fireEvent, render, screen } from "../test-utils/render";

// The rights the viewer holds: reading reports is the ops domain's right (observability), which
// opened the page; adding and removing one is registering a table in the ops domain.
const held = vi.hoisted(() => ({ capabilities: ["observability", "table_registration"] }));
vi.mock("../context/AuthContext", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../context/AuthContext")>()),
  useAuth: () => ({
    actingRoles: "org_admin",
    capabilities: held.capabilities,
    demonstrated: [],
    loading: false,
  }),
}));
vi.mock("../hooks/useAdminQueries", () => ({
  useTables: () => ({
    tables: [
      { id: 1, domainId: "ops", tableName: "ontology_quality", alias: null, sourceId: "x" },
      {
        id: 2,
        domainId: "ops",
        tableName: "sensitive_column_coverage",
        alias: null,
        sourceId: "x",
      },
      { id: 3, domainId: "ops", tableName: "zz_my_report", alias: null, sourceId: "__derived__" },
    ],
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

vi.mock("../components/admin/OntologyDriftPanel", () => ({
  OntologyDriftPanel: () => <div data-testid="ontology-drift" />,
}));

import { ReportsTab } from "../components/admin/ReportsTab";
import { DERIVED_SOURCE_ID } from "../types/admin";

const QUALITY = { initialEntries: ["/admin/reports?report=ontology_quality"] };

beforeEach(() => localStorage.clear());

describe("ReportsTab", () => {
  it("links a report's term cells to the term", () => {
    render(<ReportsTab title="Admin — Reports" />, QUALITY);
    expect(screen.getByTestId("viewer")).toHaveAttribute("data-href", "/context/ontology?term=7");
  });

  it("shows drift with the Ontology quality report", () => {
    render(<ReportsTab title="Admin — Reports" />, QUALITY);
    expect(screen.getByTestId("ontology-drift")).toBeInTheDocument();
  });

  it("offers adding and removing a report to a table registrar only", () => {
    expect(DERIVED_SOURCE_ID).toBe("__derived__");
    held.capabilities = ["observability", "table_registration"];
    const { unmount } = render(<ReportsTab title="Admin — Reports" />);
    expect(screen.getByTestId("add-report-btn")).toBeInTheDocument();
    // A custom report's panel offers its delete; a built-in report's does not.
    fireEvent.click(screen.getByTestId("report-item-sensitive_column_coverage"));
    expect(screen.queryByRole("button", { name: "Delete report" })).not.toBeInTheDocument();
    fireEvent.click(screen.getByTestId("report-item-zz_my_report"));
    expect(screen.getByRole("button", { name: "Delete report" })).toBeInTheDocument();
    unmount();
    localStorage.clear();

    held.capabilities = ["observability"];
    render(<ReportsTab title="Admin — Reports" />);
    fireEvent.click(screen.getByTestId("report-item-zz_my_report"));
    expect(screen.getByTestId("report-pane")).toHaveAttribute("data-report", "zz_my_report");
    expect(screen.queryByTestId("add-report-btn")).not.toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "Delete report" })).not.toBeInTheDocument();
    held.capabilities = ["observability", "table_registration"];
  });

  it("opens the report its deep link names", () => {
    render(<ReportsTab title="Admin — Reports" />, {
      initialEntries: ["/admin/reports?report=sensitive_column_coverage"],
    });
    expect(screen.getByTestId("report-pane")).toHaveAttribute(
      "data-report",
      "sensitive_column_coverage",
    );
    expect(screen.queryByTestId("ontology-drift")).not.toBeInTheDocument();
    expect(screen.getByTestId("report-item-sensitive_column_coverage")).toHaveClass("row-selected");
  });
});
