// Copyright (c) 2026 Kenneth Stott
// Canary: 47c2e8a5-1b93-4d60-8f7e-3a9d5c0b2e16
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

/**
 * REQ-1934: registering a Data Profiler's result table. The source is never introspected; its
 * catalog lists the result tables per member, each with the grants, masks and row rules the form
 * starts from. The operator may edit the row rules; they are saved after the table.
 */

import { beforeEach, describe, expect, it, vi } from "vitest";
import userEvent from "@testing-library/user-event";
import { fireEvent, render, screen, waitFor } from "../../../test-utils/render";

const useAvailableSchemas = vi.fn((_sourceId: string | null) => ({
  schemas: [] as string[],
  loading: false,
}));
const useAvailableTables = vi.fn((_sourceId: string | null, _schema: string | null) => ({
  tables: [] as { name: string }[],
  loading: false,
}));
vi.mock("../../../hooks/useAdminQueries", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../../hooks/useAdminQueries")>()),
  useAvailableSchemas: (sourceId: string | null) => useAvailableSchemas(sourceId),
  useAvailableTables: (sourceId: string | null, schema: string | null) =>
    useAvailableTables(sourceId, schema),
}));

const upsertRlsRule = vi.fn();
vi.mock("../../../hooks/useSecurityQueries", () => ({
  useUpsertRlsRule: () => ({ upsertRlsRule, loading: false }),
}));

const fetchProfilerCatalog = vi.fn();
vi.mock("../../../api/profiler", () => ({
  fetchProfilerCatalog: (id: string) => fetchProfilerCatalog(id),
}));

const { RegisterTableForm } = await import("../RegisterTableForm");

const TOP = "orders_7_profile_top_values";
const CATALOG = [
  {
    member: "orders",
    memberId: 7,
    tables: [
      {
        kind: "top_values",
        tableName: TOP,
        columns: [
          {
            name: "run_id",
            dataType: "varchar",
            description: "Run.",
            visibleTo: ["analyst", "org_admin"],
            unmaskedTo: [],
            maskType: null,
          },
          {
            name: "value",
            dataType: "varchar",
            description: "Value.",
            visibleTo: ["analyst", "org_admin"],
            unmaskedTo: ["org_admin"],
            maskType: "constant",
          },
        ],
        rowRules: [{ roleId: "analyst", filter: "column_name IN ('id')" }],
      },
    ],
  },
];

beforeEach(() => {
  vi.clearAllMocks();
  fetchProfilerCatalog.mockResolvedValue(CATALOG);
  upsertRlsRule.mockResolvedValue({ success: true, message: "" });
});

function renderForm() {
  const registerTable = vi.fn().mockResolvedValue({ success: true, message: "" });
  const onSuccess = vi.fn();
  render(
    <RegisterTableForm
      sources={[{ id: "prof", type: "data_profiler", allowedDomains: [] } as never]}
      domainHints={["sales"]}
      domainAccess={["*"]}
      checkedDomains={new Set<string>()}
      domainsEnabled
      tables={[]}
      roles={[{ id: "org_admin" } as never, { id: "analyst" } as never]}
      getAvailableColumnsMetadata={vi.fn()}
      suggestTableAlias={vi.fn().mockResolvedValue("")}
      registerTable={registerTable}
      onSuccess={onSuccess}
      onCancel={() => {}}
      setError={vi.fn()}
    />,
  );
  return { registerTable, onSuccess };
}

describe("RegisterTableForm on a Data Profiler source (REQ-1934)", () => {
  it("registers a picked result table with the catalog's rules, then its edited row rules", async () => {
    const { registerTable, onSuccess } = renderForm();
    const user = userEvent.setup();
    await user.selectOptions(screen.getByTestId("register-table-source-select"), "prof");
    await user.selectOptions(screen.getByTestId("register-table-domain-select"), "sales");
    expect(screen.queryByTestId("register-table-schema-select")).not.toBeInTheDocument();
    expect(useAvailableSchemas).not.toHaveBeenCalledWith("prof");

    const picker = await screen.findByTestId("register-table-profiler-select");
    await waitFor(() => expect(picker).not.toBeDisabled());
    await user.selectOptions(picker, TOP);
    const rule = await screen.findByTestId("register-table-profiler-row-rule-analyst");
    expect(rule).toHaveValue("column_name IN ('id')");
    fireEvent.change(rule, { target: { value: "column_name IN ('id', 'region')" } });

    await user.click(screen.getByTestId("register-table-submit"));
    await waitFor(() => expect(onSuccess).toHaveBeenCalled());
    const input = registerTable.mock.calls[0][0];
    expect(input.tableName).toBe(TOP);
    const value = input.columns.find((c: { name: string }) => c.name === "value");
    expect(value).toMatchObject({
      visibleTo: ["analyst", "org_admin"],
      unmaskedTo: ["org_admin"],
      maskType: "constant",
    });
    expect(upsertRlsRule).toHaveBeenCalledWith({
      tableId: TOP,
      roleId: "analyst",
      filterExpr: "column_name IN ('id', 'region')",
    });
  });
});
