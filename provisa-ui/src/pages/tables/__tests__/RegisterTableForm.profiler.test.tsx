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
            masks: [],
          },
          {
            name: "value",
            dataType: "varchar",
            description: "Value.",
            visibleTo: ["analyst", "org_admin"],
            masks: [
              { to: ["org_admin"], mask: null },
              { to: ["*"], mask: { type: "constant", value: null } },
            ],
          },
        ],
        rowRules: [
          {
            roleId: "analyst",
            filter: "column_name IN ('id')",
            description: "Keeps the profile rows analyst may read.",
          },
        ],
      },
    ],
  },
];

beforeEach(() => {
  vi.clearAllMocks();
  fetchProfilerCatalog.mockResolvedValue(CATALOG);
});

function renderForm() {
  const onContinue = vi.fn();
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
      onContinue={onContinue}
      onCancel={() => {}}
      setError={vi.fn()}
    />,
  );
  return { onContinue };
}

describe("RegisterTableForm on a Data Profiler source (REQ-1934)", () => {
  it("registers a picked result table with the catalog's rules, then its edited row rules", async () => {
    const { onContinue } = renderForm();
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
    await waitFor(() => expect(onContinue).toHaveBeenCalled());
    const input = onContinue.mock.calls[0][0].input;
    expect(input.tableName).toBe(TOP);
    const value = input.columns.find((c: { name: string }) => c.name === "value");
    expect(value).toMatchObject({
      visibleTo: ["analyst", "org_admin"],
      masks: [
        { to: ["org_admin"], mask: null },
        { to: ["*"], mask: { type: "constant", value: null } },
      ],
    });
    // The edited row rules go with the registration, saved once the table exists (REQ-2109).
    expect(onContinue.mock.calls[0][0].rowRules).toContainEqual({
      roleId: "analyst",
      filter: "column_name IN ('id', 'region')",
      description: "Keeps the profile rows analyst may read.",
    });
  });
});
