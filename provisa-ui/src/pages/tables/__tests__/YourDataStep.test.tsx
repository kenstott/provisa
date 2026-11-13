// Copyright (c) 2026 Kenneth Stott
// Canary: 2f8c4a61-7d39-4b0e-9e56-a3b1d8c7f240
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2234 (SEE THE DATA RIGHT AFTER REGISTERING): once tables are registered, the panel shows
// the first of them, the others a click away: its first rows read as the person's own role, a
// button that opens the query explorer on a query ready to run, and the other ways to reach it,
// each ready to copy. A table its registration refused is named.

import { beforeEach, describe, expect, it, vi } from "vitest";
import userEvent from "@testing-library/user-event";
import { Route, Routes, useLocation } from "react-router-dom";
import { render, screen, waitFor } from "../../../test-utils/render";
import type { TableAccess } from "../../../hooks/useTableQueries";

const ACCESS: Record<number, TableAccess> = {
  11: {
    graphql: "query {\n  s__orders(limit: 10) {\n    id\n  }\n}",
    sql: 'SELECT * FROM "sales"."orders" LIMIT 10',
    pgwire: "postgresql://data.example.com:5433/provisa?options=-csearch_path%3Dsales",
    rest: "GET https://data.example.com/data/rest/sales/orders?limit=20",
    mcp: '{\n  "tool": "run_sql"\n}',
    draft: false,
  },
  12: {
    graphql: "query {\n  s__customers(limit: 10) {\n    id\n  }\n}",
    sql: 'SELECT * FROM "sales"."customers" LIMIT 10',
    pgwire: null,
    rest: null,
    mcp: '{\n  "tool": "run_sql"\n}',
    draft: true,
  },
};
const asked: [number | null, string | null][] = [];

vi.mock("../../../hooks/useAdminQueries", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../../hooks/useAdminQueries")>()),
  useTables: () => ({
    tables: [
      { id: 11, tableName: "orders", alias: null, columns: [] },
      { id: 12, tableName: "customers", alias: null, columns: [] },
      { id: 13, tableName: "other", alias: null, columns: [] },
    ],
    loading: false,
  }),
  useTableAccess: (tableId: number | null, role: string | null) => {
    asked.push([tableId, role]);
    return { access: tableId === null ? null : ACCESS[tableId], loading: false, error: null };
  },
}));

vi.mock("../../../components/GovernedTableViewer", () => ({
  GovernedTableViewer: ({ table, role }: { table: { tableName: string }; role: string }) => (
    <div data-testid="viewer">
      rows of {table.tableName} as {role}
    </div>
  ),
}));

const { YourDataStep } = await import("../YourDataStep");

function QueryPage() {
  const location = useLocation();
  return <pre data-testid="query-page">{(location.state as { query: string }).query}</pre>;
}

function renderStep(refused: { tableName: string; message: string }[] = []) {
  render(
    <Routes>
      <Route
        path="/"
        element={
          <YourDataStep
            registered={{
              tableIds: [11, 12],
              refused: refused.map((r) => ({
                tableName: r.tableName,
                result: { success: false, message: r.message },
              })),
            }}
            role="analyst"
          />
        }
      />
      <Route path="/query" element={<QueryPage />} />
    </Routes>,
  );
}

beforeEach(() => {
  asked.length = 0;
});

describe("Your data, right after registering", () => {
  it("shows the first table's rows as the person's role, the others a click away", async () => {
    renderStep();
    expect(screen.getByTestId("viewer")).toHaveTextContent("rows of orders as analyst");
    expect(screen.getByTestId("your-data-tab-customers")).toBeInTheDocument();
    expect(screen.queryByTestId("your-data-tab-other")).toBeNull();
    await userEvent.click(screen.getByTestId("your-data-tab-customers"));
    expect(screen.getByTestId("viewer")).toHaveTextContent("rows of customers as analyst");
    expect(asked).toContainEqual([12, "analyst"]);
  });

  it("offers every way to reach the table, each ready to copy", () => {
    renderStep();
    expect(screen.getByTestId("your-data-graphql")).toHaveTextContent("s__orders(limit: 10)");
    expect(screen.getByTestId("your-data-pgwire")).toHaveTextContent(
      "postgresql://data.example.com:5433/provisa",
    );
    expect(screen.getByTestId("your-data-sql")).toHaveTextContent(
      'SELECT * FROM "sales"."orders" LIMIT 10',
    );
    expect(screen.getByTestId("your-data-rest")).toHaveTextContent("/data/rest/sales/orders");
    expect(screen.getByTestId("your-data-mcp")).toHaveTextContent("run_sql");
    expect(screen.getAllByTestId("copy-button")).toHaveLength(5);
  });

  it("leaves out a way the server is not serving", async () => {
    renderStep();
    await userEvent.click(screen.getByTestId("your-data-tab-customers"));
    expect(screen.queryByTestId("your-data-pgwire")).toBeNull();
    expect(screen.queryByTestId("your-data-rest")).toBeNull();
    expect(screen.getByTestId("your-data-graphql")).toBeInTheDocument();
  });

  it("opens the query explorer on the table's query, ready to run", async () => {
    renderStep();
    await userEvent.click(screen.getByTestId("your-data-open-query"));
    await waitFor(() =>
      expect(screen.getByTestId("query-page")).toHaveTextContent("s__orders(limit: 10)"),
    );
  });

  it("marks the ways to reach a draft as a draft's, and no other table's", async () => {
    renderStep();
    expect(screen.queryByTestId("your-data-draft")).toBeNull();
    await userEvent.click(screen.getByTestId("your-data-tab-customers"));
    expect(screen.getByTestId("your-data-draft")).toHaveTextContent("Draft");
    expect(screen.getByTestId("your-data-draft-note")).toHaveTextContent(
      "This table is a draft: until it is released, only roles that may register tables in its domain reach it these ways.",
    );
  });

  it("names each table whose registration was refused, with why", () => {
    renderStep([{ tableName: "salaries", message: "Missing capability: 'table_registration'" }]);
    expect(screen.getByTestId("your-data-refused")).toHaveTextContent(
      "salaries was not registered: Missing capability: 'table_registration'",
    );
  });
});
