// Copyright (c) 2026 Kenneth Stott
// Canary: 0c7e4b52-8a19-4d3f-b6e2-5f9a1d7c3e84
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// The GraphQL page's Provisa panel: its Semantic SQL has an Open in SQL (→) action, as its Cypher
// has Open in Graph, opening the SQL page with the whole query's semantic SQL.

import React from "react";
import { describe, it, expect, vi } from "vitest";
import { render, screen, fireEvent, waitFor } from "../../test-utils/render";

const navigate = vi.fn();
vi.mock("react-router-dom", async (importOriginal) => ({
  ...(await importOriginal<typeof import("react-router-dom")>()),
  useNavigate: () => navigate,
}));

vi.mock("@graphiql/react", async (importOriginal) => ({
  ...(await importOriginal<typeof import("@graphiql/react")>()),
  useOperationsEditorState: vi.fn(() => ["{ orders { id } customers { id } }", vi.fn()]),
}));

const root = (field: string, sql: string) => ({
  sql,
  semantic_sql: sql,
  params: [],
  route: "direct",
  sources: [],
  root_field: field,
  canonical_field: field,
  column_aliases: [],
  compiled_cypher: null,
  cypher_error: null,
});

vi.mock("../../hooks/useAdminQueries", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../hooks/useAdminQueries")>()),
  useCompileQuery: () => ({
    compileQuery: vi.fn().mockResolvedValue({
      queries: [
        root("orders", "SELECT id FROM orders"),
        root("customers", "SELECT id FROM customers"),
      ],
    }),
    loading: false,
  }),
}));

import { provisaToolsPlugin } from "../provisa-tools";

describe("Provisa panel, Semantic SQL", () => {
  it("opens the whole query's semantic SQL on the SQL page", async () => {
    const Content = provisaToolsPlugin("analyst").content as () => React.ReactElement;
    render(<Content />);
    const open = await screen.findByTestId("provisa-tools-open-sql", {}, { timeout: 3000 });
    fireEvent.click(open);
    await waitFor(() => expect(navigate).toHaveBeenCalled());
    const [path, options] = navigate.mock.calls[0];
    expect(path).toBe("/sql");
    expect(options.state.autoRun).toBe(true);
    expect(options.state.sql).toContain("FROM\n  orders");
    expect(options.state.sql).toContain("FROM\n  customers");
  });
});
