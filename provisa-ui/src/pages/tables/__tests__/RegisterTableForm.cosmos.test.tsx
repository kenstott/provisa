// Copyright (c) 2026 Kenneth Stott
// Canary: 29005e13-5996-457f-8dd4-4b893f6191c9
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

/**
 * REQ-1982: registering a Cosmos DB query table. The form takes the table's name, its query
 * ({"container", "query"}) and its @arguments, previews the query with sample values, and
 * registers the query with each argument as a query_arg parameter under the source's database.
 */

import { beforeEach, describe, expect, it, vi } from "vitest";
import userEvent from "@testing-library/user-event";
import { fireEvent, render, screen, waitFor, within } from "../../../test-utils/render";

const preview = vi.fn();

vi.mock("../../../hooks/useAdminQueries", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../../hooks/useAdminQueries")>()),
  useAvailableSchemas: () => ({ schemas: [] as string[], loading: false }),
  useAvailableTables: () => ({ tables: [] as { name: string }[], loading: false }),
}));
vi.mock("../../../hooks/useQueryPreview", () => ({
  useQueryPreview: () => ({ preview }),
}));

const { RegisterTableForm } = await import("../RegisterTableForm");

const QUERY = JSON.stringify({
  container: "orders",
  query: "SELECT c.id, c.total FROM c WHERE c.status = @status",
});

beforeEach(() => {
  vi.clearAllMocks();
  preview.mockResolvedValue({
    rows: [{ id: "o1", total: 12.5 }],
    columns: [
      { name: "id", dataType: "text", path: null },
      { name: "total", dataType: "double", path: null },
    ],
    error: null,
  });
});

describe("RegisterTableForm on a Cosmos DB query (REQ-1982)", () => {
  it("previews the query with its arguments and registers them as query_arg parameters", async () => {
    const onContinue = vi.fn();
    render(
      <RegisterTableForm
        sources={[
          { id: "cosmos", type: "cosmosdb", database: "shop", allowedDomains: [] } as never,
        ]}
        domainHints={["shop"]}
        domainAccess={["*"]}
        checkedDomains={new Set<string>()}
        domainsEnabled
        tables={[]}
        roles={[{ id: "analyst" } as never]}
        getAvailableColumnsMetadata={vi.fn().mockResolvedValue([])}
        suggestTableAlias={vi.fn().mockResolvedValue("")}
        onContinue={onContinue}
        onCancel={() => {}}
        setError={vi.fn()}
      />,
    );
    const user = userEvent.setup();
    await user.selectOptions(screen.getByTestId("register-table-source-select"), "cosmos");
    await user.click(within(screen.getByTestId("register-table-cosmosdb-mode")).getByText("Query"));
    await user.selectOptions(screen.getByTestId("register-table-domain-select"), "shop");
    fireEvent.change(await screen.findByTestId("register-table-cosmosdb-table-name"), {
      target: { value: "orders_by_status" },
    });
    fireEvent.change(screen.getByTestId("register-table-cosmosdb-pipeline"), {
      target: { value: QUERY },
    });
    await user.click(screen.getByTestId("register-table-pipeline-arg-add"));
    const row = screen.getByTestId("register-table-pipeline-arg-0");
    fireEvent.change(within(row).getByLabelText("Parameter"), { target: { value: "status" } });
    fireEvent.change(within(row).getByLabelText("Sample value"), { target: { value: "open" } });
    await user.click(screen.getByTestId("register-table-cosmosdb-preview"));
    await waitFor(() => expect(preview).toHaveBeenCalledTimes(1));
    expect(preview).toHaveBeenCalledWith({
      sourceType: "cosmosdb",
      sourceId: "cosmos",
      query: QUERY,
      arguments: [{ name: "status", dataType: "text", value: "open" }],
    });
    await waitFor(() =>
      expect(screen.getByTestId("register-table-col-selected-total")).toBeChecked(),
    );
    fireEvent.click(screen.getByTestId("register-table-submit"));
    await waitFor(() => expect(onContinue).toHaveBeenCalledTimes(1));
    const input = onContinue.mock.calls[0][0].input;
    expect(input).toMatchObject({
      schemaName: "shop",
      tableName: "orders_by_status",
      queryTemplate: QUERY,
    });
    expect(input.columns.find((c: { name: string }) => c.name === "_nf_status")).toMatchObject({
      nativeFilterType: "query_arg",
      nativeFilterRequired: false,
    });
  });
});
