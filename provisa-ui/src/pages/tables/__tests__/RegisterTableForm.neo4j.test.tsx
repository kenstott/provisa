// Copyright (c) 2026 Kenneth Stott
// Canary: 2f7b9e14-6a3c-4d8e-b1f5-9c0a7d2e4b61
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

/**
 * REQ-1670: registering a table on a neo4j source.
 *
 * A neo4j source has no tables to list — its table IS a Cypher projection. The form takes a table
 * name and the Cypher, previews it (rows plus the column types the registration will carry), and
 * submits the Cypher as the table's queryTemplate. Nothing is introspected and no watermark or
 * discovery applies.
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
const preview = vi.fn();

vi.mock("../../../hooks/useAdminQueries", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../../hooks/useAdminQueries")>()),
  useAvailableSchemas: (sourceId: string | null) => useAvailableSchemas(sourceId),
  useAvailableTables: (sourceId: string | null, schema: string | null) =>
    useAvailableTables(sourceId, schema),
}));
vi.mock("../../../hooks/useNeo4jPreview", () => ({
  useNeo4jPreview: () => ({ preview }),
}));

const { RegisterTableForm } = await import("../RegisterTableForm");

const CYPHER = "MATCH (a:Adopter) RETURN a.adopter_id AS adopter_id, a.name AS name";

function renderForm() {
  const registerTable = vi.fn().mockResolvedValue({ success: true, message: "" });
  const setError = vi.fn();
  const getAvailableColumnsMetadata = vi.fn().mockResolvedValue([]);
  render(
    <RegisterTableForm
      sources={[{ id: "neo4j-demo", type: "neo4j", allowedDomains: [] } as never]}
      domainHints={["shelter"]}
      domainAccess={["*"]}
      checkedDomains={new Set<string>()}
      domainsEnabled
      tables={[]}
      roles={[{ id: "org_admin" } as never, { id: "analyst" } as never]}
      getAvailableColumnsMetadata={getAvailableColumnsMetadata}
      suggestTableAlias={vi.fn().mockResolvedValue("")}
      registerTable={registerTable}
      onSuccess={vi.fn()}
      setError={setError}
    />,
  );
  return { registerTable, setError, getAvailableColumnsMetadata };
}

async function pickSourceAndType() {
  const user = userEvent.setup();
  await user.selectOptions(screen.getByTestId("register-table-source-select"), "neo4j-demo");
  await user.selectOptions(screen.getByTestId("register-table-domain-select"), "shelter");
  fireEvent.change(await screen.findByTestId("register-table-neo4j-table-name"), {
    target: { value: "adopter" },
  });
  fireEvent.change(screen.getByTestId("register-table-neo4j-cypher"), {
    target: { value: CYPHER },
  });
  return user;
}

beforeEach(() => {
  vi.clearAllMocks();
  preview.mockResolvedValue({
    rows: [
      { adopter_id: 1, name: "Sara Kim" },
      { adopter_id: 2, name: "Tom Evans" },
    ],
    columns: [
      { name: "adopter_id", dataType: "integer" },
      { name: "name", dataType: "text" },
    ],
    error: null,
  });
});

describe("RegisterTableForm on a neo4j source (REQ-1670)", () => {
  it("replaces the schema/table pickers with a table name and Cypher, and never introspects", async () => {
    const { getAvailableColumnsMetadata } = renderForm();
    await pickSourceAndType();

    expect(screen.queryByTestId("register-table-schema-select")).not.toBeInTheDocument();
    expect(screen.queryByTestId("register-table-table-select")).not.toBeInTheDocument();
    expect(screen.queryByTestId("discover-columns-checkbox")).not.toBeInTheDocument();
    expect(screen.queryByTestId("register-table-watermark-select")).not.toBeInTheDocument();
    for (const call of useAvailableSchemas.mock.calls) expect(call[0]).toBeNull();
    for (const call of useAvailableTables.mock.calls) expect(call[0]).toBeNull();
    expect(getAvailableColumnsMetadata).not.toHaveBeenCalled();
  });

  it("previews the Cypher and seeds typed columns from what it returns", async () => {
    renderForm();
    const user = await pickSourceAndType();
    await user.click(screen.getByTestId("register-table-neo4j-preview"));

    await waitFor(() =>
      expect(screen.getByTestId("register-table-neo4j-preview-rows")).toBeInTheDocument(),
    );
    expect(preview).toHaveBeenCalledWith({ sourceId: "neo4j-demo", cypher: CYPHER });
    expect(screen.getByTestId("register-table-col-selected-adopter_id")).toBeChecked();
    expect(screen.getByTestId("register-table-col-datatype-adopter_id")).toHaveValue("integer");
    expect(screen.getByTestId("register-table-col-datatype-name")).toHaveValue("text");
  });

  it("shows a preview failure and keeps the operator's Cypher", async () => {
    preview.mockResolvedValueOnce({ rows: [], columns: [], error: "SyntaxError: bad cypher" });
    const { setError } = renderForm();
    const user = await pickSourceAndType();
    await user.click(screen.getByTestId("register-table-neo4j-preview"));

    await waitFor(() => expect(setError).toHaveBeenCalledWith("SyntaxError: bad cypher"));
    expect(screen.getByTestId("register-table-neo4j-cypher")).toHaveValue(CYPHER);
    expect(screen.queryByTestId("register-table-neo4j-preview-rows")).not.toBeInTheDocument();
  });

  it("refuses to register before a preview typed the columns", async () => {
    const { registerTable, setError } = renderForm();
    await pickSourceAndType();
    fireEvent.click(screen.getByTestId("register-table-submit"));

    await waitFor(() =>
      expect(setError).toHaveBeenCalledWith(
        "Preview the Cypher first so the columns and their types are known.",
      ),
    );
    expect(registerTable).not.toHaveBeenCalled();
  });

  it("registers the previewed columns with the Cypher as queryTemplate under the neo4j schema", async () => {
    const { registerTable } = renderForm();
    const user = await pickSourceAndType();
    await user.click(screen.getByTestId("register-table-neo4j-preview"));
    await waitFor(() =>
      expect(screen.getByTestId("register-table-col-datatype-name")).toHaveValue("text"),
    );
    fireEvent.click(screen.getByTestId("register-table-submit"));

    await waitFor(() => expect(registerTable).toHaveBeenCalledTimes(1));
    const input = registerTable.mock.calls[0][0];
    expect(input).toMatchObject({
      sourceId: "neo4j-demo",
      domainId: "shelter",
      tableName: "adopter",
      queryTemplate: CYPHER,
      discover: false,
      watermarkColumn: null,
    });
    expect(
      input.columns.map((c: { name: string; dataType: string }) => [c.name, c.dataType]),
    ).toEqual([
      ["adopter_id", "integer"],
      ["name", "text"],
    ]);
  });
});
