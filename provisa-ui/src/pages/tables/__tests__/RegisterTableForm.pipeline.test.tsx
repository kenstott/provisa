// Copyright (c) 2026 Kenneth Stott
// Canary: 0195e0c5-95c3-4f1f-b71b-1d52470ea021
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

/**
 * REQ-1980: registering a MongoDB table from an aggregation pipeline. The form takes the table's
 * name, its native query and its arguments (each a parameter of the table, REQ-1986), previews the
 * query with sample argument values, and submits the native query as the table's queryTemplate
 * with each argument as a query_arg parameter column under the source's database.
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

const NATIVE = JSON.stringify({
  inputCollection: "movies",
  pipeline: [{ $match: { genre: "{{genre}}" } }, { $project: { _id: 0, title: 1 } }],
});

function renderForm() {
  const onContinue = vi.fn();
  const setError = vi.fn();
  render(
    <RegisterTableForm
      sources={[
        { id: "atlas", type: "mongodb", database: "sample_mflix", allowedDomains: [] } as never,
      ]}
      domainHints={["films"]}
      domainAccess={["*"]}
      checkedDomains={new Set<string>()}
      domainsEnabled
      tables={[]}
      roles={[{ id: "analyst" } as never]}
      getAvailableColumnsMetadata={vi.fn().mockResolvedValue([])}
      suggestTableAlias={vi.fn().mockResolvedValue("")}
      onContinue={onContinue}
      onCancel={() => {}}
      setError={setError}
    />,
  );
  return { onContinue, setError };
}

async function pickPipeline() {
  const user = userEvent.setup();
  await user.selectOptions(screen.getByTestId("register-table-source-select"), "atlas");
  await user.click(within(screen.getByTestId("register-table-mongodb-mode")).getByText("Pipeline"));
  await user.selectOptions(screen.getByTestId("register-table-domain-select"), "films");
  fireEvent.change(await screen.findByTestId("register-table-mongodb-table-name"), {
    target: { value: "dramas" },
  });
  fireEvent.change(screen.getByTestId("register-table-mongodb-pipeline"), {
    target: { value: NATIVE },
  });
  await user.click(screen.getByTestId("register-table-pipeline-arg-add"));
  const row = screen.getByTestId("register-table-pipeline-arg-0");
  fireEvent.change(within(row).getByLabelText("Parameter"), { target: { value: "genre" } });
  fireEvent.change(within(row).getByLabelText("Sample value"), { target: { value: "Drama" } });
  return user;
}

beforeEach(() => {
  vi.clearAllMocks();
  preview.mockResolvedValue({
    rows: [{ title: "Up" }],
    columns: [{ name: "title", dataType: "text", path: null }],
    error: null,
  });
});

describe("RegisterTableForm on a MongoDB pipeline (REQ-1980)", () => {
  it("offers a collection or a pipeline, and a pipeline lists no collections", async () => {
    renderForm();
    await pickPipeline();
    expect(screen.queryByTestId("register-table-schema-select")).not.toBeInTheDocument();
    expect(screen.queryByTestId("register-table-table-select")).not.toBeInTheDocument();
  });

  it("previews the native query with the sample argument values", async () => {
    renderForm();
    const user = await pickPipeline();
    await user.click(screen.getByTestId("register-table-mongodb-preview"));
    await waitFor(() => expect(preview).toHaveBeenCalledTimes(1));
    expect(preview).toHaveBeenCalledWith({
      sourceType: "mongodb",
      sourceId: "atlas",
      query: NATIVE,
      arguments: [{ name: "genre", dataType: "text", value: "Drama" }],
    });
  });

  it("refuses to register before a preview typed the output columns", async () => {
    const { onContinue, setError } = renderForm();
    await pickPipeline();
    fireEvent.click(screen.getByTestId("register-table-submit"));
    await waitFor(() => expect(setError).toHaveBeenCalled());
    expect(onContinue).not.toHaveBeenCalled();
  });

  it("registers the native query with its arguments as parameters under the database", async () => {
    const { onContinue } = renderForm();
    const user = await pickPipeline();
    const row = screen.getByTestId("register-table-pipeline-arg-0");
    await user.click(within(row).getByRole("switch"));
    await user.click(screen.getByTestId("register-table-mongodb-preview"));
    await waitFor(() =>
      expect(screen.getByTestId("register-table-col-selected-title")).toBeChecked(),
    );
    fireEvent.click(screen.getByTestId("register-table-submit"));
    await waitFor(() => expect(onContinue).toHaveBeenCalledTimes(1));
    const input = onContinue.mock.calls[0][0].input;
    expect(input).toMatchObject({
      sourceId: "atlas",
      schemaName: "sample_mflix",
      tableName: "dramas",
      queryTemplate: NATIVE,
      discover: false,
    });
    const genre = input.columns.find((c: { name: string }) => c.name === "_nf_genre");
    expect(genre).toMatchObject({
      dataType: "text",
      nativeFilterType: "query_arg",
      nativeFilterRequired: false,
      requiredBySteward: true,
    });
    expect(input.columns.map((c: { name: string }) => c.name)).toContain("title");
  });
});
