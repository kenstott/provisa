// Copyright (c) 2026 Kenneth Stott
// Canary: 306c85f9-3c71-4749-b7fe-2893d6f852be
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

/** REQ-1683: registering a table on a sparql source takes the Neo4j-shaped path with a SPARQL editor. */

import { beforeEach, describe, expect, it, vi } from "vitest";
import userEvent from "@testing-library/user-event";
import { fireEvent, render, screen, waitFor } from "../../../test-utils/render";

const preview = vi.fn();

vi.mock("../../../hooks/useAdminQueries", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../../hooks/useAdminQueries")>()),
  useAvailableSchemas: () => ({ schemas: [] as string[], loading: false }),
  useAvailableTables: () => ({ tables: [] as { name: string }[], loading: false }),
}));
vi.mock("../../../hooks/useQueryPreview", () => ({
  useQueryPreview: () => ({ preview }),
}));
// REQ-1923 (amended 2026-11-20): the columns are read from the query's SELECT by the server.
const fetchSparqlColumns = vi.hoisted(() => vi.fn());
vi.mock("../../../api/wikidata", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../../api/wikidata")>()),
  fetchSparqlColumns,
}));

const { RegisterTableForm } = await import("../RegisterTableForm");

const QUERY = "SELECT ?volunteer_id ?name WHERE { ?v <urn:id> ?volunteer_id ; <urn:name> ?name }";

beforeEach(() => {
  vi.clearAllMocks();
  fetchSparqlColumns.mockResolvedValue([
    { name: "volunteer_id", data_type: "text" },
    { name: "name", data_type: "text" },
  ]);
  preview.mockResolvedValue({
    rows: [{ volunteer_id: "V-01", name: "Grace" }],
    columns: [
      { name: "volunteer_id", dataType: "text" },
      { name: "name", dataType: "text" },
    ],
    error: null,
  });
});

function renderForm(onContinue: (r: unknown) => void, setError = vi.fn()) {
  return render(
    <RegisterTableForm
      sources={[{ id: "sparql-demo", type: "sparql", allowedDomains: [] } as never]}
      domainHints={["shelter"]}
      domainAccess={["*"]}
      checkedDomains={new Set<string>()}
      domainsEnabled
      tables={[]}
      roles={[{ id: "org_admin" } as never]}
      suggestTableAlias={vi.fn().mockResolvedValue("")}
      onContinue={onContinue}
      onCancel={() => {}}
      onProductsRegistered={vi.fn()}
      onRegisteredMany={vi.fn()}
      setError={setError}
    />,
  );
}

async function fillQuery(query: string) {
  const user = userEvent.setup();
  await user.selectOptions(screen.getByTestId("register-table-source-select"), "sparql-demo");
  await user.selectOptions(screen.getByTestId("register-table-domain-select"), "shelter");
  fireEvent.change(await screen.findByTestId("register-table-sparql-table-name"), {
    target: { value: "volunteer" },
  });
  fireEvent.change(screen.getByTestId("register-table-sparql-query"), {
    target: { value: query },
  });
  return user;
}

describe("RegisterTableForm on a sparql source (REQ-1683)", () => {
  it("registers without a preview, its columns read from the SELECT, every binding text", async () => {
    const onContinue = vi.fn();
    renderForm(onContinue);
    await fillQuery(QUERY);
    await waitFor(() => expect(fetchSparqlColumns).toHaveBeenCalledWith(QUERY));
    fireEvent.click(await screen.findByTestId("register-table-submit"));
    await waitFor(() => expect(onContinue).toHaveBeenCalledTimes(1));
    expect(preview).not.toHaveBeenCalled();
    const input = (onContinue.mock.calls[0][0] as { input: Record<string, unknown> }).input;
    expect(input).toMatchObject({ tableName: "volunteer", queryTemplate: QUERY });
    expect((input.columns as { name: string; dataType: string }[]).map((c) => c.name)).toEqual([
      "volunteer_id",
      "name",
    ]);
  });

  it("says by name that a preview answered no rows", async () => {
    preview.mockResolvedValue({ rows: [], columns: [], error: null });
    renderForm(vi.fn());
    const user = await fillQuery(QUERY);
    await user.click(screen.getByTestId("register-table-sparql-preview"));
    expect(await screen.findByTestId("register-table-sparql-preview-problem")).toHaveTextContent(
      "The preview answered no rows.",
    );
  });

  it("shows why a preview failed", async () => {
    preview.mockResolvedValue({
      rows: [],
      columns: [],
      error: "timed out waiting for a response from POST https://query.wikidata.org/sparql",
    });
    renderForm(vi.fn());
    const user = await fillQuery(QUERY);
    await user.click(screen.getByTestId("register-table-sparql-preview"));
    expect(await screen.findByTestId("register-table-sparql-preview-problem")).toHaveTextContent(
      "The preview failed: timed out waiting for a response from POST",
    );
  });

  it("names SPARQL, not Cypher, when the query is missing", async () => {
    const setError = vi.fn();
    renderForm(vi.fn(), setError);
    const user = userEvent.setup();
    await user.selectOptions(screen.getByTestId("register-table-source-select"), "sparql-demo");
    await user.selectOptions(screen.getByTestId("register-table-domain-select"), "shelter");
    fireEvent.change(await screen.findByTestId("register-table-sparql-table-name"), {
      target: { value: "volunteer" },
    });
    fireEvent.click(screen.getByTestId("register-table-submit"));
    expect(setError).toHaveBeenLastCalledWith(
      "Enter the SPARQL query that produces the table's rows.",
    );
  });

  it("previews the SELECT and registers its variables as text columns with the query", async () => {
    const onContinue = vi.fn();
    render(
      <RegisterTableForm
        sources={[{ id: "sparql-demo", type: "sparql", allowedDomains: [] } as never]}
        domainHints={["shelter"]}
        domainAccess={["*"]}
        checkedDomains={new Set<string>()}
        domainsEnabled
        tables={[]}
        roles={[{ id: "org_admin" } as never]}
        suggestTableAlias={vi.fn().mockResolvedValue("")}
        onContinue={onContinue}
        onCancel={() => {}}
        onProductsRegistered={vi.fn()}
        onRegisteredMany={vi.fn()}
        setError={vi.fn()}
      />,
    );
    const user = userEvent.setup();
    await user.selectOptions(screen.getByTestId("register-table-source-select"), "sparql-demo");
    await user.selectOptions(screen.getByTestId("register-table-domain-select"), "shelter");
    expect(screen.queryByTestId("register-table-schema-select")).not.toBeInTheDocument();
    fireEvent.change(await screen.findByTestId("register-table-sparql-table-name"), {
      target: { value: "volunteer" },
    });
    fireEvent.change(screen.getByTestId("register-table-sparql-query"), {
      target: { value: QUERY },
    });
    await user.click(screen.getByTestId("register-table-sparql-preview"));
    await waitFor(() =>
      expect(screen.getByTestId("register-table-sparql-preview-rows")).toBeInTheDocument(),
    );
    expect(preview).toHaveBeenCalledWith({
      sourceType: "sparql",
      sourceId: "sparql-demo",
      query: QUERY,
    });
    fireEvent.click(screen.getByTestId("register-table-submit"));
    await waitFor(() => expect(onContinue).toHaveBeenCalledTimes(1));
    expect(onContinue.mock.calls[0][0].input).toMatchObject({
      sourceId: "sparql-demo",
      schemaName: "sparql",
      tableName: "volunteer",
      queryTemplate: QUERY,
    });
  });
});
