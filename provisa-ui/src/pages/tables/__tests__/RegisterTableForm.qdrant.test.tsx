// Copyright (c) 2026 Kenneth Stott
// Canary: dccdd4e4-9450-4d8d-aefc-9136023fb075
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

/**
 * REQ-1983: registering a Qdrant search. The form takes the table's name, its search
 * ({"collection", "query": "@name", ...}) and its arguments, previews it with sample values, and
 * registers it under the source's one schema, the query's argument required (Qdrant requires a
 * query) and every other argument as the steward left it.
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
vi.mock("../QdrantVectorsNote", () => ({ QdrantVectorsNote: () => null }));

const { RegisterTableForm } = await import("../RegisterTableForm");

const SEARCH = JSON.stringify({
  collection: "docs",
  query: "@q",
  filter: { must: [{ key: "lang", match: { value: "@lang" } }] },
});

beforeEach(() => {
  vi.clearAllMocks();
  preview.mockResolvedValue({
    rows: [{ "@id": "a1", "@score": 0.9, title: "Dogs" }],
    columns: [
      { name: "id", dataType: "text", path: "@id" },
      { name: "score", dataType: "double", path: "@score" },
      { name: "title", dataType: "text", path: null },
    ],
    error: null,
  });
});

describe("RegisterTableForm on a Qdrant search (REQ-1983)", () => {
  it("previews the search with its arguments and registers its query as required", async () => {
    const onContinue = vi.fn();
    render(
      <RegisterTableForm
        sources={[{ id: "vec", type: "qdrant", database: "", allowedDomains: [] } as never]}
        domainHints={["kb"]}
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
    await user.selectOptions(screen.getByTestId("register-table-source-select"), "vec");
    await user.click(within(screen.getByTestId("register-table-qdrant-mode")).getByText("Search"));
    await user.selectOptions(screen.getByTestId("register-table-domain-select"), "kb");
    fireEvent.change(await screen.findByTestId("register-table-qdrant-table-name"), {
      target: { value: "docs_search" },
    });
    fireEvent.change(screen.getByTestId("register-table-qdrant-pipeline"), {
      target: { value: SEARCH },
    });
    for (const [i, name, sample] of [
      [0, "q", "[0.1, 0.2]"],
      [1, "lang", "en"],
    ] as const) {
      await user.click(screen.getByTestId("register-table-pipeline-arg-add"));
      const row = screen.getByTestId(`register-table-pipeline-arg-${i}`);
      fireEvent.change(within(row).getByLabelText("Parameter"), { target: { value: name } });
      fireEvent.change(within(row).getByLabelText("Sample value"), { target: { value: sample } });
    }
    await user.click(screen.getByTestId("register-table-qdrant-preview"));
    await waitFor(() => expect(preview).toHaveBeenCalledTimes(1));
    expect(preview.mock.calls[0][0]).toMatchObject({ sourceType: "qdrant", query: SEARCH });
    await waitFor(() =>
      expect(screen.getByTestId("register-table-col-selected-score")).toBeChecked(),
    );
    fireEvent.click(screen.getByTestId("register-table-submit"));
    await waitFor(() => expect(onContinue).toHaveBeenCalledTimes(1));
    const input = onContinue.mock.calls[0][0].input;
    expect(input).toMatchObject({
      schemaName: "default",
      tableName: "docs_search",
      queryTemplate: SEARCH,
    });
    const arg = (n: string) => input.columns.find((c: { name: string }) => c.name === `_nf_${n}`);
    expect(arg("q")).toMatchObject({ nativeFilterType: "query_arg", nativeFilterRequired: true });
    expect(arg("lang")).toMatchObject({
      nativeFilterType: "query_arg",
      nativeFilterRequired: false,
    });
  });
});
