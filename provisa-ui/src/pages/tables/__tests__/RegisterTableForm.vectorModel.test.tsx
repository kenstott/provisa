// Copyright (c) 2026 Kenneth Stott
// Canary: 64bc1451-27a2-41c9-b79f-18b6894b0584
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

/**
 * REQ-2000: a Qdrant collection's vector column names the registered model it is an embedding
 * of, chosen from the models of the vector's size; naming it registers the vector. REQ-2234:
 * Register Table offers such a collection but it cannot be picked, naming its vectors; its
 * Register opens it unsaved in the table's panel, whose Columns section names each vector's model,
 * as it does for a saved collection.
 */

import { describe, expect, it, vi } from "vitest";
import { fireEvent, render, screen, waitFor, within } from "../../../test-utils/render";
import { expectReason, held, openTables, registerAction } from "./registerFormHelpers";

const useVectorModels = vi.fn();

vi.mock("../../../hooks/useAdminQueries", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../../hooks/useAdminQueries")>()),
  useAvailableSchemas: () => ({ schemas: ["qdrant"], loading: false }),
  useAvailableTables: () => ({ tables: [{ name: "docs", comment: null }], loading: false }),
}));
vi.mock("../useVectorModels", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../useVectorModels")>()),
  useVectorModels: (...args: unknown[]) => useVectorModels(...args),
}));
const COLUMNS = [
  { name: "id", dataType: "text", path: "@id", isPrimaryKey: true },
  { name: "title", dataType: "text", path: "title" },
  { name: "embedding", dataType: "vector(384)", path: "@vector" },
  { name: "image", dataType: "vector(512)", path: "@vector.image" },
].map((c) => ({ comment: null, nativeFilterType: null, isPrimaryKey: false, ...c }));
const readColumns = async () => COLUMNS;
vi.mock("../../../hooks/useAvailableColumnsMetadataLazy", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../../hooks/useAvailableColumnsMetadataLazy")>()),
  useReadColumns: () => readColumns,
}));
vi.mock("../../../api/admin", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../../api/admin")>()),
  fetchTableUniqueConstraints: vi.fn().mockResolvedValue([]),
  fetchIrTypes: vi.fn().mockResolvedValue(["text"]),
}));

const { RegisterTableForm } = await import("../RegisterTableForm");
const { TableColumnsSection } = await import("../TableColumnsSection");
const { draftFromRegistration } = await import("../draftTable");
const { registrationInput } = await import("../useCreateTable");

const SOURCE = { id: "vec", type: "qdrant", database: "", allowedDomains: [] };

/** docs, opened on its own from its option, as it will be registered. */
async function openedOnItsOwn() {
  const onContinue = vi.fn();
  render(
    <RegisterTableForm
      sources={[SOURCE as never]}
      domainHints={["kb"]}
      domainAccess={["*"]}
      checkedDomains={new Set<string>()}
      domainsEnabled
      tables={[]}
      roles={[{ id: "analyst" } as never]}
      suggestTableAlias={vi.fn().mockResolvedValue("")}
      onContinue={onContinue}
      onCancel={() => {}}
      onProductsRegistered={vi.fn()}
      onRegisteredMany={vi.fn()}
      setError={vi.fn()}
      initialSourceId="vec"
    />,
  );
  const user = await openTables();
  await waitFor(() => expect(registerAction("docs")).not.toBeNull());
  return { user, onContinue };
}

/** The Columns section of a Qdrant collection's table. */
function renderColumns(table: ReturnType<typeof draftFromRegistration>) {
  const updateEditCol = vi.fn();
  render(
    <TableColumnsSection
      editingTable={table}
      updateEditCol={updateEditCol}
      editSource={SOURCE as never}
      roles={[]}
      hidingOnly={false}
      writeGrants={null}
    />,
  );
  return updateEditCol;
}

/** Picks the vector's one model offered. */
function nameModel(vectors: HTMLElement, column: string) {
  const input = within(vectors).getByTestId(`vector-model-${column}`);
  fireEvent.click(input);
  // Mantine keeps the drop-down out of the accessibility tree in jsdom: reach it through the
  // input's aria-controls listbox.
  const listbox = document.getElementById(input.getAttribute("aria-controls") as string);
  fireEvent.click(within(listbox as HTMLElement).getByRole("option", { hidden: true }));
}

describe("a Qdrant collection's vector models (REQ-2000)", () => {
  it("are needed: the collection cannot be picked, and names its vectors", async () => {
    useVectorModels.mockReturnValue({ "": ["minilm"], image: [] });
    const { user } = await openedOnItsOwn();
    expect(held("docs")).toBe(true);
    await expectReason(
      user,
      "docs",
      "Needs input: name the model of each vector (embedding, image). Register it on its own.",
    );
  });

  it("are named in the panel of the collection opened on its own, and registered", async () => {
    useVectorModels.mockReturnValue({ "": ["minilm"], image: [] });
    const { user, onContinue } = await openedOnItsOwn();
    await user.click(registerAction("docs") as HTMLElement);
    await waitFor(() => expect(onContinue).toHaveBeenCalledTimes(1));
    const registration = onContinue.mock.calls[0][0];
    const draft = draftFromRegistration(registration);
    // The vectors go with it, without a model yet.
    expect(draft.columns.map((c) => c.columnName)).toEqual(["id", "title", "embedding", "image"]);

    document.body.innerHTML = "";
    const updateEditCol = renderColumns(draft);
    const vectors = screen.getByTestId("register-table-vector-models");
    expect(useVectorModels).toHaveBeenLastCalledWith("vec", "docs", true);
    // Only vector columns are asked; one with no model of its size says so.
    expect(within(vectors).queryByText("title")).toBeNull();
    expect(within(vectors).getByTestId("vector-model-none-image")).toBeInTheDocument();
    nameModel(vectors, "embedding");
    expect(updateEditCol).toHaveBeenLastCalledWith(2, "embeddingModel", "minilm");

    const named = {
      ...draft,
      columns: draft.columns.map((c) =>
        c.columnName === "embedding" ? { ...c, embeddingModel: "minilm" } : c,
      ),
    };
    const sent = registrationInput(named, registration, null).columns as { name: string }[];
    expect(sent.find((c) => c.name === "embedding")).toMatchObject({
      embedding: true,
      embeddingModel: "minilm",
      path: "@vector",
    });
    // A vector left without a model stays unregistered, as offered.
    expect(sent.find((c) => c.name === "image")).toBeUndefined();
  });

  it("are named in the panel of a saved collection", () => {
    useVectorModels.mockReturnValue({ "": ["minilm"] });
    const saved = {
      ...draftFromRegistration({
        input: {
          sourceId: "vec",
          domainId: "kb",
          schemaName: "default",
          tableName: "docs",
          columns: [{ name: "embedding", dataType: "vector(384)", path: "@vector" }],
        },
        pagination: null,
        pagingKind: null,
        pagingCeilingRows: null,
        rowRules: [],
      }),
      id: 7,
      draft: false,
    };
    const updateEditCol = renderColumns(saved);
    nameModel(screen.getByTestId("register-table-vector-models"), "embedding");
    expect(updateEditCol).toHaveBeenLastCalledWith(0, "embeddingModel", "minilm");
  });
});
