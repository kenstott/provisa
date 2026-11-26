// Copyright (c) 2026 Kenneth Stott
// Canary: 9b2e6d41-7c3a-4f58-8e19-2a6c4d0f3b77
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2279: a JSON column's fake is a template, written in the code editor: checked by the server
// as typed, filled from the profile's proposal, and applied as written. Any other column keeps the
// list of kinds and methods.

import { beforeEach, describe, expect, it, vi } from "vitest";
import { act, fireEvent, render, screen, waitFor } from "../../../../test-utils/render";
import { editorOf, languageOf, typeInto } from "../../../../test-utils/codeEditor";

const api = vi.hoisted(() => ({
  fetchFakeCatalog: vi.fn(),
  checkColumnFake: vi.fn(),
  proposeFakes: vi.fn(),
}));
vi.mock("../../../../api/fakes", () => api);

import { ColumnFakeDialog } from "../ColumnFakeDialog";
import { holdsJson } from "../declaration";
import type { FakeCatalog } from "../../../../api/fakes";
import type { RegisteredTable, TableColumn } from "../../../../types/admin";

const CATALOG: FakeCatalog = {
  kinds: [],
  methods: [
    {
      name: "email",
      category: "internet",
      params: [],
      families: ["text"],
      stable: true,
      stableNeeds: [],
    },
  ],
  typeFamilies: { json: "other", varchar: "text" },
};

const TEMPLATE = '{"email": email(), "phone": phone_number()}';

function column(columnName: string, dataType: string, fake?: string): TableColumn {
  return {
    id: 1,
    columnName,
    visibleTo: [],
    masks: [],
    alias: null,
    computedSqlAlias: columnName,
    computedGqlAlias: columnName,
    description: null,
    dataType,
    nativeFilterType: null,
    isPrimaryKey: false,
    isForeignKey: false,
    isAlternateKey: false,
    scope: "domain",
    isImplicitMeasure: false,
    isImplicitDimension: false,
    fake,
  } satisfies TableColumn;
}

const TABLE = { id: 4, tableName: "people", columns: [] } as unknown as RegisteredTable;

function openDialog(col: TableColumn, onSave = vi.fn()) {
  render(
    <ColumnFakeDialog
      table={TABLE}
      column={col}
      catalog={CATALOG}
      onClose={vi.fn()}
      onSave={onSave}
    />,
  );
  return onSave;
}

beforeEach(() => {
  api.checkColumnFake.mockReset().mockResolvedValue({ ok: true });
  api.proposeFakes.mockReset().mockResolvedValue({ runId: "r1", columns: {}, unmatchedPii: [] });
});

describe("a JSON column's template fake (REQ-2279)", () => {
  it("knows a JSON column by its type", () => {
    expect(holdsJson("json")).toBe(true);
    expect(holdsJson("JSONB")).toBe(true);
    expect(holdsJson("varchar")).toBe(false);
    expect(holdsJson(null)).toBe(false);
  });

  it("writes the template in the code editor, checks it as typed and applies it", async () => {
    const onSave = openDialog(column("contact", "json"));
    expect(screen.queryByTestId("testdata-dialog-fake-pick")).toBeNull();
    const view = editorOf("testdata-dialog-template");
    expect(languageOf(view)).toBe("json");
    act(() => typeInto(view, TEMPLATE));
    await waitFor(() =>
      expect(api.checkColumnFake).toHaveBeenLastCalledWith(
        expect.objectContaining({ column: "contact", dataType: "json", fake: TEMPLATE }),
      ),
    );
    fireEvent.click(screen.getByTestId("testdata-dialog-stable"));
    await waitFor(() => expect(screen.getByTestId("testdata-dialog-save")).toBeEnabled());
    fireEvent.click(screen.getByTestId("testdata-dialog-save"));
    expect(onSave).toHaveBeenCalledWith(TEMPLATE, "", true);
  });

  it("shows the server's refusal of a field by name and holds Apply", async () => {
    api.checkColumnFake.mockRejectedValue(new Error("field 'tags': pattern() cannot fake a field"));
    openDialog(column("contact", "json", '{"tags": pattern()}'));
    expect(await screen.findByTestId("testdata-dialog-refusal")).toHaveTextContent(
      "field 'tags': pattern() cannot fake a field",
    );
    expect(screen.getByTestId("testdata-dialog-save")).toBeDisabled();
  });

  it("fills the editor with the profile's proposed template", async () => {
    api.proposeFakes.mockResolvedValue({
      runId: "r1",
      columns: { contact: { fake: TEMPLATE } },
      unmatchedPii: [],
    });
    openDialog(column("contact", "json"));
    fireEvent.click(screen.getByTestId("testdata-dialog-fill"));
    await waitFor(() =>
      expect(editorOf("testdata-dialog-template").state.doc.toString()).toBe(TEMPLATE),
    );
  });

  it("keeps the list of kinds and methods for a column that is not JSON", () => {
    openDialog(column("email", "varchar"));
    expect(screen.queryByTestId("testdata-dialog-template")).toBeNull();
    expect(screen.getByTestId("testdata-dialog-fake-pick")).toBeInTheDocument();
  });
});
