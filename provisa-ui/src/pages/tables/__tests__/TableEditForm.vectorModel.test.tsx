// Copyright (c) 2026 Kenneth Stott
// Canary: f281079f-1066-4d30-a025-01d9d251d384
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2000: the table editor of a Qdrant collection lets a vector column name the registered model
// it is an embedding of, chosen from the models of the vector's size, and saves it.

import { describe, it, expect, vi } from "vitest";
import { fireEvent, render, screen, waitFor, within } from "../../../test-utils/render";

vi.mock("../../../api/fakes", () => ({
  fetchFakeCatalog: vi.fn().mockResolvedValue({ kinds: [], methods: [] }),
  checkColumnFake: vi.fn(),
  proposeFakes: vi.fn(),
}));
const useVectorModels = vi.fn();
vi.mock("../useVectorModels", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../useVectorModels")>()),
  useVectorModels: (...args: unknown[]) => useVectorModels(...args),
}));

import { TableEditForm } from "../TableEditForm";
import { buildTableUpdateInput } from "../helpers";
import type { RegisteredTable, Source, TableColumn } from "../../../types/admin";

function makeTable(overrides: Partial<RegisteredTable> = {}): RegisteredTable {
  return {
    id: 2,
    sourceId: "vec",
    domainId: "sales",
    schemaName: "qdrant",
    tableName: "docs",
    alias: null,
    description: null,
    cacheTtl: null,
    roleTtl: [],
    pagingKind: null,
    pagination: null,
    pagingCeilingRows: null,
    replicate: null,
    region: null,
    draft: false,
    loadProtected: null,
    offPeakWindow: null,
    offPeakTz: null,
    refreshPolicySummary: null,
    gqlNamingConvention: null,
    watermarkColumn: null,
    changeSignal: null,
    probeQuery: null,
    probeType: null,
    columns: [],
    implicitMeasures: [],
    implicitDimensions: [],
    apiEndpoint: null,
    viewSql: null,
    dqContract: null,
    materialize: false,
    mvRefreshInterval: 300,
    mvDebounceQuiet: 0,
    mvDebounceMaxDelay: 5,
    pushDebounceQuiet: 0,
    pushDebounceMaxDelay: 5,
    mvConsistency: "shared",
    mvPreprocess: null,
    mvBitemporalMode: null,
    mvBitemporalKey: [],
    mvPersist: "replace",
    mvPrimaryKey: [],
    mvIncremental: false,
    mvCalendar: null,
    mvGrain: null,
    mvAllowedLateness: 0,
    mvExpectedEvents: null,
    mvBusinessDayGrain: false,
    productId: null,
    enableAggregates: false,
    enableGroupBy: false,
    canDeployToDb: false,
    writeOps: ["delete", "insert", "update"],
    live: null,
    writeGrants: [],
    uniqueConstraints: [],
    graphqlFieldName: null,
    dqDataset: null,
    qualityCheckers: [],
    ...overrides,
  };
}

function column(columnName: string, path: string, extra: Partial<TableColumn> = {}): TableColumn {
  return {
    id: columnName.length,
    columnName,
    visibleTo: [],
    masks: [],
    alias: null,
    computedSqlAlias: columnName,
    computedGqlAlias: columnName,
    description: null,
    dataType: "text",
    nativeFilterType: null,
    isPrimaryKey: false,
    isForeignKey: false,
    isAlternateKey: false,
    scope: "domain",
    isImplicitMeasure: false,
    isImplicitDimension: false,
    path,
    ...extra,
  };
}

const COLUMNS = [
  column("title", "title"),
  column("embedding", "@vector", { dataType: "vector(384)" }),
  column("image", "@vector.image", { dataType: "vector(512)" }),
];

function renderForm(updateEditCol = vi.fn()) {
  render(
    <TableEditForm
      hidingOnly={false}
      savedProfilerId={null}
      editingTable={makeTable({ columns: COLUMNS })}
      setEditingTable={vi.fn()}
      editingColumnTypes={{}}
      cacheTtlEdits={{}}
      setCacheTtlEdits={vi.fn()}
      sources={[{ id: "vec", type: "qdrant" } as unknown as Source]}
      roles={[]}
      dataProducts={[]}
      settings={null}
      saving={false}
      generatingDesc={false}
      setGeneratingDesc={vi.fn()}
      generatingColDesc={null}
      setGeneratingColDesc={vi.fn()}
      generateTableDescription={vi.fn()}
      generateColumnDescription={vi.fn()}
      cancelEditing={vi.fn()}
      handleSaveEdit={vi.fn()}
      updateEditCol={updateEditCol}
      relationships={[]}
    />,
  );
  return updateEditCol;
}

describe("TableEditForm on a Qdrant collection: a vector's model (REQ-2000)", () => {
  it("offers each vector the models of its size, and nothing for any other column", () => {
    useVectorModels.mockReturnValue({ "": ["minilm"], image: [] });
    const updateEditCol = renderForm();
    expect(useVectorModels).toHaveBeenLastCalledWith("vec", "docs", true);
    fireEvent.click(screen.getByTestId("table-section-columns"));
    fireEvent.click(screen.getByTestId("column-tab-type"));
    expect(screen.queryByTestId("vector-model-title")).toBeNull();
    expect(screen.queryByTestId("vector-model-none-title")).toBeNull();
    expect(screen.getByTestId("vector-model-none-image")).toBeInTheDocument();
    const input = screen.getByTestId("vector-model-embedding");
    fireEvent.click(input);
    const listbox = document.getElementById(input.getAttribute("aria-controls") as string);
    fireEvent.click(within(listbox as HTMLElement).getByRole("option", { hidden: true }));
    expect(updateEditCol).toHaveBeenCalledWith(1, "embeddingModel", "minilm");
  });

  it("saves a named model as an embedding of it, and a cleared one as none", () => {
    const saved = (embeddingModel: string | null) =>
      (
        buildTableUpdateInput(
          makeTable({ columns: [column("embedding", "@vector", { embeddingModel })] }),
        ).columns as Record<string, unknown>[]
      )[0];
    expect(saved("minilm")).toMatchObject({ embedding: true, embeddingModel: "minilm" });
    expect(saved("")).toMatchObject({ embedding: undefined, embeddingModel: undefined });
  });

  it("offers no Replicate choice on a live-only kind's table", async () => {
    useVectorModels.mockReturnValue({});
    const asked = vi.spyOn(globalThis, "fetch").mockImplementation(async (url) => {
      // REQ-2122: the editor also reads the table's catalogs; this table's domain has none.
      if (String(url).includes("/catalog")) {
        return new Response(JSON.stringify({ catalogs: [] }));
      }
      const kind = decodeURIComponent(String(url).split("/admin/source-kinds/")[1]);
      return new Response(
        JSON.stringify({
          cache_ttl_required: false,
          sentinel_path: false,
          tables_land: false,
          live_only: kind === "qdrant",
        }),
      );
    });
    renderForm();
    fireEvent.click(screen.getByTestId("table-section-load"));
    await waitFor(() => expect(asked).toHaveBeenCalled());
    await waitFor(() => expect(screen.queryByTestId("table-replicate-select")).toBeNull());
    expect(screen.getByTestId("load-management-panel")).toBeInTheDocument();
    asked.mockRestore();
  });
});
