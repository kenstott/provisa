// Copyright (c) 2026 Kenneth Stott
// Canary: 3ea66092-fd64-43d6-94c7-cf3421abc071
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1494: the table editor's column list switches between its metadata and its test data --
// each column's fake, synthetic rule and stable flag, edited in the cell or in the column dialog,
// whose declaration the server checks as it is typed -- and sends them with the table's columns.

import { beforeEach, describe, it, expect, vi } from "vitest";
import { fireEvent, render, screen, waitFor } from "../../../test-utils/render";

const api = vi.hoisted(() => ({
  fetchFakeCatalog: vi.fn(),
  checkColumnFake: vi.fn(),
  proposeFakes: vi.fn(),
}));
vi.mock("../../../api/fakes", () => api);

import { TableEditForm } from "../TableEditForm";
import { buildTableUpdateInput } from "../helpers";
import type { RegisteredTable, Source, TableColumn } from "../../../types/admin";

function makeTable(overrides: Partial<RegisteredTable> = {}): RegisteredTable {
  return {
    id: 2,
    sourceId: "wh",
    origin: "admin",
    domainId: "sales",
    schemaName: "quality",
    tableName: "orders_scans",
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
    columnPresets: [],
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
    uniqueConstraints: [],
    graphqlFieldName: null,
    dqDataset: null,
    ...overrides,
  };
}

function renderForm(table: RegisteredTable, sources: Source[], updateEditCol = vi.fn()) {
  render(
    <TableEditForm
      savedProfilerId={null}
      editingTable={table}
      setEditingTable={vi.fn()}
      editingColumnTypes={{}}
      cacheTtlEdits={{}}
      setCacheTtlEdits={vi.fn()}
      sources={sources}
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
    />,
  );
}

const SOURCE = { id: "wh", type: "postgresql" } as unknown as Source;

const EMAIL = {
  id: 1,
  columnName: "email",
  visibleTo: [],
  writableBy: [],
  unmaskedTo: [],
  maskType: null,
  maskPattern: null,
  maskReplace: null,
  maskValue: null,
  maskPrecision: null,
  alias: null,
  computedSqlAlias: "email",
  computedGqlAlias: "email",
  description: null,
  dataType: "varchar",
  nativeFilterType: null,
  isPrimaryKey: false,
  isForeignKey: false,
  isAlternateKey: false,
  scope: "domain",
  isImplicitMeasure: false,
  isImplicitDimension: false,
} satisfies TableColumn;

const CATALOG = {
  kinds: [
    {
      category: "values",
      name: "bool",
      positional: true,
      ruleOnly: false,
      args: [{ name: "share", kind: "number", required: false }],
    },
    {
      category: "rule",
      name: "sql_group",
      positional: true,
      ruleOnly: true,
      args: [{ name: "expression", kind: "expression", required: true }],
    },
  ],
  methods: [
    { name: "email", category: "internet", params: [], stable: true },
    {
      name: "pyint",
      category: "python",
      params: [
        { name: "min_value", required: false, default: "0" },
        { name: "max_value", required: false, default: "9999" },
      ],
      stable: false,
    },
  ],
};

beforeEach(() => {
  api.fetchFakeCatalog.mockReset().mockResolvedValue(CATALOG);
  api.checkColumnFake.mockReset().mockResolvedValue({ ok: true });
  api.proposeFakes.mockReset().mockResolvedValue({
    runId: "r1",
    columns: { email: { fake: "email()" }, tier: { syntheticRule: "categories()" } },
    unmatchedPii: ["secret"],
  });
});

function testDataMode() {
  fireEvent.click(screen.getByText("Test data"));
}

describe("TableEditForm — a column's test data (REQ-1494)", () => {
  it("offers metadata and test-data modes, the fake edited in its cell", () => {
    const updateEditCol = vi.fn();
    renderForm(makeTable({ columns: [{ ...EMAIL, fake: "email()" }] }), [SOURCE], updateEditCol);
    expect(screen.getByTestId("table-columns-mode")).toHaveAttribute(
      "data-tour",
      "table-columns-mode",
    );
    expect(screen.queryByTestId("testdata-columns")).toBeNull();
    testDataMode();
    const input = screen.getByTestId("testdata-fake-email") as HTMLInputElement;
    expect(input.value).toBe("email()");
    fireEvent.change(input, { target: { value: "categories((a, b))" } });
    expect(updateEditCol).toHaveBeenCalledWith(0, "fake", "categories((a, b))");
    fireEvent.change(screen.getByTestId("testdata-rule-email"), {
      target: { value: "sql_group(1)" },
    });
    expect(updateEditCol).toHaveBeenCalledWith(0, "syntheticRule", "sql_group(1)");
    fireEvent.click(screen.getByTestId("testdata-stable-email"));
    expect(updateEditCol).toHaveBeenCalledWith(0, "fakeStable", true);
  });

  it("marks a pii column that declares no fake", () => {
    renderForm(makeTable({ columns: [{ ...EMAIL, isPii: true }] }), [SOURCE]);
    testDataMode();
    expect(screen.getByTestId("testdata-pii-email")).toBeInTheDocument();
  });

  it("picks a method in the dialog, shows the server's refusal, and applies what it allows", async () => {
    const updateEditCol = vi.fn();
    renderForm(makeTable({ columns: [{ ...EMAIL }] }), [SOURCE], updateEditCol);
    testDataMode();
    await waitFor(() => expect(api.fetchFakeCatalog).toHaveBeenCalled());
    const [open] = screen.getAllByTestId("testdata-open-email");
    await waitFor(() => expect(open).not.toBeDisabled());
    fireEvent.click(open);
    api.checkColumnFake.mockRejectedValueOnce(new Error("orders.email: emial() is no fake"));
    fireEvent.change(screen.getByTestId("testdata-dialog-fake-text"), {
      target: { value: "emial()" },
    });
    expect(await screen.findByTestId("testdata-dialog-refusal")).toHaveTextContent(
      "emial() is no fake",
    );
    expect(screen.getByTestId("testdata-dialog-save")).toBeDisabled();
    fireEvent.change(screen.getByTestId("testdata-dialog-fake-text"), {
      target: { value: "email()" },
    });
    await waitFor(() => expect(screen.queryByTestId("testdata-dialog-refusal")).toBeNull());
    expect(api.checkColumnFake).toHaveBeenLastCalledWith(
      expect.objectContaining({
        column: "email",
        fake: "email()",
        syntheticRule: null,
        stable: false,
      }),
    );
    fireEvent.click(screen.getByTestId("testdata-dialog-save"));
    expect(updateEditCol).toHaveBeenCalledWith(0, "fake", "email()");
    expect(updateEditCol).toHaveBeenCalledWith(0, "syntheticRule", "");
    expect(updateEditCol).toHaveBeenCalledWith(0, "fakeStable", false);
  });

  it("sends the fake and synthetic rule with the column, and none when blank", () => {
    const [faked] = buildTableUpdateInput(
      makeTable({
        columns: [{ ...EMAIL, fake: " email() ", fakeStable: true, syntheticRule: " bool(.5) " }],
      }),
    ).columns as Record<string, unknown>[];
    expect(faked).toMatchObject({ fake: "email()", fakeStable: true, syntheticRule: "bool(.5)" });
    const [plain] = buildTableUpdateInput(makeTable({ columns: [{ ...EMAIL, fake: "  " }] }))
      .columns as Record<string, unknown>[];
    expect(plain.fake).toBeUndefined();
    expect(plain.fakeStable).toBeUndefined();
    expect(plain.syntheticRule).toBeUndefined();
  });

  it("fills from profile only the columns that declare nothing, and names unmatched pii", async () => {
    const updateEditCol = vi.fn();
    const columns = [
      { ...EMAIL },
      { ...EMAIL, id: 2, columnName: "tier", syntheticRule: "bool()" },
    ];
    renderForm(makeTable({ columns }), [SOURCE], updateEditCol);
    testDataMode();
    expect(screen.getByTestId("testdata-fill")).toHaveAttribute("data-tour", "fill-from-profile");
    fireEvent.click(screen.getByTestId("testdata-fill"));
    expect(await screen.findByTestId("testdata-filled")).toHaveTextContent("secret");
    expect(api.proposeFakes).toHaveBeenCalledWith(2);
    expect(updateEditCol).toHaveBeenCalledWith(0, "fake", "email()");
    expect(updateEditCol).not.toHaveBeenCalledWith(1, "syntheticRule", "categories()");
  });

  it("fills one column's dialog from profile", async () => {
    renderForm(makeTable({ columns: [{ ...EMAIL }] }), [SOURCE]);
    testDataMode();
    const [open] = screen.getAllByTestId("testdata-open-email");
    await waitFor(() => expect(open).not.toBeDisabled());
    fireEvent.click(open);
    fireEvent.click(screen.getByTestId("testdata-dialog-fill"));
    await waitFor(() =>
      expect((screen.getByTestId("testdata-dialog-fake-text") as HTMLInputElement).value).toBe(
        "email()",
      ),
    );
  });
});
