// Copyright (c) 2026 Kenneth Stott
// Canary: 7b2e9f04-6c1d-4a53-8e7f-0d4a9c61b3e8
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1958: the table editor opens for a registrar who is not shown a column's grant lists and
// mask (no view_governance; the server answers them null). The form then carries no governance
// column, and its save names none of those fields, so the store keeps what it holds. A caller
// who is shown them edits and saves them as before.

import { describe, it, expect, vi } from "vitest";
import { render, screen } from "../../../test-utils/render";
import "../../../i18n";
import en from "../../../i18n/locales/en/tableEditForm.json";

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


const H = en.tableEditForm;
const GOVERNANCE_HEADERS = [H.visibleToHeader, H.writableByHeader, H.maskingHeader];
const GOVERNANCE_KEYS = [
  "writableBy",
  "unmaskedTo",
  "maskType",
  "maskPattern",
  "maskReplace",
  "maskValue",
  "maskPrecision",
];

const SHOWN = {
  ...EMAIL,
  visibleTo: ["analyst"],
  writableBy: ["steward"],
  unmaskedTo: ["auditor"],
  maskType: "constant",
  maskValue: "***",
} as TableColumn;

// What the server answers a caller without view_governance: null lists, null mask.
const WITHHELD = {
  ...EMAIL,
  visibleTo: null,
  writableBy: null,
  unmaskedTo: null,
} as unknown as TableColumn;

describe("the table editor for a caller who is shown governance", () => {
  it("has the grant-list and mask columns", () => {
    renderForm(makeTable({ columns: [SHOWN] }), [SOURCE]);
    for (const header of GOVERNANCE_HEADERS) {
      expect(screen.getByRole("columnheader", { name: header })).toBeInTheDocument();
    }
  });

  it("saves the grant lists and mask it holds", () => {
    const input = buildTableUpdateInput(makeTable({ columns: [SHOWN] }));
    const [column] = input.columns as Record<string, unknown>[];
    expect(column).toMatchObject({
      name: "email",
      visibleTo: ["analyst"],
      writableBy: ["steward"],
      unmaskedTo: ["auditor"],
      maskType: "constant",
      maskValue: "***",
    });
  });
});

describe("the table editor for a caller who is not shown governance", () => {
  it("opens, without the grant-list and mask columns", () => {
    renderForm(makeTable({ columns: [WITHHELD] }), [SOURCE]);
    for (const header of GOVERNANCE_HEADERS) {
      expect(screen.queryByRole("columnheader", { name: header })).not.toBeInTheDocument();
    }
    // The rest of the column editor is there.
    expect(screen.getByRole("columnheader", { name: H.scopeHeader })).toBeInTheDocument();
    expect(screen.getByRole("columnheader", { name: H.descriptionHeader })).toBeInTheDocument();
  });

  it("saves a column without naming a grant list or mask", () => {
    const input = buildTableUpdateInput(makeTable({ columns: [WITHHELD] }));
    const [column] = input.columns as Record<string, unknown>[];
    // The input type requires visibleTo; an empty list is "not set by this save", which the
    // server fills from the store. Nothing else of governance is in the payload at all.
    expect(column.visibleTo).toEqual([]);
    for (const key of GOVERNANCE_KEYS) {
      expect(column).not.toHaveProperty(key);
    }
    expect(column.name).toBe("email");
    expect(column.scope).toBe("domain");
  });
});
