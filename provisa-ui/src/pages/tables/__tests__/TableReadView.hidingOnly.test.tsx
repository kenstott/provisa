// Copyright (c) 2026 Kenneth Stott
// Canary: 7c3f2a95-1e6d-4b80-a4c9-d52e8f10b6a3
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1944: a governance-only viewer of a table opens its editor (how its columns are hidden) and
// previews it, but is not offered the table editor's acts -- deploy, profile, delete.

import { describe, it, expect, vi } from "vitest";
import userEvent from "@testing-library/user-event";
import { fireEvent, render, screen, within } from "../../../test-utils/render";
// The glossary terms that name the table's columns, as the server answers: one rooted term, and an
// abstract one reachable from it (which names no column).
// The table's class term reads the meta domain as the user: its own tests (termGrounding) hold it.
vi.mock("../TableClassTerm", () => ({ TableClassTerm: () => <span>class term</span> }));
vi.mock("../../../api/glossary", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../../api/glossary")>()),
  fetchRelatedGlossaryTerms: vi.fn(async () => [
    { id: 1, name: "Customer", definition: null, is_abstract: false, live: true, domains: [] },
    { id: 2, name: "Revenue", definition: null, is_abstract: true, live: true, domains: [] },
  ]),
}));
import { TableReadView } from "../TableReadView";
import type { Relationship } from "../../../types/admin";
import type { RegisteredTable } from "../../../types/admin";

function makeTable(overrides: Partial<RegisteredTable> = {}): RegisteredTable {
  return {
    id: 2,
    sourceId: "wh",
    domainId: "sales",
    schemaName: "public",
    tableName: "orders",
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
    businessKey: [],
    validTime: null,
    recordedTime: null,
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

const TABLE = makeTable({ id: 7, sourceId: "pg", canDeployToDb: true });

function renderView(hidingOnly: boolean, shown: RegisteredTable = TABLE, acts = {}) {
  render(
    <TableReadView
      t={shown}
      hidingOnly={hidingOnly}
      dataProducts={[]}
      navigate={vi.fn()}
      viewsOnly={false}
      deploying={{}}
      setDeploying={vi.fn()}
      deployMsg={{}}
      setDeployMsg={vi.fn()}
      tableProfiles={{}}
      deployViewToDb={vi.fn()}
      reload={vi.fn()}
      startEditing={vi.fn()}
      handleDelete={vi.fn()}
      handleProfile={vi.fn()}
      onPreview={vi.fn()}
      relationships={[]}
      {...acts}
    />,
  );
}

describe("TableReadView for a governance-only viewer (REQ-1944)", () => {
  it("offers the editor and the preview, not deploy, profile or delete", () => {
    renderView(true);
    expect(screen.getByTestId("table-read-view-edit")).toBeInTheDocument();
    expect(screen.getByTestId("table-read-view-preview")).toBeInTheDocument();
    expect(screen.queryByTestId("table-read-view-deploy")).not.toBeInTheDocument();
    expect(screen.queryByTestId("table-read-view-profile")).not.toBeInTheDocument();
    expect(screen.queryByTestId("table-read-view-delete")).not.toBeInTheDocument();
  });

  it("offers a table editor every act", () => {
    renderView(false);
    expect(screen.getByTestId("table-read-view-deploy")).toBeInTheDocument();
    expect(screen.getByTestId("table-read-view-profile")).toBeInTheDocument();
    expect(screen.getByTestId("table-read-view-delete")).toBeInTheDocument();
  });
});

// #204: a table with a required parameter keeps both buttons. Each hands the table on to what
// asks for the required values first (the preview's own form; the dialog Profile opens).
describe("TableReadView for a table with a required parameter", () => {
  const argument = {
    id: 1,
    columnName: "_nf_owner",
    visibleTo: [],
    masks: [],
    scope: "domain",
    nativeFilterType: "query_param",
    nativeFilterRequired: true,
  } as unknown as RegisteredTable["columns"][number];
  const needing = makeTable({ id: 9, sourceId: "gh", columns: [argument] });

  it("offers Preview and Profile for a remote GraphQL table's required argument", () => {
    const onPreview = vi.fn();
    const handleProfile = vi.fn();
    renderView(false, needing, { onPreview, handleProfile });
    fireEvent.click(screen.getByTestId("table-read-view-preview"));
    fireEvent.click(screen.getByTestId("table-read-view-profile"));
    expect(onPreview).toHaveBeenCalledWith(needing);
    expect(handleProfile).toHaveBeenCalledWith(9);
  });
});

// REQ-1921: the Draft box sits with the table's other actions. It shows the stored state and
// asks for the change; whoever may not release a table sees the state and cannot change it.
describe("TableReadView's Draft box", () => {
  it("is checked on a draft and asks to release it when unticked", () => {
    const onSetDraft = vi.fn();
    const draft = makeTable({ id: 7, draft: true });
    renderView(false, draft, { onSetDraft });
    const box = screen.getByTestId("table-read-view-draft");
    expect(box).toBeChecked();
    expect(screen.getByLabelText("Draft")).toBe(box);
    fireEvent.click(box);
    expect(onSetDraft).toHaveBeenCalledTimes(1);
    expect(onSetDraft).toHaveBeenCalledWith(draft, false);
    // Not optimistic: the box follows the stored state, which has not changed yet.
    expect(box).toBeChecked();
  });

  it("is unchecked on a table in service and asks to return it to draft when ticked", () => {
    const onSetDraft = vi.fn();
    const released = makeTable({ id: 7, draft: false });
    renderView(false, released, { onSetDraft });
    const box = screen.getByTestId("table-read-view-draft");
    expect(box).not.toBeChecked();
    fireEvent.click(box);
    expect(onSetDraft).toHaveBeenCalledWith(released, true);
  });

  it("shows the state and cannot be changed by a caller who may not register tables", async () => {
    const onSetDraft = vi.fn();
    renderView(true, makeTable({ id: 7, draft: true }), { onSetDraft });
    const box = screen.getByTestId("table-read-view-draft");
    expect(box).toBeChecked();
    expect(box).toBeDisabled();
    // As a person clicks: a disabled control takes no click (a dispatched event would).
    await userEvent.setup({ pointerEventsCheck: 0 }).click(box);
    expect(onSetDraft).not.toHaveBeenCalled();
    expect(box).toBeChecked();
  });

  it("says a draft's preview is a draft preview, and keeps Preview and Profile offered", () => {
    renderView(false, makeTable({ id: 7, draft: true }), { onSetDraft: vi.fn() });
    expect(screen.getByTestId("table-read-view-draft-note")).toHaveTextContent(
      "Draft preview: readers cannot see this table yet.",
    );
    expect(screen.getByTestId("table-read-view-preview")).toBeEnabled();
    expect(screen.getByTestId("table-read-view-profile")).toBeEnabled();
  });

  it("says nothing of drafts on a table in service", () => {
    renderView(false, makeTable({ id: 7, draft: false }), { onSetDraft: vi.fn() });
    expect(screen.queryByTestId("table-read-view-draft-note")).not.toBeInTheDocument();
  });
});

describe("TableReadView — a column's test data is the fourth tab", () => {
  const COLUMN = {
    id: 1,
    columnName: "email",
    visibleTo: [],
    masks: [],
    isPrimaryKey: false,
  };
  const withColumns = (...columns: object[]) =>
    makeTable({ id: 7, columns: columns as RegisteredTable["columns"] });

  it("shows the same four topics as the editor, the column's name first on each", () => {
    renderView(
      false,
      withColumns({ ...COLUMN, fake: "email()", syntheticRule: "sql_group(1)", fakeStable: true }),
    );
    fireEvent.click(screen.getByTestId("table-section-columns"));
    const topics = within(screen.getByRole("tablist", { name: "Column attributes" }));
    expect(topics.getAllByRole("tab").map((tab) => tab.textContent)).toEqual([
      "Naming",
      "Type and keys",
      "Access",
      "Testing",
    ]);
    fireEvent.click(screen.getByTestId("column-tab-testing"));
    const headers = screen.getAllByRole("columnheader").map((h) => h.textContent);
    expect(headers).toEqual(["Column", "Fake", "Synthetic rule", "Stable"]);
    expect(screen.getByTestId("read-fake-email")).toHaveTextContent("email()");
    expect(screen.getByTestId("read-rule-email")).toHaveTextContent("sql_group(1)");
    expect(
      screen.getByTestId("read-fake-email").closest("tr")!.querySelector("td"),
    ).toHaveTextContent("email");
  });

  it("says a column with no fake shows its real value, and marks a pii column with none", () => {
    renderView(false, withColumns({ ...COLUMN, isPii: true }));
    fireEvent.click(screen.getByTestId("table-section-columns"));
    fireEvent.click(screen.getByTestId("column-tab-testing"));
    expect(screen.getByTestId("read-fake-email")).toHaveTextContent("real value");
    expect(screen.getByTestId("read-fake-email")).toHaveTextContent("pii, no fake");
  });

  it("is not offered to a caller who may change only how columns are hidden", () => {
    renderView(true, withColumns(COLUMN));
    fireEvent.click(screen.getByTestId("table-section-columns"));
    expect(screen.queryByTestId("column-tab-testing")).toBeNull();
    expect(screen.getByTestId("column-tab-access")).toBeInTheDocument();
  });
});

describe("TableReadView — the table's seven sections", () => {
  const rel = (id: number, source: string, target: string, sourceTableId: number) =>
    ({
      id,
      sourceTableId,
      targetTableId: sourceTableId === 7 ? 9 : 7,
      sourceTableName: source,
      targetTableName: target,
      sourceColumn: "customer_id",
      targetColumn: "id",
    }) as unknown as Relationship;

  it("shows the same seven sections as the edit form, Identity first", () => {
    renderView(false, makeTable({ id: 7 }));
    const sections = within(screen.getByRole("tablist", { name: "Table sections" }));
    expect(sections.getAllByRole("tab").map((tab) => tab.textContent)).toEqual([
      "Identity",
      "Associations",
      "Load and timeliness",
      "Query",
      "Quality",
      "Columns",
      "Writes",
    ]);
    expect(screen.getByTestId("table-identity")).toHaveTextContent("orders");
  });

  it("lists the relationships the table takes part in and the terms that name its columns", async () => {
    renderView(false, makeTable({ id: 7 }), {
      relationships: [rel(1, "orders", "customers", 7), rel(2, "returns", "orders", 9)],
    });
    fireEvent.click(screen.getByTestId("table-section-associations"));
    expect(screen.getByTestId("table-association-relationship-1")).toHaveTextContent(
      "orders.customer_id → customers.id",
    );
    expect(screen.getByTestId("table-association-relationship-2")).toHaveAttribute(
      "href",
      "/relationships?search=returns",
    );
    expect(await screen.findByText("Customer")).toBeInTheDocument();
    expect(screen.getByTestId("table-associations-terms")).not.toHaveTextContent("Revenue");
  });

  it("says when the table takes part in no relationship", () => {
    renderView(false, makeTable({ id: 7 }));
    fireEvent.click(screen.getByTestId("table-section-associations"));
    expect(screen.getByTestId("table-associations-relationships")).toHaveTextContent(
      "This table takes part in no relationship",
    );
  });

  it("goes to Quality when the table is profiled, where the profile lands", () => {
    renderView(false, makeTable({ id: 7 }), { handleProfile: vi.fn() });
    fireEvent.click(screen.getByTestId("table-read-view-profile"));
    expect(screen.getByTestId("table-section-quality")).toHaveAttribute("aria-selected", "true");
    expect(screen.getByTestId("table-quality")).toBeInTheDocument();
  });
});

describe("TableReadView — Invalidate Cache in the head", () => {
  it("is offered as an icon action, named by the label, when the page gives it", async () => {
    const onInvalidateCache = vi.fn();
    renderView(false, makeTable({ id: 7 }), { onInvalidateCache });
    const action = screen.getByRole("button", { name: "Invalidate Cache" });
    expect(action).toBe(screen.getByTestId("table-read-view-invalidate-cache"));
    fireEvent.click(action);
    expect(onInvalidateCache).toHaveBeenCalledWith(7);
  });

  it("is dimmed while the cache is being dropped", () => {
    renderView(false, makeTable({ id: 7 }), { onInvalidateCache: vi.fn(), invalidating: true });
    expect(screen.getByTestId("table-read-view-invalidate-cache")).toBeDisabled();
  });

  it("is not there for a table with no cache to drop", () => {
    renderView(false, makeTable({ id: 7 }));
    expect(screen.queryByTestId("table-read-view-invalidate-cache")).toBeNull();
  });
});

describe("TableReadView — Load and timeliness shows every stored setting", () => {
  it("lists paging, the off-peak window, incremental reload and live delivery, read-only", () => {
    renderView(
      false,
      makeTable({
        id: 7,
        pagingKind: "endpoint",
        pagination: {
          type: "offset",
          cursorField: null,
          cursorParam: null,
          pageParam: "offset",
          pageSizeParam: "limit",
          pageSize: 100,
          maxPages: 50,
          maxRows: null,
          rowsField: null,
        },
        loadProtected: true,
        offPeakWindow: "22:00-06:00",
        offPeakTz: "Europe/London",
        delta: {
          apply: "upsert",
          deletes: "none",
          tombstoneColumn: null,
          rebuildEvery: 86400,
          query: null,
        },
        live: {
          queryId: null,
          watermarkColumn: "updated_at",
          pollInterval: 30,
          strategy: "poll",
          kafka: null,
          outputs: [
            {
              type: "kafka",
              topic: "orders.changes",
              keyColumn: "id",
              bootstrapServers: "kafka:9092",
            },
          ],
        },
      } as Partial<RegisteredTable>),
    );
    fireEvent.click(screen.getByTestId("table-section-load"));
    const load = screen.getByTestId("table-load");
    for (const text of [
      "Offset and limit",
      "22:00",
      "06:00",
      "Europe/London",
      "86400",
      "updated_at",
      "orders.changes",
      "kafka:9092",
    ])
      expect(load).toHaveTextContent(text);
    // Nothing stored reads as the one empty mark, not as a missing row.
    expect(within(load).getByText("Rows are under").nextElementSibling).toHaveTextContent("—");
  });
});

describe("TableReadView — Preview, Profile and Policies are icon actions in the head", () => {
  it("names each by its word, shows no word, and keeps what it does", () => {
    const onPreview = vi.fn();
    const handleProfile = vi.fn();
    const navigate = vi.fn();
    renderView(false, makeTable({ id: 7 }), { onPreview, handleProfile, navigate });
    for (const [name, testId] of [
      ["Preview", "table-read-view-preview"],
      ["Profile", "table-read-view-profile"],
      ["Policies", "table-read-view-policies"],
    ]) {
      const action = screen.getByRole("button", { name });
      expect(action).toBe(screen.getByTestId(testId));
      expect(action).toHaveTextContent("");
    }
    fireEvent.click(screen.getByRole("button", { name: "Preview" }));
    fireEvent.click(screen.getByRole("button", { name: "Profile" }));
    fireEvent.click(screen.getByRole("button", { name: "Policies" }));
    expect(onPreview).toHaveBeenCalledTimes(1);
    expect(handleProfile).toHaveBeenCalledWith(7);
    expect(navigate).toHaveBeenCalledWith("/security/rls", { state: { tableFilter: "orders" } });
  });
});

describe("TableReadView — Query shows every stored setting", () => {
  it("lists the write grants on the Writes section (REQ-2074)", () => {
    renderView(
      false,
      makeTable({
        id: 7,
        writeGrants: [
          {
            role: "clerk",
            operation: "update",
            columns: ["status"],
            rows: "status = 'placed'",
            check: null,
            presets: [{ column: "updated_at", source: "now", name: null, value: null }],
          },
        ],
      } as Partial<RegisteredTable>),
    );
    fireEvent.click(screen.getByTestId("table-section-writes"));
    const grants = screen.getByTestId("write-grants-read");
    for (const text of [
      "clerk",
      "Update",
      "status = 'placed'",
      "updated_at to the time of the write",
    ])
      expect(grants).toHaveTextContent(text);
  });

  it("lists a metric view's parts", () => {
    renderView(
      false,
      makeTable({
        id: 7,
        viewSql: "SELECT 1",
        viewMetrics: { metrics: ["revenue"], dimensions: ["region"], filters: ["year = 2026"] },
      } as Partial<RegisteredTable>),
    );
    fireEvent.click(screen.getByTestId("table-section-query"));
    const query = screen.getByTestId("table-query");
    for (const text of ["revenue", "region", "year = 2026"]) expect(query).toHaveTextContent(text);
  });
});

// A graph's column links to its row: the Columns section opens on the table, the column's row
// marked and scrolled to.
describe("TableReadView opened on a column", () => {
  it("marks the named column's row and scrolls it into view", () => {
    const column = (name: string) =>
      ({
        id: name.length,
        columnName: name,
        visibleTo: [],
        masks: [],
        scope: "domain",
      }) as unknown as RegisteredTable["columns"][number];
    const scrolled = vi.fn();
    Element.prototype.scrollIntoView = scrolled;
    renderView(false, makeTable({ id: 7, columns: [column("id"), column("email")] }), {
      // The page holds the section: a link sets it to Columns.
      section: "columns",
      onSectionChange: vi.fn(),
      column: "email",
    });
    const row = screen.getByTestId("table-read-column-email");
    expect(row).toHaveAttribute("data-linked", "true");
    expect(row).toHaveClass("row-selected");
    expect(screen.getByTestId("table-read-column-id")).not.toHaveAttribute("data-linked");
    expect(scrolled).toHaveBeenCalledWith({ block: "center" });
    expect(scrolled.mock.contexts[0]).toBe(row);
  });
});
