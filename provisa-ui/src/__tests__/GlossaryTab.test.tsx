// Copyright (c) 2026 Kenneth Stott
// Canary: c4aca815-dfb5-4a23-b2c0-b946101d6150
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1387: admin business-glossary curation tab.

import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import { render, screen, fireEvent, waitFor, within } from "../test-utils/render";
import i18n from "../i18n";
import { GlossaryTab } from "../components/admin/GlossaryTab";
import type { GlossaryTermDetail, GlossaryTermSummary } from "../api/glossary";

const t = i18n.getFixedT("en");

vi.mock("../api/glossary", async (importOriginal) => ({
  // Spread the real module so the enum constants stay real; replace the fetchers.
  ...(await importOriginal<typeof import("../api/glossary")>()),
  listGlossaryTerms: vi.fn(),
  fetchGlossaryTerm: vi.fn(),
  createGlossaryTerm: vi.fn(),
  updateGlossaryTerm: vi.fn(),
  deleteGlossaryTerm: vi.fn(),
  moveGlossaryRef: vi.fn(),
  addGlossaryEdge: vi.fn(),
  removeGlossaryEdge: vi.fn(),
  retypeGlossaryEdge: vi.fn(),
  addGlossaryExpert: vi.fn(),
  removeGlossaryExpert: vi.fn(),
  generateGlossaryDefinition: vi.fn(),
  generateAllGlossaryDefinitions: vi.fn(),
  generateGlossaryRelationships: vi.fn(),
  listTermTables: vi.fn(async () => []),
  groundTermTable: vi.fn(),
  ungroundTermTable: vi.fn(),
  fetchGlossaryFindings: vi.fn(async () => []),
  retireGlossaryTerm: vi.fn(),
  restoreGlossaryTerm: vi.fn(),
  deprecateGlossaryTerm: vi.fn(),
  fetchAppliedHistory: vi.fn(),
}));

// A term's tag taken off from its To review finding.
const unassignTag = vi.hoisted(() => vi.fn(async () => ({ success: true, message: "" })));
vi.mock("../hooks/useTagQueries", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../hooks/useTagQueries")>()),
  useUnassignTag: () => ({ unassignTag, loading: false }),
}));
const navigateTo = vi.hoisted(() => vi.fn());
vi.mock("react-router-dom", async (importOriginal) => ({
  ...(await importOriginal<typeof import("react-router-dom")>()),
  useNavigate: () => navigateTo,
}));

// The term each column is, for Claim columns: read from the meta domain, here a fixture.
vi.mock("../components/admin/claimColumns", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../components/admin/claimColumns")>()),
  columnTerms: vi.fn(
    async () => new Map([["11:churn_flag", { id: 2, name: "Churn", draft: true }]]),
  ),
}));

// The registered tables Claim columns offers columns of, and Grounded in tables offers.
vi.mock("../hooks/useAdminQueries", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../hooks/useAdminQueries")>()),
  useTables: () => ({
    tables: [
      {
        id: 11,
        domainId: "sales",
        tableName: "events",
        draft: false,
        columns: [
          { columnName: "event_id", isPrimaryKey: true },
          { columnName: "churn_flag" },
          { columnName: "at" },
        ],
      },
      {
        id: 12,
        domainId: "sales",
        tableName: "alerts",
        draft: false,
        columns: [{ columnName: "event_ref" }, { columnName: "note" }],
      },
    ],
    loading: false,
    error: undefined,
    refetch: vi.fn(),
  }),
  // alerts.event_ref is declared many-to-one into events (11): a key of events', by declaration.
  useRelationships: () => ({
    relationships: [
      {
        sourceTableId: 12,
        sourceColumn: "event_ref",
        targetTableId: 11,
        cardinality: "many-to-one",
        viaTableId: null,
      },
    ],
    loading: false,
    error: undefined,
    refetch: vi.fn(),
  }),
}));

vi.mock("@mantine/notifications", () => ({ notifications: { show: vi.fn() } }));

// REQ-1590: every case below is a curator's, so the tab is mounted holding both glossary rights.
// What a holder of `glossary_read` alone is shown is glossaryReadOnly.test.tsx.
vi.mock("../context/AuthContext", () => ({
  useAuth: () => ({
    capabilities: ["glossary_read", "glossary_rw"],
    activeOrgId: "acme",
    actingRoles: "org_admin",
  }),
}));

// REQ-1592: the expert/author picker is fed by the org's roster, not by typing a user id.
vi.mock("../api/admin", () => ({
  fetchOrgMembers: vi.fn(async () => [
    {
      user_id: "u1",
      email: "ada@acme.test",
      display_name: "Ada Lovelace",
      provider: "local",
      is_org_admin: false,
    },
    {
      user_id: "u2",
      email: "grace@acme.test",
      display_name: "",
      provider: "local",
      is_org_admin: false,
    },
  ]),
}));

import {
  addGlossaryEdge,
  addGlossaryExpert,
  listTermTables,
  removeGlossaryEdge,
} from "../api/glossary";
import { notifications } from "@mantine/notifications";
import {
  deleteGlossaryTerm,
  fetchGlossaryTerm,
  generateAllGlossaryDefinitions,
  generateGlossaryDefinition,
  generateGlossaryRelationships,
  listGlossaryTerms,
  deprecateGlossaryTerm,
  fetchAppliedHistory,
  fetchGlossaryFindings,
  moveGlossaryRef,
  restoreGlossaryTerm,
  retireGlossaryTerm,
  retypeGlossaryEdge,
  TagLossRefused,
  updateGlossaryTerm,
} from "../api/glossary";

const mockRetire = vi.mocked(retireGlossaryTerm);
const mockFindings = vi.mocked(fetchGlossaryFindings);
const mockDeprecate = vi.mocked(deprecateGlossaryTerm);
const mockHistory = vi.mocked(fetchAppliedHistory);
const mockRestore = vi.mocked(restoreGlossaryTerm);
const mockList = vi.mocked(listGlossaryTerms);
const mockFetchTerm = vi.mocked(fetchGlossaryTerm);
const mockUpdate = vi.mocked(updateGlossaryTerm);
const mockDelete = vi.mocked(deleteGlossaryTerm);
const mockGenerate = vi.mocked(generateGlossaryDefinition);
const mockBulkDefinitions = vi.mocked(generateAllGlossaryDefinitions);
const mockBulkRelationships = vi.mocked(generateGlossaryRelationships);
const mockMoveRef = vi.mocked(moveGlossaryRef);
const mockRetypeEdge = vi.mocked(retypeGlossaryEdge);
const mockNotify = vi.mocked(notifications.show);

const REVENUE: GlossaryTermSummary = {
  id: 1,
  name: "Revenue",
  definition: "Money in.",
  is_abstract: false,
  deprecated: false,
  ref_count: 1,
  table_count: 0,
  identity_gap: false,
  findings: 0,
  status: "live",
  status_reason: null,
  status_by: null,
  retire_on: null,
  export_excluded: false,
  retired: false,
  live: true,
  grounded: true,
  domains: [],
};
const CHURN: GlossaryTermSummary = {
  id: 2,
  name: "Churn",
  definition: null,
  is_abstract: true,
  deprecated: false,
  ref_count: 0,
  table_count: 0,
  identity_gap: false,
  findings: 0,
  status: "live",
  status_reason: null,
  status_by: null,
  retire_on: null,
  export_excluded: false,
  retired: false,
  live: true,
  grounded: true,
  domains: [],
};
const MARGIN: GlossaryTermSummary = {
  id: 3,
  name: "Margin",
  definition: null,
  is_abstract: false,
  deprecated: true,
  ref_count: 0,
  table_count: 0,
  identity_gap: false,
  findings: 0,
  status: "deprecated",
  status_reason: null,
  status_by: null,
  retire_on: null,
  export_excluded: false,
  retired: false,
  live: true,
  grounded: true,
  domains: [],
};
// Abstract with no edge chain to a rooted term -- the dangling case (grounded: false).
const DANGLING: GlossaryTermSummary = {
  id: 4,
  name: "Orphan Concept",
  definition: "Declared but wired to nothing physical.",
  is_abstract: true,
  deprecated: false,
  ref_count: 0,
  table_count: 0,
  identity_gap: false,
  findings: 0,
  status: "draft",
  status_reason: null,
  status_by: null,
  retire_on: null,
  export_excluded: false,
  retired: false,
  live: false,
  grounded: false,
  domains: [],
};

const REVENUE_DETAIL: GlossaryTermDetail = {
  ...REVENUE,
  refs: [
    {
      table_id: 10,
      column_name: "revenue",
      source_id: "erp",
      schema_name: "public",
      table_name: "orders",
      alias: "Orders",
      domain_id: "sales",
    },
  ],
  edges_out: [{ term_id: 2, rel_type: "RELATED_TO", name: "Churn" }],
  edges_in: [],
  experts: [{ user_id: "u1", kind: "expert" }],
};

const CHURN_DETAIL: GlossaryTermDetail = {
  ...CHURN,
  refs: [],
  edges_out: [],
  edges_in: [{ term_id: 1, rel_type: "RELATED_TO", name: "Revenue" }],
  experts: [],
};

// No refs, no relationships: the one fixture where delete is actually enabled.
const MARGIN_DETAIL: GlossaryTermDetail = {
  ...MARGIN,
  refs: [],
  edges_out: [],
  edges_in: [],
  experts: [],
};

// Mantine Select in jsdom: floating-ui hides the detached dropdown (all rects are 0),
// so visible-only role queries miss the options. Scope by the input's aria-controls
// listbox and query with hidden: true.
async function openSelect(testid: string): Promise<HTMLElement> {
  const input = screen.getByTestId(testid);
  fireEvent.click(input);
  await waitFor(() => {
    if (!input.getAttribute("aria-controls")) throw new Error("dropdown not open");
  });
  return document.getElementById(input.getAttribute("aria-controls") as string) as HTMLElement;
}

/** Opens the selected term as one form. */
async function edit() {
  fireEvent.click(await screen.findByTestId("glossary-edit-btn"));
  return screen.findByTestId("glossary-edit-form");
}
const save = () => fireEvent.click(screen.getByTestId("glossary-edit-save"));

describe("GlossaryTab", () => {
  beforeEach(() => {
    mockFindings.mockResolvedValue([]);
    mockHistory.mockResolvedValue(null);
    mockList.mockReset();
    mockFetchTerm.mockReset();
    mockUpdate.mockReset();
    mockDelete.mockReset();
    mockBulkDefinitions.mockReset();
    mockBulkRelationships.mockReset();
    mockMoveRef.mockReset();
    mockRetypeEdge.mockReset();
    mockNotify.mockClear();
    vi.mocked(listTermTables).mockImplementation(async () => []);
    window.localStorage.clear();
    mockList.mockResolvedValue([REVENUE, CHURN, MARGIN]);
    mockFetchTerm.mockImplementation(async (id) => {
      if (id === 1) return REVENUE_DETAIL;
      if (id === 2) return CHURN_DETAIL;
      if (id === 3) return MARGIN_DETAIL;
      throw new Error(`no detail fixture for term ${id}`);
    });
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  it("renders the term list with role and deprecated badges and column counts", async () => {
    render(<GlossaryTab />);

    expect(await screen.findByTestId("glossary-item-1")).toBeInTheDocument();
    expect(mockList).toHaveBeenCalledWith("", true, null);

    const revenue = screen.getByTestId("glossary-item-1");
    expect(within(revenue).getByText("Revenue")).toBeInTheDocument();
    expect(within(revenue).getByText("1")).toBeInTheDocument();

    // A concept names no column: its role says so, and no count is shown.
    const churn = screen.getByTestId("glossary-item-2");
    expect(within(churn).getByTestId("glossary-role-2")).toHaveTextContent(
      t("glossaryTab.role_concept"),
    );
    expect(within(churn).queryByText("Abstract")).toBeNull();

    const margin = screen.getByTestId("glossary-item-3");
    expect(within(margin).getByTestId("glossary-status-3")).toHaveTextContent(
      t("glossaryTab.status_deprecated"),
    );
    // A live term is the ordinary case: no status badge.
    expect(within(revenue).queryByTestId("glossary-status-1")).toBeNull();
  });

  it("badges a draft term Draft in the list", async () => {
    mockList.mockResolvedValue([REVENUE, DANGLING]);
    render(<GlossaryTab />);
    expect(await screen.findByTestId("glossary-status-4")).toHaveTextContent(
      t("glossaryTab.status_draft"),
    );
  });

  it("deprecates a live term with a reason", async () => {
    render(<GlossaryTab />);
    fireEvent.click(await screen.findByTestId("glossary-item-1"));
    fireEvent.click(await screen.findByTestId("glossary-deprecate-btn"));
    const submit = await screen.findByTestId("glossary-deprecate-submit");
    expect(submit).toBeDisabled();
    fireEvent.change(screen.getByTestId("glossary-deprecate-reason"), {
      target: { value: "superseded by Net revenue" },
    });
    fireEvent.click(submit);
    await waitFor(() =>
      expect(mockDeprecate).toHaveBeenCalledWith(1, {
        reason: "superseded by Net revenue",
        retire_on: null,
      }),
    );
  });

  it("offers no Deprecate on a term that is not live", async () => {
    render(<GlossaryTab />);
    fireEvent.click(await screen.findByTestId("glossary-item-3"));
    expect(await screen.findByTestId("glossary-restore-btn")).toBeInTheDocument();
    expect(screen.queryByTestId("glossary-deprecate-btn")).toBeNull();
    expect(screen.getByTestId("glossary-retire-btn")).toBeInTheDocument();
  });

  it("shows a retired term's successor and what it last applied to", async () => {
    mockFetchTerm.mockImplementation(async (id) =>
      id === 1
        ? {
            ...REVENUE_DETAIL,
            refs: [],
            retired: true,
            status: "retired",
            status_reason: "merged into Churn",
            status_by: "alice",
            edges_in: [{ term_id: 2, rel_type: "REPLACES", name: "Churn" }],
          }
        : CHURN_DETAIL,
    );
    mockHistory.mockResolvedValue({
      retired_in: "b2",
      as_of: "a1",
      columns: [{ table: "erp/public/orders.yaml", column: "revenue" }],
      tables: [],
    });
    render(<GlossaryTab />);
    fireEvent.click(await screen.findByTestId("glossary-item-1"));
    expect(await screen.findByTestId("glossary-applied-history")).toHaveTextContent(
      "erp/public/orders.yaml.revenue",
    );
    expect(mockHistory).toHaveBeenCalledWith(1);
    fireEvent.click(screen.getByTestId("glossary-replaced-by-2"));
    await waitFor(() => expect(mockFetchTerm).toHaveBeenCalledWith(2));
  });

  it("says when the model's history holds no retirement of a retired term", async () => {
    mockFetchTerm.mockResolvedValue({ ...REVENUE_DETAIL, retired: true, status: "retired" });
    mockHistory.mockResolvedValue(null);
    render(<GlossaryTab />);
    fireEvent.click(await screen.findByTestId("glossary-item-1"));
    expect(await screen.findByTestId("glossary-applied-none")).toHaveTextContent(
      t("serverErrors.glossary.no_retirement_history"),
    );
  });

  it("flags a dangling concept (no path to a term naming data) in red", async () => {
    mockList.mockResolvedValue([REVENUE, CHURN, DANGLING]);
    render(<GlossaryTab />);

    const churn = await screen.findByTestId("glossary-item-2");
    expect(within(churn).getByTestId("glossary-role-2")).not.toHaveAttribute("data-dangling");

    const orphan = screen.getByTestId("glossary-item-4");
    const badge = within(orphan).getByTestId("glossary-role-4");
    expect(badge).toHaveAttribute("data-dangling", "true");
    expect(badge).toHaveTextContent(t("glossaryTab.role_concept"));
  });

  it("explains what the glossary is for next to the add button", async () => {
    render(<GlossaryTab />);

    // HoverCard renders its dropdown on hover, so the assertion is on the trigger opening it —
    // the copy itself lands in the portal once the pointer arrives.
    const help = await screen.findByTestId("glossary-purpose-help");
    expect(help).toHaveAttribute("aria-label", t("glossaryTab.purposeAria"));

    fireEvent.mouseEnter(help);
    expect(await screen.findByText(t("glossaryTab.purposeTitle"))).toBeInTheDocument();
    expect(screen.getByText(t("glossaryTab.purposeBody"))).toBeInTheDocument();
    expect(screen.getByText(t("glossaryTab.purposeAdd"))).toBeInTheDocument();
  });

  it("retypes a relationship in the form, in both directions, on Save", async () => {
    render(<GlossaryTab />);
    fireEvent.click(await screen.findByTestId("glossary-item-1"));
    await edit();
    const outgoing = await openSelect("glossary-edge-out-rel-2");
    fireEvent.click(within(outgoing).getByText(t("glossaryTab.rel_KIND_OF")));
    expect(mockRetypeEdge).not.toHaveBeenCalled();
    save();
    await waitFor(() => expect(mockRetypeEdge).toHaveBeenCalledWith(1, 2, "RELATED_TO", "KIND_OF"));

    // The incoming picker shows the same forward types and sends the stored forward type; the
    // edge keeps its own direction: the other term is still the source.
    fireEvent.click(screen.getByTestId("glossary-item-2"));
    await edit();
    const incoming = await openSelect("glossary-edge-in-rel-1");
    fireEvent.click(within(incoming).getByText(t("glossaryTab.rel_PART_OF")));
    save();
    await waitFor(() => expect(mockRetypeEdge).toHaveBeenCalledWith(1, 2, "RELATED_TO", "PART_OF"));
  });

  it("navigates to a relationship's target term when its link is clicked, in both directions", async () => {
    render(<GlossaryTab />);
    fireEvent.click(await screen.findByTestId("glossary-item-1"));
    fireEvent.click(await screen.findByTestId("glossary-edge-out-link-2"));
    await screen.findByTestId("glossary-edge-in-link-1");

    fireEvent.click(screen.getByTestId("glossary-edge-in-link-1"));
    await screen.findByTestId("glossary-edge-out-link-2");
  });

  it("lists only the terms To review when that filter is on", async () => {
    mockList.mockResolvedValue([{ ...REVENUE, findings: 1 }, CHURN]);
    render(<GlossaryTab />);
    await screen.findByTestId("glossary-item-2");
    expect(screen.getByTestId("glossary-review-1")).toHaveTextContent(t("glossaryTab.toReview"));
    fireEvent.click(screen.getByTestId("glossary-to-review-only"));
    await waitFor(() => expect(screen.queryByTestId("glossary-item-2")).toBeNull());
    expect(screen.getByTestId("glossary-item-1")).toBeInTheDocument();
  });

  it("passes the search text and the deprecated toggle to the list endpoint", async () => {
    render(<GlossaryTab />);
    await screen.findByTestId("glossary-item-1");

    mockList.mockResolvedValue([REVENUE]);
    fireEvent.change(screen.getByTestId("glossary-search"), { target: { value: "rev" } });
    await waitFor(() => expect(mockList).toHaveBeenCalledWith("rev", true, null));
    await waitFor(() => expect(screen.queryByTestId("glossary-item-3")).toBeNull());
    expect(screen.getByTestId("glossary-item-1")).toBeInTheDocument();

    mockList.mockResolvedValue([REVENUE, CHURN]);
    fireEvent.click(screen.getByTestId("glossary-show-deprecated"));
    await waitFor(() => expect(mockList).toHaveBeenCalledWith("rev", false, null));
  });

  it("reads every value of the term, under the form's labels", async () => {
    mockFetchTerm.mockImplementation(async (id) => {
      if (id === 3) return { ...MARGIN_DETAIL, domains: ["finance", "sales"] };
      if (id === 1) return REVENUE_DETAIL;
      throw new Error(`no detail fixture for term ${id}`);
    });
    render(<GlossaryTab />);
    fireEvent.click(await screen.findByTestId("glossary-item-3"));
    const read = await screen.findByTestId("glossary-read-fields");
    const fields = within(read)
      .getAllByRole("term")
      .map((dt) => [dt.textContent, dt.nextElementSibling?.textContent]);
    expect(fields).toEqual([
      ["Name", "Margin"],
      ["Definition", t("glossaryTab.hoverNoDefinition")],
      ["Domains", "finance, sales"],
      // Grounded in neither a table nor a column.
      ["Role", "Concept"],
      // Deprecated by its last column leaving: no one gave a reason or a date.
      ["Status", "Deprecated"],
      ["Reason", "—"],
      ["By", "—"],
      ["Planned retirement", "—"],
      [t("glossaryTab.excludeFromExportLabel"), "No"],
    ]);
    // Nothing is edited in place: no input until Edit.
    expect(screen.queryByTestId("glossary-name-input")).toBeNull();
    // An unscoped term is the enterprise's, as the create form names it.
    fireEvent.click(screen.getByTestId("glossary-item-1"));
    await waitFor(() =>
      expect(
        within(screen.getByTestId("glossary-read-fields")).getAllByRole("definition")[2],
      ).toHaveTextContent("All domains (enterprise)"),
    );
  });

  it("renames the term on Save and reads it again; Cancel drops the change", async () => {
    mockUpdate.mockResolvedValue(undefined);
    render(<GlossaryTab />);

    fireEvent.click(await screen.findByTestId("glossary-item-1"));
    await edit();
    expect(screen.getByTestId("glossary-name-input")).toHaveValue("Revenue");
    fireEvent.change(screen.getByTestId("glossary-name-input"), { target: { value: "Gross" } });
    fireEvent.click(screen.getByTestId("glossary-edit-cancel"));
    expect(screen.queryByTestId("glossary-edit-form")).toBeNull();
    expect(mockUpdate).not.toHaveBeenCalled();

    await edit();
    // The form starts from the term again, not from what was cancelled.
    expect(screen.getByTestId("glossary-name-input")).toHaveValue("Revenue");
    fireEvent.change(screen.getByTestId("glossary-name-input"), {
      target: { value: "Net Revenue" },
    });
    save();

    await waitFor(() => expect(mockUpdate).toHaveBeenCalledWith(1, { name: "Net Revenue" }));
    // Save reads the term and the list again, and the panel is read once more.
    await waitFor(() => expect(mockFetchTerm).toHaveBeenCalledTimes(2));
    await waitFor(() => expect(screen.queryByTestId("glossary-edit-form")).toBeNull());
  });

  it("says a refused change in place and keeps the form open with what was typed", async () => {
    mockUpdate.mockRejectedValue(new Error("a term named Churn already exists"));
    render(<GlossaryTab />);
    fireEvent.click(await screen.findByTestId("glossary-item-1"));
    await edit();
    fireEvent.change(screen.getByTestId("glossary-name-input"), { target: { value: "Churn" } });
    save();
    expect(await screen.findByTestId("glossary-edit-refused-fields")).toHaveTextContent(
      "a term named Churn already exists",
    );
    expect(screen.getByTestId("glossary-name-input")).toHaveValue("Churn");
  });

  it("drafts a definition with the AI button into the form without saving it", async () => {
    mockGenerate.mockResolvedValue("Money earned from sales.");
    render(<GlossaryTab />);

    fireEvent.click(await screen.findByTestId("glossary-item-1"));
    await edit();

    fireEvent.click(screen.getByTestId("description-field-generate"));
    await waitFor(() => expect(mockGenerate).toHaveBeenCalledWith(1));

    // The draft lands in the form only; Save sends it.
    const input = within(screen.getByTestId("glossary-definition-input")).getByRole("textbox");
    await waitFor(() => expect(input).toHaveValue("Money earned from sales."));
    expect(mockUpdate).not.toHaveBeenCalled();
  });

  it("offers exactly the ten closed rel_type values in the add-edge form", async () => {
    render(<GlossaryTab />);
    fireEvent.click(await screen.findByTestId("glossary-item-1"));
    await edit();
    await screen.findByTestId("glossary-edge-rel-select");

    const listbox = await openSelect("glossary-edge-rel-select");
    const options = within(listbox).getAllByRole("option", { hidden: true });
    expect(options.map((o) => o.textContent)).toEqual([
      t("glossaryTab.rel_KIND_OF"),
      t("glossaryTab.rel_RELATED_TO"),
      t("glossaryTab.rel_PART_OF"),
      t("glossaryTab.rel_SYNONYM_OF"),
      t("glossaryTab.rel_VALID_VALUE_OF"),
      t("glossaryTab.rel_DERIVED_FROM"),
      t("glossaryTab.rel_REPLACES"),
      t("glossaryTab.rel_PREFERRED_TERM_FOR"),
      t("glossaryTab.rel_TRANSLATION_OF"),
      t("glossaryTab.rel_ANTONYM_OF"),
    ]);
  });

  it("adds and removes relationships in the form, sent on Save", async () => {
    render(<GlossaryTab />);
    fireEvent.click(await screen.findByTestId("glossary-item-1"));
    await edit();
    fireEvent.click(
      within(screen.getByTestId("glossary-edge-out-2-RELATED_TO")).getByRole("button", {
        name: t("glossaryTab.removeEdge"),
      }),
    );
    save();
    await waitFor(() =>
      expect(vi.mocked(removeGlossaryEdge)).toHaveBeenCalledWith(1, 2, "RELATED_TO"),
    );
    expect(vi.mocked(addGlossaryEdge)).not.toHaveBeenCalled();
  });

  it("disables delete for a term with refs or relationships, otherwise surfaces the server 400", async () => {
    render(<GlossaryTab />);

    // Revenue has a physical ref: delete is disabled.
    fireEvent.click(await screen.findByTestId("glossary-item-1"));
    await screen.findByTestId("glossary-delete-btn");
    expect(screen.getByTestId("glossary-delete-btn")).toBeDisabled();

    // Churn is refless but has a relationship: delete is still disabled.
    fireEvent.click(screen.getByTestId("glossary-item-2"));
    await screen.findByTestId("glossary-edge-in-1-RELATED_TO");
    expect(screen.getByTestId("glossary-delete-btn")).toBeDisabled();

    // Margin has neither refs nor relationships: delete is enabled, and a server 400 lands in
    // the error alert.
    fireEvent.click(screen.getByTestId("glossary-item-3"));
    await waitFor(() => expect(screen.getByTestId("glossary-delete-btn")).toBeEnabled());
    vi.spyOn(window, "confirm").mockReturnValue(true);
    mockDelete.mockRejectedValue(new Error("term 3 still has refs"));

    fireEvent.click(screen.getByTestId("glossary-delete-btn"));
    await waitFor(() => expect(mockDelete).toHaveBeenCalledWith(3));
    expect(await screen.findByTestId("glossary-error")).toHaveTextContent("term 3 still has refs");
  });

  it("sets export exclusion in the form, sent on Save", async () => {
    mockUpdate.mockResolvedValue(undefined);
    render(<GlossaryTab />);

    fireEvent.click(await screen.findByTestId("glossary-item-1"));
    await edit();
    const checkbox = screen.getByTestId("glossary-export-excluded-checkbox");
    expect(checkbox).not.toBeChecked();
    fireEvent.click(checkbox);
    expect(mockUpdate).not.toHaveBeenCalled();
    save();
    await waitFor(() => expect(mockUpdate).toHaveBeenCalledWith(1, { export_excluded: true }));
  });

  it("orders its toolbar: + Term, then the icons (Ontology view, the AI menu), then help", async () => {
    render(<GlossaryTab />);
    await screen.findByTestId("glossary-item-1");
    const ontology = screen.getByTestId("glossary-ontology-toggle");
    expect(ontology).toHaveAccessibleName("Ontology view");
    expect(ontology).toHaveAttribute("aria-pressed", "false");
    expect(ontology).not.toHaveTextContent("Ontology view");
    const order = [
      "glossary-new-btn",
      "glossary-ontology-toggle",
      "glossary-generate-menu",
      "glossary-purpose-help",
    ].map((id) => screen.getByTestId(id));
    for (let i = 1; i < order.length; i++)
      expect(
        order[i - 1].compareDocumentPosition(order[i]) & Node.DOCUMENT_POSITION_FOLLOWING,
      ).toBe(Node.DOCUMENT_POSITION_FOLLOWING);
    expect(screen.getByTestId("glossary-generate-menu")).toHaveAccessibleName("Generate with AI");
    // Its two drafts are items of the menu, in this order, and nowhere else on the page.
    expect(screen.queryByTestId("glossary-bulk-relationships-btn")).toBeNull();
    fireEvent.click(screen.getByTestId("glossary-generate-menu"));
    const menu = await screen.findByRole("menu");
    // Mantine's items are not exposed by role in jsdom (mantine-menu-test-pattern): read them in place.
    expect([...menu.querySelectorAll('[role="menuitem"]')].map((m) => m.textContent)).toEqual([
      "Relationships",
      "Definitions",
    ]);
  });

  it("presses the Ontology view icon while the view is on", async () => {
    render(<GlossaryTab />);
    await screen.findByTestId("glossary-item-1");
    fireEvent.click(screen.getByTestId("glossary-ontology-toggle"));
    expect(screen.getByTestId("glossary-ontology-toggle")).toHaveAttribute("aria-pressed", "true");
  });

  it("bulk-generates definitions and shows a count notification", async () => {
    mockBulkDefinitions.mockResolvedValue({ generated: 3 });
    render(<GlossaryTab />);
    await screen.findByTestId("glossary-item-1");

    fireEvent.click(screen.getByTestId("glossary-generate-menu"));
    fireEvent.click(await screen.findByTestId("glossary-bulk-definitions-btn"));
    await waitFor(() => expect(mockBulkDefinitions).toHaveBeenCalled());
    await waitFor(() =>
      expect(mockNotify).toHaveBeenCalledWith(
        expect.objectContaining({
          message: t("glossaryTab.definitionsGenerated_other", { count: 3 }),
        }),
      ),
    );
  });

  // Moving a term's last ref retires the term server-side, so re-reading it would 404.
  it("clears the selection when a move removed the term it was showing", async () => {
    mockMoveRef.mockResolvedValue({ source_term_removed: true });
    render(<GlossaryTab />);

    fireEvent.click(await screen.findByTestId("glossary-item-1"));
    await edit();
    const listbox = await openSelect("glossary-move-select-10-revenue");
    fireEvent.click(within(listbox).getByText("Churn", { ignore: "" }));
    expect(mockMoveRef).not.toHaveBeenCalled();
    save();

    await waitFor(() =>
      expect(mockMoveRef).toHaveBeenCalledWith({
        table_id: 10,
        column_name: "revenue",
        to_term_id: 2,
      }),
    );
    // The panel drops back to the empty state; no second read of the removed term.
    await waitFor(() => expect(screen.queryByTestId("glossary-ref-10-revenue")).toBeNull());
    expect(mockFetchTerm).toHaveBeenCalledTimes(1);
  });

  it("keeps the term open when a move left it with other refs", async () => {
    mockMoveRef.mockResolvedValue({ source_term_removed: false });
    render(<GlossaryTab />);

    fireEvent.click(await screen.findByTestId("glossary-item-1"));
    await edit();
    const listbox = await openSelect("glossary-move-select-10-revenue");
    fireEvent.click(within(listbox).getByText("Churn", { ignore: "" }));
    save();

    await waitFor(() => expect(mockFetchTerm).toHaveBeenCalledTimes(2));
    expect(await screen.findByTestId("glossary-ref-10-revenue")).toBeInTheDocument();
  });

  it("claims a column from another term on Save; the emptied draft leaves the list", async () => {
    mockMoveRef.mockResolvedValue({ source_term_removed: false });
    render(<GlossaryTab />);
    fireEvent.click(await screen.findByTestId("glossary-item-3"));
    await edit();
    const listbox = await openSelect("glossary-claim-columns");
    // Each column says the term it belongs to now.
    expect(within(listbox).getByText("from: Churn (draft)")).toBeInTheDocument();
    fireEvent.click(within(listbox).getByText("sales.events.churn_flag"));
    // The server removes Churn, a draft its last column left.
    mockList.mockResolvedValue([REVENUE, MARGIN]);
    save();
    await waitFor(() =>
      expect(mockMoveRef).toHaveBeenCalledWith({
        table_id: 11,
        column_name: "churn_flag",
        to_term_id: 3,
      }),
    );
    await waitFor(() => expect(screen.queryByTestId("glossary-item-2")).toBeNull());
  });

  it("lets a property claim attribute columns, a concept do either, a class claim none", async () => {
    render(<GlossaryTab />);
    // Revenue names a column (a property): no tables, and no key column to claim.
    fireEvent.click(await screen.findByTestId("glossary-item-1"));
    await edit();
    expect(screen.getByTestId("glossary-grounded-tables-select")).toBeDisabled();
    expect(screen.getByText(t("glossaryTab.groundDisabled"))).toBeInTheDocument();
    const listbox = await openSelect("glossary-claim-columns");
    const option = (label: string) =>
      within(listbox).getByText(label).closest('[role="option"]') as HTMLElement;
    // events' primary key, and alerts.event_ref declared many-to-one into events: keys, each with why.
    expect(option("sales.events.event_id")).toHaveAttribute("data-combobox-disabled");
    expect(option("sales.alerts.event_ref")).toHaveAttribute("data-combobox-disabled");
    expect(within(listbox).getAllByText(t("glossaryTab.keyColumnClaim"))).toHaveLength(2);
    expect(option("sales.events.churn_flag")).not.toHaveAttribute("data-combobox-disabled");
    expect(option("sales.alerts.note")).not.toHaveAttribute("data-combobox-disabled");
    fireEvent.click(screen.getByTestId("glossary-edit-cancel"));

    // Margin names nothing (a concept): it may claim columns or be grounded in tables.
    fireEvent.click(screen.getByTestId("glossary-item-3"));
    await edit();
    await waitFor(() =>
      expect(screen.getByTestId("glossary-grounded-tables-select")).toBeEnabled(),
    );
    expect(screen.getByTestId("glossary-claim-columns")).toBeEnabled();
    fireEvent.click(screen.getByTestId("glossary-edit-cancel"));

    // Grounded in events, Margin is a class: its keys follow from its tables; it claims nothing.
    vi.mocked(listTermTables).mockImplementation(async (id) =>
      id === 3
        ? ([
            {
              table_id: 11,
              table_name: "events",
              domain_id: "sales",
              source_id: "s",
              schema_name: "p",
            },
          ] as never)
        : [],
    );
    fireEvent.click(screen.getByTestId("glossary-item-2"));
    fireEvent.click(await screen.findByTestId("glossary-item-3"));
    await edit();
    expect(await screen.findByTestId("glossary-class-keys")).toHaveTextContent(
      t("glossaryTab.classKeys"),
    );
    expect(screen.queryByTestId("glossary-claim-columns")).toBeNull();
    expect(screen.getByTestId("glossary-grounded-tables-select")).toBeEnabled();
  });

  it("shows a key column's ref without Move: it follows from its table's class", async () => {
    mockFetchTerm.mockImplementation(async (id) => {
      if (id === 1)
        return {
          ...REVENUE_DETAIL,
          refs: [
            {
              table_id: 11,
              column_name: "event_id",
              source_id: "erp",
              schema_name: "public",
              table_name: "events",
              alias: null,
              domain_id: "sales",
            },
          ],
        };
      throw new Error(`no detail fixture for term ${id}`);
    });
    render(<GlossaryTab />);
    fireEvent.click(await screen.findByTestId("glossary-item-1"));
    await edit();
    expect(screen.getByTestId("glossary-key-ref-11:event_id")).toHaveTextContent(
      t("glossaryTab.keyColumn"),
    );
    expect(screen.queryByTestId("glossary-move-select-11-event_id")).toBeNull();
  });

  // Soft delete: a rooted term cannot be deleted (its refs would recreate it), so retiring is
  // how a curator takes one out of service — server-side it drops out of agent term search
  // and metadata export.
  it("retires the selected term through its dialog, then offers to restore it", async () => {
    mockRetire.mockReset();
    render(<GlossaryTab />);
    fireEvent.click(await screen.findByTestId("glossary-item-1"));
    const retire = await screen.findByTestId("glossary-retire-btn");
    expect(retire).toHaveTextContent(t("glossaryTab.retire"));
    fireEvent.click(retire);

    // It holds a column: it says what moves, and asks for a reason and a successor.
    expect(await screen.findByTestId("glossary-retire-moves")).toHaveTextContent("orders.revenue");
    const submit = screen.getByTestId("glossary-retire-submit");
    expect(submit).toBeDisabled();
    fireEvent.change(screen.getByTestId("glossary-retire-reason"), {
      target: { value: "merged into Churn" },
    });
    expect(submit).toBeDisabled();
    const successors = await openSelect("glossary-retire-successor");
    fireEvent.click(within(successors).getByRole("option", { name: "Churn", hidden: true }));

    // The move would take pii off the column: what it loses is shown, and the reader carries it.
    mockRetire.mockRejectedValueOnce(
      new TagLossRefused("loses tags", [
        { column: "revenue", table: "erp.public.orders", loses: ["pii"], gains: [] },
      ]),
    );
    fireEvent.click(screen.getByTestId("glossary-retire-submit"));
    expect(await screen.findByTestId("glossary-tag-loss")).toHaveTextContent(
      t("glossaryTab.loses", { tags: "pii" }),
    );
    mockRetire.mockResolvedValueOnce({ status: "retired", tag_changes: [] });
    mockFetchTerm.mockResolvedValue({ ...REVENUE_DETAIL, retired: true, status: "retired" });
    fireEvent.click(screen.getByTestId("glossary-tag-loss-carry"));
    await waitFor(() =>
      expect(mockRetire).toHaveBeenLastCalledWith(1, {
        reason: "merged into Churn",
        successor: 2,
        carry_tags: true,
      }),
    );
    // Retired: Restore takes Retire's place.
    fireEvent.click(await screen.findByTestId("glossary-restore-btn"));
    expect(screen.queryByTestId("glossary-retire-btn")).toBeNull();
    await waitFor(() => expect(mockRestore).toHaveBeenCalledWith(1));
  });

  it("offers each finding's fix in the term's To review section", async () => {
    mockFindings.mockResolvedValue([
      {
        term_id: 1,
        term: "Revenue",
        findings: [
          {
            kind: "unlinked_tables",
            code: "glossary.finding.unlinked_tables",
            message: "unlinked",
            params: { tables: "erp.public.orders, crm.public.deals" },
          },
          {
            kind: "tag_wrong_kind",
            code: "glossary.finding.tag_wrong_kind",
            message: "wrong kind",
            params: { tags: "pii", role: "concept" },
          },
          {
            kind: "retirement_blocked",
            code: "glossary.finding.retirement_blocked",
            message: "blocked",
            params: { retire_on: "2026-10-01", role: "property" },
          },
        ],
      },
    ]);
    render(<GlossaryTab />);
    fireEvent.click(await screen.findByTestId("glossary-item-1"));
    const chips = await screen.findByTestId("glossary-to-review");

    fireEvent.click(within(chips).getByTestId("glossary-finding-0"));
    fireEvent.click(screen.getByTestId("glossary-finding-fix-same-entity"));
    expect(navigateTo).toHaveBeenCalledWith("/relationships?search=orders%20deals");

    fireEvent.click(within(chips).getByTestId("glossary-finding-1"));
    fireEvent.click(screen.getByTestId("glossary-finding-fix-unmark-pii"));
    await waitFor(() =>
      expect(unassignTag).toHaveBeenCalledWith({ tagId: "pii", objectType: "term", termId: 1 }),
    );

    fireEvent.click(within(chips).getByTestId("glossary-finding-2"));
    fireEvent.click(screen.getByTestId("glossary-finding-fix-retire"));
    expect(await screen.findByTestId("glossary-retire-reason")).toBeInTheDocument();
  });

  it("moves anyway with each lost tag confirmed", async () => {
    mockRetire.mockReset();
    render(<GlossaryTab />);
    fireEvent.click(await screen.findByTestId("glossary-item-1"));
    fireEvent.click(await screen.findByTestId("glossary-retire-btn"));
    fireEvent.change(await screen.findByTestId("glossary-retire-reason"), {
      target: { value: "obsolete" },
    });
    const successors = await openSelect("glossary-retire-successor");
    fireEvent.click(within(successors).getByRole("option", { name: "Churn", hidden: true }));
    mockRetire.mockRejectedValueOnce(
      new TagLossRefused("loses tags", [
        { column: "revenue", table: "erp.public.orders", loses: ["pii", "finance"], gains: [] },
      ]),
    );
    fireEvent.click(screen.getByTestId("glossary-retire-submit"));
    mockRetire.mockResolvedValueOnce({ status: "retired", tag_changes: [] });
    fireEvent.click(await screen.findByTestId("glossary-tag-loss-move"));
    await waitFor(() =>
      expect(mockRetire).toHaveBeenLastCalledWith(1, {
        reason: "obsolete",
        successor: 2,
        confirm_tag_loss: ["pii", "finance"],
      }),
    );
  });

  it("bulk-generates relationships and shows a count notification", async () => {
    mockBulkRelationships.mockResolvedValue({ added: 1 });
    render(<GlossaryTab />);
    await screen.findByTestId("glossary-item-1");

    fireEvent.click(screen.getByTestId("glossary-generate-menu"));
    fireEvent.click(await screen.findByTestId("glossary-bulk-relationships-btn"));
    await waitFor(() => expect(mockBulkRelationships).toHaveBeenCalled());
    await waitFor(() =>
      expect(mockNotify).toHaveBeenCalledWith(
        expect.objectContaining({ message: t("glossaryTab.relationshipsGenerated", { count: 1 }) }),
      ),
    );
  });

  // The user id is what the term is keyed on, so it is never typed: a mistyped id would attribute
  // the term to nobody and — for an author — lock every curator out of it.
  it("picks the author from the org roster and sends that user's id on Save", async () => {
    render(<GlossaryTab />);
    fireEvent.click(await screen.findByTestId("glossary-item-1"));
    await edit();

    const listbox = await openSelect("glossary-expert-user-input");
    // A member with no display name is offered under the email the server does have.
    expect(within(listbox).getByText("grace@acme.test")).toBeInTheDocument();
    // Ada is already the term's expert: the picker offers the members it has not named.
    expect(within(listbox).queryByText("Ada Lovelace")).toBeNull();
    fireEvent.click(within(listbox).getByText("grace@acme.test"));

    const kinds = await openSelect("glossary-expert-kind-select");
    fireEvent.click(within(kinds).getByText(t("glossaryTab.kind_author")));

    fireEvent.click(screen.getByTestId("glossary-expert-add-btn"));
    expect(vi.mocked(addGlossaryExpert)).not.toHaveBeenCalled();
    save();
    await waitFor(() =>
      expect(vi.mocked(addGlossaryExpert)).toHaveBeenCalledWith(1, "u2", "author"),
    );
  });

  // REQ-1845: this page is REST-backed (never Apollo), so Polly's server-side glossary tools
  // (create/update/delete_glossary_term, add/remove_glossary_term_edge) have no other way to
  // tell an already-open Glossary tab its list — and the currently-open term's detail — are
  // stale. Reported live: the page stayed frozen at pre-edit data until manually refreshed.
  it("refetches the list and the open term's detail on a provisa:glossary-changed event", async () => {
    render(<GlossaryTab />);
    await screen.findByTestId("glossary-item-1");
    fireEvent.click(screen.getByTestId("glossary-item-1"));
    await waitFor(() => expect(mockFetchTerm).toHaveBeenCalledWith(1));

    mockList.mockClear();
    mockFetchTerm.mockClear();

    window.dispatchEvent(new CustomEvent("provisa:glossary-changed"));

    await waitFor(() => expect(mockList).toHaveBeenCalled());
    await waitFor(() => expect(mockFetchTerm).toHaveBeenCalledWith(1));
  });
});
