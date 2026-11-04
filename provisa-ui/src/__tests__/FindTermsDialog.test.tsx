// Copyright (c) 2026 Kenneth Stott
// Canary: 6b3e0f94-d27a-4c18-8e51-a9f4c2d07b63
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2179: Find terms in a document. The steward picks a library document or gives a new file or
// URL, with the domains they work in, and starts a scan; its review queue shows each candidate's
// proposal, definition (explicit or synthesized), synonyms, passages and conflicting explanations
// side by side, and the steward accepts, merges, rejects or accepts several at once. REQ-2202: or
// the steward picks a registered table -- domain, table, id column, text column -- and each
// candidate's passages cite the table and the row's id.

import { describe, it, expect, vi, beforeEach } from "vitest";
import { fireEvent, render, screen, waitFor, within } from "../test-utils/render";

const api = vi.hoisted(() => ({
  fetchScannableDocuments: vi.fn(),
  fetchScans: vi.fn(),
  fetchScan: vi.fn(),
  startScan: vi.fn(),
  editDraft: vi.fn(),
  acceptDraft: vi.fn(),
  mergeDraft: vi.fn(),
  rejectDraft: vi.fn(),
  bulkAccept: vi.fn(),
  cancelScan: vi.fn(),
}));
vi.mock("../api/termFinding", () => api);
const glossary = vi.hoisted(() => ({ generateGlossaryRelationships: vi.fn() }));
vi.mock("../api/glossary", () => glossary);

const col = (columnName: string, dataType: string) => ({
  id: columnName.length,
  columnName,
  alias: null,
  computedSqlAlias: null,
  description: null,
  dataType,
  nativeFilterType: null,
  nativeFilterRequired: false,
  requiredBySteward: false,
  isPrimaryKey: columnName === "note_id",
  isForeignKey: false,
});
const NOTES = {
  id: 21,
  sourceId: "crm",
  domainId: "sales",
  schemaName: "public",
  tableName: "notes",
  alias: null,
  draft: false,
  columns: [col("note_id", "varchar"), col("body", "text")],
};
// One value per hook across renders: a fresh array each render reruns the form's effects.
const TABLES = {
  tables: [
    NOTES,
    { ...NOTES, id: 22, tableName: "draft_notes", draft: true },
    { ...NOTES, id: 23, domainId: "finance", tableName: "ledger" },
  ],
  loading: false,
  refetch: vi.fn(),
};
vi.mock("../hooks/useAdminQueries", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../hooks/useAdminQueries")>()),
  useTables: () => TABLES,
}));

import { FindTermsDialog } from "../components/admin/FindTermsDialog";

const DOC = {
  id: 4,
  title: "Client policy",
  source_kind: "url",
  source: "https://policies.example.com/client.html",
  status: "kept",
  domains: ["sales"],
  passages: 12,
  added_at: "2026-10-20T09:12:00Z",
  refreshed_at: null,
  replaced_at: null,
};

const SCAN = {
  id: 7,
  document_id: 4,
  document_title: "Client policy",
  document_source: DOC.source,
  document_removed: false,
  domains: ["sales"],
  status: "done",
  error: null,
  summary: {
    passages: 12,
    mentions: 9,
    candidates: 2,
    calls: 3,
    left_out: {
      not_a_concept: 1,
      quote_not_in_passage: 1,
      unknown_passage: 0,
      malformed: 0,
      unresolved: 0,
    },
  },
  started_by: "ada",
  started_at: "2026-10-29T09:00:00Z",
  finished_at: "2026-10-29T09:01:00Z",
  model: "anthropic:claude-haiku-4-5",
  step: "merging",
  batch: 0,
  batches: 0,
  budget_limit: null,
  counts: { draft: 2 },
};

const RUNNING = {
  ...SCAN,
  status: "running",
  summary: null,
  finished_at: null,
  step: "reading",
  batch: 3,
  batches: 12,
  counts: {},
};

const cite = (section: string, quote: string, role: string, mention: number) => ({
  mention,
  ordinal: mention,
  section,
  page: null,
  quote,
  explicit: role === "definition",
  sense: "",
  role,
});

const NEW_DRAFT = {
  id: 11,
  scan_id: 7,
  name: "record retention period",
  category: "policy",
  sense: "how long records are kept",
  definition: "How long client records are kept.",
  definition_kind: "synthesized",
  proposal: "new",
  match: null,
  match_kind: null,
  status: "draft",
  term: null,
  decided_by: null,
  decided_at: null,
  passages: [
    cite("Retention", "Client records are kept for seven years.", "synthesis", 5),
    cite("Archive", "Client records are kept for five years.", "synthesis", 6),
  ],
  variants: [{ text: "retention period", kind: "alternative name", keep: true }],
  conflicts: [
    [
      cite("Retention", "Client records are kept for seven years.", "synthesis", 5),
      cite("Archive", "Client records are kept for five years.", "synthesis", 6),
    ],
  ],
  homonyms: [],
  matches: [],
};

const MATCH_DRAFT = {
  ...NEW_DRAFT,
  id: 12,
  name: "customer",
  category: "business concept",
  definition: '"Customer" means any party that holds an account.',
  definition_kind: "explicit",
  proposal: "merge",
  match: { term_id: 3, name: "client" },
  match_kind: "synonym",
  passages: [cite("Definitions", '"Customer" means any party.', "definition", 0)],
  variants: [{ text: "Customers", kind: "form", keep: false }],
  conflicts: [],
  homonyms: [],
  matches: [{ term_id: 3, name: "client", via: "client" }],
};

const TABLE_SOURCE = {
  domain: "sales",
  table: "notes",
  id_column: "note_id",
  text_column: "body",
};

const TABLE_SCAN = {
  ...SCAN,
  id: 9,
  document_id: null,
  document_title: "sales.notes",
  document_source: "sales.notes",
  source_table: TABLE_SOURCE,
};

const rowCite = (row: string, quote: string, role: string, mention: number) => ({
  ...cite("", quote, role, mention),
  section: null,
  row,
});

const ROW_DRAFT = {
  ...MATCH_DRAFT,
  id: 13,
  scan_id: 9,
  passages: [
    rowCite("n-1", '"Customer" means any party.', "definition", 0),
    rowCite("n-4", "A customer account is opened.", "supporting", 1),
  ],
};

/** Pick ``option`` in the Select whose input has ``testId``. */
async function choose(testId: string, option: string) {
  const input = screen.getByTestId(testId);
  fireEvent.click(input);
  await waitFor(() => expect(input).toHaveAttribute("aria-controls"));
  const list = document.getElementById(input.getAttribute("aria-controls") as string);
  fireEvent.click(within(list as HTMLElement).getByRole("option", { name: option, hidden: true }));
}

function open() {
  return render(
    <FindTermsDialog
      terms={[]}
      domainOptions={["sales", "finance"]}
      domainsEnabled
      defaultDomains={["sales"]}
      onClose={() => undefined}
      onGlossaryChanged={() => undefined}
    />,
  );
}

beforeEach(() => {
  Object.values(api).forEach((fn) => fn.mockReset());
  glossary.generateGlossaryRelationships.mockReset();
  api.fetchScannableDocuments.mockResolvedValue([DOC]);
  api.fetchScans.mockResolvedValue([{ ...SCAN, source_table: null }]);
  api.fetchScan.mockResolvedValue({ scan: SCAN, drafts: [NEW_DRAFT, MATCH_DRAFT] });
});

describe("FindTermsDialog", () => {
  it("lists earlier scans and starts one on a new URL in the steward's domains", async () => {
    api.startScan.mockResolvedValue({ scan: { ...SCAN, id: 8, status: "running" }, document: DOC });
    open();
    const row = await screen.findByTestId("find-terms-scan-7");
    expect(row).toHaveTextContent("Client policy");
    expect(row).toHaveTextContent("2 to review, 0 decided");
    fireEvent.click(screen.getByRole("radio", { name: "A URL" }));
    expect(screen.getByTestId("find-terms-new-note")).toHaveTextContent(
      "joins the reference library as proposed",
    );
    fireEvent.click(screen.getByTestId("find-terms-start"));
    expect(await screen.findByText("Choose a document to scan.")).toBeInTheDocument();
    fireEvent.change(screen.getByTestId("find-terms-url"), {
      target: { value: "https://x.example.com/records.html" },
    });
    fireEvent.click(screen.getByTestId("find-terms-start"));
    await waitFor(() =>
      expect(api.startScan).toHaveBeenCalledWith({ url: "https://x.example.com/records.html" }, [
        "sales",
      ]),
    );
    await waitFor(() => expect(api.fetchScan).toHaveBeenCalledWith(8));
  });

  it("scans a library document again on demand", async () => {
    api.startScan.mockResolvedValue({ scan: { ...SCAN, id: 9 }, document: DOC });
    open();
    fireEvent.click(await screen.findByTestId("find-terms-rescan-7"));
    await waitFor(() => expect(api.startScan).toHaveBeenCalledWith({ document_id: 4 }, ["sales"]));
  });

  it("shows each draft's proposal, definition, passages and conflicts side by side", async () => {
    open();
    fireEvent.click(await screen.findByTestId("find-terms-open-7"));
    const draft = await screen.findByTestId("find-terms-draft-11");
    expect(screen.getByTestId("find-terms-summary")).toHaveTextContent(
      "12 passages read, 9 mentions, 2 candidate terms.",
    );
    expect(within(draft).getByText("New term")).toBeInTheDocument();
    expect(within(draft).getByTestId("find-terms-synthesized")).toHaveTextContent("Synthesized");
    const conflicts = within(draft).getByTestId("find-terms-conflicts");
    expect(within(conflicts).getAllByTestId("find-terms-passage")).toHaveLength(2);
    expect(conflicts).toHaveTextContent("seven years");
    expect(conflicts).toHaveTextContent("five years");
    const match = screen.getByTestId("find-terms-draft-12");
    expect(within(match).getByTestId("find-terms-match")).toHaveTextContent(
      "Another name for client",
    );
    expect(within(match).queryByTestId("find-terms-accept-12")).toBeNull();
  });

  it("accepts, merges, rejects and bulk accepts", async () => {
    api.acceptDraft.mockResolvedValue({ term_id: 30 });
    api.mergeDraft.mockResolvedValue({ term_id: 3 });
    api.rejectDraft.mockResolvedValue(undefined);
    api.bulkAccept.mockResolvedValue({ decided: [{ id: 11 }], refused: [] });
    open();
    fireEvent.click(await screen.findByTestId("find-terms-open-7"));
    fireEvent.click(await screen.findByTestId("find-terms-accept-11"));
    await waitFor(() => expect(api.acceptDraft).toHaveBeenCalledWith(11));
    fireEvent.click(screen.getByTestId("find-terms-merge-12"));
    fireEvent.click(screen.getByTestId("find-terms-merge-confirm"));
    await waitFor(() =>
      expect(api.mergeDraft).toHaveBeenCalledWith(12, { term_id: 3, use_definition: false }),
    );
    fireEvent.click(screen.getByTestId("find-terms-reject-11"));
    await waitFor(() => expect(api.rejectDraft).toHaveBeenCalledWith(11));
    fireEvent.click(screen.getByTestId("find-terms-select-all"));
    fireEvent.click(screen.getByTestId("find-terms-accept-selected"));
    await waitFor(() => expect(api.bulkAccept).toHaveBeenCalledWith(7, [11, 12]));
  });

  it("hands the accepted terms to Generate relationships", async () => {
    api.fetchScan.mockResolvedValue({
      scan: SCAN,
      drafts: [{ ...NEW_DRAFT, status: "accepted", term: { term_id: 30, name: "x" } }],
    });
    glossary.generateGlossaryRelationships.mockResolvedValue({ added: 2 });
    open();
    fireEvent.click(await screen.findByTestId("find-terms-open-7"));
    fireEvent.click(await screen.findByTestId("find-terms-propose-relationships"));
    await waitFor(() => expect(glossary.generateGlossaryRelationships).toHaveBeenCalled());
  });

  it("shows a running scan's batch and model, then the merging step", async () => {
    api.fetchScans.mockResolvedValue([RUNNING]);
    api.fetchScan.mockResolvedValue({ scan: RUNNING, drafts: [] });
    open();
    expect(await screen.findByTestId("find-terms-progress-7")).toHaveTextContent(
      "Reading part 3 of 12...",
    );
    fireEvent.click(screen.getByTestId("find-terms-open-7"));
    expect(await screen.findByTestId("find-terms-progress")).toHaveTextContent(
      "Reading part 3 of 12...",
    );
    expect(screen.getByTestId("find-terms-model")).toHaveTextContent(
      "Model: anthropic:claude-haiku-4-5",
    );
    api.fetchScan.mockResolvedValue({
      scan: { ...RUNNING, step: "merging", batch: 0, batches: 0 },
      drafts: [],
    });
    await waitFor(
      () =>
        expect(screen.getByTestId("find-terms-progress")).toHaveTextContent(
          "Merging what was found into candidate terms...",
        ),
      { timeout: 4000 },
    );
  });

  it("keeps the list current while a scan in it runs", async () => {
    api.fetchScans.mockResolvedValueOnce([RUNNING]);
    api.fetchScans.mockResolvedValue([
      { ...RUNNING, status: "failed", error: "anthropic:m gave no answer within 300 seconds" },
    ]);
    open();
    await screen.findByTestId("find-terms-progress-7");
    expect(
      await screen.findByTestId("find-terms-stopped-7", {}, { timeout: 4000 }),
    ).toHaveTextContent("anthropic:m gave no answer within 300 seconds");
    expect(screen.queryByTestId("find-terms-progress-7")).toBeNull();
  });

  it("cancels a running scan and shows that it stopped", async () => {
    api.fetchScans.mockResolvedValue([RUNNING]);
    api.fetchScan.mockResolvedValue({ scan: RUNNING, drafts: [] });
    api.cancelScan.mockResolvedValue({
      scan: { ...RUNNING, status: "failed", error: "cancelled by ada" },
    });
    open();
    fireEvent.click(await screen.findByTestId("find-terms-open-7"));
    fireEvent.click(await screen.findByTestId("find-terms-cancel"));
    await waitFor(() => expect(api.cancelScan).toHaveBeenCalledWith(7));
    expect(await screen.findByTestId("find-terms-failed")).toHaveTextContent(
      "The scan stopped: cancelled by ada",
    );
    expect(screen.queryByTestId("find-terms-cancel")).toBeNull();
  });

  it("shows what the role's budget did to a scan", async () => {
    api.fetchScan.mockResolvedValue({
      scan: {
        ...RUNNING,
        status: "failed",
        error: "role 'steward' has spent its LLM budget for the hour: 900 tokens of 900 tokens",
        budget_limit: 500,
      },
      drafts: [],
    });
    open();
    fireEvent.click(await screen.findByTestId("find-terms-open-7"));
    expect(await screen.findByTestId("find-terms-failed")).toHaveTextContent(
      "has spent its LLM budget for the hour",
    );
    expect(screen.getByTestId("find-terms-budget-limited")).toHaveTextContent(
      "held a call to 500 output tokens",
    );
  });

  it("names why a scan stopped", async () => {
    api.fetchScan.mockResolvedValue({
      scan: { ...SCAN, status: "failed", error: "the role's LLM budget for the hour is spent" },
      drafts: [],
    });
    open();
    fireEvent.click(await screen.findByTestId("find-terms-open-7"));
    expect(await screen.findByTestId("find-terms-failed")).toHaveTextContent(
      "the role's LLM budget for the hour is spent",
    );
  });

  it("scans a registered table: its domain, the table, its id column and its text column", async () => {
    api.startScan.mockResolvedValue({ scan: { ...TABLE_SCAN, status: "running" } });
    open();
    await screen.findByTestId("find-terms-scan-7");
    fireEvent.click(screen.getByRole("radio", { name: "A table" }));
    expect(screen.queryByTestId("find-terms-mode")).toBeNull();
    expect(screen.getByTestId("find-terms-table-note")).toHaveTextContent(
      "its row rules and masks apply",
    );
    fireEvent.click(screen.getByTestId("find-terms-start"));
    expect(
      await screen.findByText("Choose a table, its ID column and its text column."),
    ).toBeInTheDocument();
    await choose("find-terms-table-domain", "sales");
    fireEvent.click(screen.getByTestId("find-terms-table"));
    // A draft is in no role's schema, and a table of another domain is not offered.
    expect(screen.queryByRole("option", { name: "draft_notes", hidden: true })).toBeNull();
    expect(screen.queryByRole("option", { name: "ledger", hidden: true })).toBeNull();
    await choose("find-terms-table", "notes");
    await choose("find-terms-id-column", "note_id (varchar)");
    await choose("find-terms-text-column", "body (text)");
    fireEvent.click(screen.getByTestId("find-terms-start"));
    await waitFor(() =>
      expect(api.startScan).toHaveBeenCalledWith({ table: TABLE_SOURCE }, ["sales"]),
    );
  });

  it("marks a table's scan and scans the table again on demand", async () => {
    api.fetchScans.mockResolvedValue([TABLE_SCAN]);
    api.startScan.mockResolvedValue({ scan: { ...TABLE_SCAN, id: 10 } });
    open();
    const row = await screen.findByTestId("find-terms-scan-9");
    expect(row).toHaveTextContent("sales.notes");
    expect(within(row).getByTestId("find-terms-table-scan-9")).toHaveTextContent("Table");
    expect(row).not.toHaveTextContent("Removed from the library");
    fireEvent.click(screen.getByTestId("find-terms-rescan-9"));
    await waitFor(() =>
      expect(api.startScan).toHaveBeenCalledWith({ table: TABLE_SOURCE }, ["sales"]),
    );
  });

  it("cites a table's passages by the table and the row's id", async () => {
    api.fetchScans.mockResolvedValue([TABLE_SCAN]);
    api.fetchScan.mockResolvedValue({ scan: TABLE_SCAN, drafts: [ROW_DRAFT] });
    open();
    fireEvent.click(await screen.findByTestId("find-terms-open-9"));
    const draft = await screen.findByTestId("find-terms-draft-13");
    fireEvent.click(within(draft).getByTestId("find-terms-passages-toggle-13"));
    const passages = within(draft).getAllByTestId("find-terms-passage");
    expect(passages[0]).toHaveTextContent("sales.notes, row n-1");
    expect(passages[1]).toHaveTextContent("sales.notes, row n-4");
  });
});
