// Copyright (c) 2026 Kenneth Stott
// Canary: 9a41c7e2-3d58-4b06-8f19-c5e2a07d6b14
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2201: a demo's tour drives the glossary's Find terms and points at the reference library.
// A find_terms step types a document's address, starts a scan and cancels it; a scan step opens
// the scan done ahead; a library step points at the document's row. Each is walked here on the
// real screens, its actions run as the tour runs them and its anchors found where they draw.

import { describe, it, expect, vi, beforeEach } from "vitest";
import { fireEvent, render, screen, waitFor } from "../test-utils/render";

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
vi.mock("../api/glossary", () => ({ generateGlossaryRelationships: vi.fn() }));
const library = vi.hoisted(() => ({
  fetchLibrary: vi.fn(),
  fetchLibraryDocument: vi.fn(),
  fetchCitedBy: vi.fn(),
  fetchRetrieval: vi.fn(),
  saveRetrieval: vi.fn(),
  addLibraryDocument: vi.fn(),
  setLibraryDomains: vi.fn(),
  keepLibraryDocument: vi.fn(),
  refreshLibraryDocument: vi.fn(),
  reembedLibraryDocument: vi.fn(),
  reembedLibrary: vi.fn(),
  uploadLibraryVersion: vi.fn(),
  removeLibraryDocument: vi.fn(),
}));
vi.mock("../api/referenceLibrary", () => library);
vi.mock("../hooks/useCapability", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../hooks/useCapability")>()),
  useCapability: () => false,
}));
vi.mock("../hooks/useAdminQueries", () => ({
  useDomains: () => ({ domains: [{ id: "bank" }] }),
}));

import { FindTermsDialog } from "../components/admin/FindTermsDialog";
import { LibraryPage } from "../pages/library/LibraryPage";
import { runUiActions, type UiAction } from "../mcp/uiActions";
import { demoTourSteps } from "../tour/demoTour";

const RULE =
  "https://www.ecfr.gov/api/versioner/v1/full/2026-10-15/title-31.xml?part=1010&section=1010.311";

const DONE = {
  id: 7,
  document_id: 4,
  document_title: "31 CFR 1010.311",
  document_source: RULE,
  document_removed: false,
  domains: ["bank"],
  status: "done",
  error: null,
  summary: {
    passages: 6,
    mentions: 9,
    candidates: 1,
    calls: 3,
    left_out: {
      not_a_concept: 0,
      quote_not_in_passage: 0,
      unknown_passage: 0,
      malformed: 0,
      unresolved: 0,
    },
  },
  started_by: "org_admin",
  started_at: "2026-11-03T09:00:00Z",
  finished_at: "2026-11-03T09:04:00Z",
  model: "openrouter:some-model",
  step: "merging",
  batch: 0,
  batches: 0,
  budget_limit: null,
  counts: { draft: 1 },
};
const RUNNING = {
  ...DONE,
  id: 8,
  status: "running",
  summary: null,
  finished_at: null,
  step: "reading",
  batch: 1,
  batches: 6,
  counts: {},
};
const DRAFT = {
  id: 11,
  scan_id: 7,
  name: "transaction in currency",
  category: "regulatory term",
  sense: "",
  definition: "A transaction involving the physical transfer of currency.",
  definition_kind: "explicit",
  proposal: "new",
  match: null,
  match_kind: null,
  status: "draft",
  term: null,
  decided_by: null,
  decided_at: null,
  passages: [],
  variants: [],
  conflicts: [],
  homonyms: [],
  matches: [],
};

const STEPS = demoTourSteps(
  [
    { key: "find", route: "/admin/ontology", capability: "glossary_read", find_terms: RULE },
    { key: "scan", scan: RULE },
    { key: "lib", route: "/library", capability: "glossary_read", library: RULE },
  ],
  [],
);

function dialog(defaultDomains: string[] = ["bank"]) {
  return render(
    <FindTermsDialog
      terms={[]}
      domainOptions={["bank"]}
      domainsEnabled
      defaultDomains={defaultDomains}
      onClose={() => undefined}
      onGlossaryChanged={() => undefined}
    />,
  );
}

const RUN = { confirm: async () => true, paceMs: 0, waitMs: 2000 };

beforeEach(() => {
  Object.values(api).forEach((fn) => fn.mockReset());
  Object.values(library).forEach((fn) => fn.mockReset());
  api.fetchScannableDocuments.mockResolvedValue([]);
});

describe("a find_terms step", () => {
  it("opens Find terms from the glossary's menu, then types the address, starts and cancels", () => {
    const [find] = STEPS;
    expect(find.route).toBe("/admin/ontology");
    // Opened as a scan step opens it, each click waiting for what it opens: a menu pressed as an
    // action inside a running tour closed again before its item was pressed.
    expect(find.ensureOpen).toEqual(STEPS[1].ensureOpen);
    expect(find.actions).toEqual([
      { do: "press", target: { testId: "find-terms-mode-url" } },
      { do: "fill", target: { testId: "find-terms-url" }, value: RULE },
      { do: "press", target: { testId: "find-terms-start" } },
      { do: "press", target: { testId: "find-terms-cancel" } },
    ]);
    // Leaving the step closes the window, so the next step opens it afresh.
    expect(find.clickAfterNext).toBe('[data-testid="find-terms-dialog"] .mantine-Modal-close');
  });

  it("anchors on the stopped scan its actions leave in the window", async () => {
    const [find] = STEPS;
    api.fetchScans.mockResolvedValue([]);
    api.startScan.mockResolvedValue({ scan: RUNNING, document: {} });
    api.fetchScan.mockResolvedValue({ scan: RUNNING, drafts: [] });
    api.cancelScan.mockResolvedValue({
      scan: { ...RUNNING, status: "failed", error: "cancelled by visitor" },
    });
    dialog();
    await screen.findByTestId("find-terms-start");
    const inDialog = find.actions as UiAction[];
    const acted = await runUiActions(inDialog, RUN);
    expect(acted.steps).toHaveLength(4);
    expect(acted.success).toBe(true);
    expect(api.startScan).toHaveBeenCalledWith({ url: RULE }, ["bank"]);
    expect(api.cancelScan).toHaveBeenCalledWith(8);
    await waitFor(() => expect(document.querySelector(find.element)).not.toBeNull());
  });

  it("chooses the domains it names before it starts, when Find terms opens with none", async () => {
    // A sandbox's domain filter shows every domain, so Find terms proposes none; a scan in none
    // is refused in a demo in domains.
    const [find] = demoTourSteps(
      [
        {
          key: "find",
          route: "/admin/ontology",
          capability: "glossary_read",
          find_terms: { url: RULE, domains: ["bank"] },
        },
      ],
      [],
    );
    expect((find.actions as UiAction[])[2]).toEqual({
      do: "choose",
      target: { testId: "find-terms-domains" },
      option: "bank",
    });
    api.fetchScans.mockResolvedValue([]);
    api.startScan.mockResolvedValue({ scan: RUNNING, document: {} });
    api.fetchScan.mockResolvedValue({ scan: RUNNING, drafts: [] });
    api.cancelScan.mockResolvedValue({
      scan: { ...RUNNING, status: "failed", error: "cancelled by visitor" },
    });
    dialog([]);
    await screen.findByTestId("find-terms-start");
    const acted = await runUiActions(find.actions as UiAction[], RUN);
    expect(acted.steps.at(-1)).toMatch(/^Pressed/);
    expect(acted.success).toBe(true);
    expect(api.startScan).toHaveBeenCalledWith({ url: RULE }, ["bank"]);
    await waitFor(() => expect(document.querySelector(find.element)).not.toBeNull());
  });
});

describe("a scan step", () => {
  it("opens the scan done ahead from the list, never the one the tour cancelled", async () => {
    const [, scan] = STEPS;
    const cancelled = { ...RUNNING, status: "failed", error: "cancelled by visitor" };
    api.fetchScans.mockResolvedValue([cancelled, DONE]);
    api.fetchScan.mockResolvedValue({ scan: DONE, drafts: [DRAFT] });
    dialog();
    await screen.findByTestId("find-terms-scan-7");
    const opener = document.querySelector<HTMLElement>(scan.clickBefore ?? "");
    expect(opener).toBe(screen.getByTestId("find-terms-open-7"));
    fireEvent.click(opener!);
    await waitFor(() => expect(document.querySelector(scan.readySelector ?? "")).not.toBeNull());
    expect(document.querySelector(scan.element)).toBe(screen.getByTestId("find-terms-queue"));
    expect(api.fetchScan).toHaveBeenCalledWith(7);
  });

  it("opens Find terms first when it is not open", () => {
    const [, scan] = STEPS;
    expect(scan.ensureOpen).toEqual([
      {
        click: '[data-testid="glossary-generate-menu"]',
        unlessPresent: '[data-testid="find-terms-dialog"], [data-testid="glossary-find-terms-btn"]',
      },
      {
        click: '[data-testid="glossary-find-terms-btn"]',
        unlessPresent: '[data-testid="find-terms-dialog"]',
      },
    ]);
    expect(scan.clickAfterNext).toBe('[data-testid="find-terms-dialog"] .mantine-Modal-close');
  });
});

describe("a library step", () => {
  it("points at the document's row on the Reference library page, by its address", async () => {
    const [, , lib] = STEPS;
    library.fetchRetrieval.mockResolvedValue("passage");
    library.fetchLibrary.mockResolvedValue([
      {
        id: 4,
        title: "31 CFR 1010.311",
        source_kind: "url",
        source: RULE,
        media_type: "application/xml",
        status: "proposed",
        index_status: "ready",
        index_error: null,
        index_step: null,
        index_done: null,
        index_total: null,
        terms_note: null,
        domains: ["bank"],
        embedding_model: "sentence-transformers/all-MiniLM-L6-v2",
        needs_reembedding: false,
        passages: 6,
        added_by: "org_admin",
        added_at: "2026-11-03T09:00:00Z",
        refreshed_at: null,
        replaced_at: null,
      },
    ]);
    render(<LibraryPage />);
    const row = await screen.findByTestId("reflib-row-4");
    expect(lib.route).toBe("/library");
    expect(document.querySelector(lib.element)).toBe(row);
  });
});
