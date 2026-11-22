// Copyright (c) 2026 Kenneth Stott
// Canary: 1f8c4e27-b9d6-4a30-8e52-d6a7c0b3f914
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2259: Context Health -- the steward summary with its bulk approval as the one action, and a
// card per specified signal (terms, drift, classification awaiting a decision, documents with
// reviews due, Context Sync per catalog). Every number links to the page it counts, drawn as a link
// only for a viewer holding that page's right; the Context Sync card only for a holder of its right.

import { beforeEach, describe, expect, it, vi } from "vitest";
import { render, screen, within } from "../../../test-utils/render";
import type { GlossaryTermSummary } from "../../../api/glossary";
import type { SyncEntry } from "../../../api/syncing";

const auth = vi.hoisted(() => ({ capabilities: ["glossary_read"] as string[] }));
vi.mock("../../../context/AuthContext", () => ({ useAuth: () => auth }));

const api = vi.hoisted(() => ({
  listGlossaryTerms: vi.fn(),
  fetchStewardshipSummary: vi.fn(),
  fetchOntologyDrift: vi.fn(),
  fetchClassificationAwaiting: vi.fn(),
  fetchTagsLost: vi.fn(),
  fetchReviewsDue: vi.fn(),
  fetchSyncing: vi.fn(),
  fetchReview: vi.fn(),
}));
vi.mock("../../../api/glossary", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../../api/glossary")>()),
  listGlossaryTerms: api.listGlossaryTerms,
  fetchStewardshipSummary: api.fetchStewardshipSummary,
  fetchOntologyDrift: api.fetchOntologyDrift,
}));
vi.mock("../../../api/contextViews", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../../api/contextViews")>()),
  fetchClassificationAwaiting: api.fetchClassificationAwaiting,
  fetchTagsLost: api.fetchTagsLost,
}));
vi.mock("../../../api/referenceLibrary", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../../api/referenceLibrary")>()),
  fetchReviewsDue: api.fetchReviewsDue,
}));
vi.mock("../../../api/syncing", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../../api/syncing")>()),
  fetchSyncing: api.fetchSyncing,
  fetchReview: api.fetchReview,
}));

import { ContextHealthPage } from "../ContextHealthPage";
import { blankEntry } from "../../../api/syncing";

const term = (
  id: number,
  status: GlossaryTermSummary["status"],
  grounding: GlossaryTermSummary["grounding"],
) => ({ id, name: `t${id}`, status, grounding }) as GlossaryTermSummary;

const entry = (id: string, extra: Partial<SyncEntry>): SyncEntry => ({
  ...blankEntry("data_catalog"),
  id,
  role: "steward",
  ...extra,
});

beforeEach(() => {
  auth.capabilities = ["glossary_read"];
  api.listGlossaryTerms.mockResolvedValue([
    term(1, "approved", "grounded"),
    term(2, "approved", "grounded"),
    term(3, "approved", "not_grounded"),
    term(4, "draft", "grounded_pending"),
    term(5, "draft", "not_grounded"),
    term(6, "draft", "not_grounded"),
  ]);
  api.fetchStewardshipSummary.mockResolvedValue({
    agent_visible: [
      { term_id: 1, name: "t1", status: "approved" },
      { term_id: 2, name: "t2", status: "approved" },
      { term_id: 4, name: "t4", status: "draft" },
    ],
    waiting_on_one_approval: [],
    approved_not_grounded: [{ term_id: 3, name: "t3" }],
  });
  api.fetchOntologyDrift.mockResolvedValue({
    has_history: true,
    cutoff: "2026-11-01T00:00:00Z",
    base: { sha: "abc", committed_at: "2026-10-31T00:00:00Z" },
    terms: [],
    counts: { lost: 2, gained: 1, new: 4 },
  });
  api.fetchClassificationAwaiting.mockResolvedValue({
    tables: [{ tableId: 9, tableRef: "pg.public.customers", awaiting: 3 }],
    hidden: 2,
  });
  api.fetchTagsLost.mockResolvedValue({
    cutoff: "2026-10-01T00:00:00+00:00",
    count: 3,
    hidden: 1,
    losses: [
      {
        tableId: 9,
        tableRef: "pg.public.customers",
        column: "home_address",
        tag: "pii",
        termId: 21,
        term: "Home address",
        confirmedBy: "alice",
        confirmedAt: "2026-11-20T14:00:00+00:00",
      },
      {
        tableId: 9,
        tableRef: "pg.public.customers",
        column: "email",
        tag: "pii",
        termId: null,
        term: "Email",
        confirmedBy: null,
        confirmedAt: "2026-11-02T09:00:00+00:00",
      },
    ],
  });
  api.fetchReviewsDue.mockResolvedValue({
    documents: [{ id: 11, title: "Privacy policy", reviews: 2 }],
  });
  api.fetchSyncing.mockResolvedValue({
    entries: [
      entry("collibra", {
        last_run: {
          id: "collibra",
          provider: "collibra",
          ok: false,
          published: {},
          total_published: 0,
          errors: [
            { asset: "a", message: "refused" },
            { asset: "b", message: "refused" },
          ],
        },
      }),
      entry("purview", { last_run: null }),
      entry("owl", { type: "ontology", direction: "import", role: null }),
    ],
  });
  api.fetchReview.mockImplementation(async (id: string) => {
    if (id === "purview") throw new Error("reading 'purview' failed: timed out");
    return { id, total: 5 };
  });
});

const count = async (testId: string) => screen.findByTestId(testId);

describe("Context Health's cards, from real reads", () => {
  it("counts terms visible to agents, approved from data, reaching no data, and drafts", async () => {
    render(<ContextHealthPage />);
    expect(await count("health-terms-agent-visible")).toHaveTextContent("3");
    expect(screen.getByTestId("health-terms-from-data")).toHaveTextContent("2");
    expect(screen.getByTestId("health-terms-no-data")).toHaveTextContent("1");
    expect(screen.getByTestId("health-terms-drafts")).toHaveTextContent("3");
  });

  it("counts the grounding drift this period, and says when there is no history", async () => {
    const view = render(<ContextHealthPage />);
    expect(await count("health-drift-lost")).toHaveTextContent("2");
    expect(screen.getByTestId("health-drift-gained")).toHaveTextContent("1");
    expect(screen.getByTestId("health-drift-new")).toHaveTextContent("4");
    view.unmount();
    api.fetchOntologyDrift.mockResolvedValue({
      has_history: false,
      cutoff: "2026-11-01T00:00:00Z",
      base: null,
      terms: null,
      counts: null,
    });
    render(<ContextHealthPage />);
    expect(await screen.findByTestId("health-drift-no-history")).toBeInTheDocument();
    expect(screen.queryByTestId("health-drift-lost")).not.toBeInTheDocument();
  });

  it("counts detections awaiting a decision, naming only tables the viewer reads", async () => {
    render(<ContextHealthPage />);
    expect(await count("health-classification-total")).toHaveTextContent("5");
    expect(screen.getByTestId("health-classification-table-9")).toHaveTextContent(
      "pg.public.customers",
    );
    expect(screen.getByTestId("health-classification-hidden")).toHaveTextContent("2");
  });

  it("lists the documents with reviews due", async () => {
    render(<ContextHealthPage />);
    expect(await count("health-documents-total")).toHaveTextContent("1");
    expect(screen.getByTestId("health-documents-11")).toHaveTextContent("Privacy policy");
  });

  it("counts this period's confirmed tag losses and lists each, naming only readable columns", async () => {
    render(<ContextHealthPage />);
    expect(await count("health-tags-lost-total")).toHaveTextContent("3");
    const first = screen.getByTestId("health-tags-lost-0");
    expect(within(first).getByTestId("health-tags-lost-column-0")).toHaveTextContent(
      "pg.public.customers.home_address",
    );
    expect(within(first).getByTestId("health-tags-lost-tag-0")).toHaveTextContent("pii");
    expect(within(first).getByTestId("health-tags-lost-term-0")).toHaveTextContent(
      "Home address",
    );
    expect(first).toHaveTextContent("alice");
    // A loss no member signed names no one; a term since deleted is its name alone.
    const second = screen.getByTestId("health-tags-lost-1");
    expect(second).not.toHaveTextContent("alice");
    expect(within(second).getByTestId("health-tags-lost-term-1").tagName).toBe("SPAN");
    expect(screen.getByTestId("health-tags-lost-hidden")).toHaveTextContent("1");
    expect(screen.queryByTestId("health-tags-lost-unrecorded")).not.toBeInTheDocument();
  });

  it("shows a card's read refused in words, the other cards unaffected", async () => {
    api.fetchReviewsDue.mockRejectedValue(new Error("Reviews due failed (500)"));
    render(<ContextHealthPage />);
    expect(await screen.findByTestId("health-documents-error")).toHaveTextContent("500");
    expect(await count("health-terms-drafts")).toHaveTextContent("3");
  });
});

describe("links are drawn only for the right their page asks (REQ-2259)", () => {
  it("a glossary reader follows the term and document counts, not the drift report or tables", async () => {
    render(<ContextHealthPage />);
    expect(await count("health-terms-drafts")).toHaveAttribute("href", "/context/ontology");
    expect(await count("health-documents-11")).toHaveAttribute(
      "href",
      "/context/documents?document=11",
    );
    expect(screen.getByTestId("health-classification-total")).toHaveAttribute(
      "href",
      "/context/classification",
    );
    // Observability opens the Ontology quality report; table_registration the table's page.
    expect(await count("health-drift-lost")).not.toHaveAttribute("href");
    expect(screen.getByTestId("health-drift-lost").tagName).toBe("SPAN");
    expect(screen.getByTestId("health-classification-table-9")).not.toHaveAttribute("href");
    // The Tags lost count links to the Tags page; the column to its table page, a right it lacks.
    expect(await count("health-tags-lost-total")).toHaveAttribute("href", "/context/tags");
    expect(screen.getByTestId("health-tags-lost-column-0").tagName).toBe("SPAN");
  });

  it("draws the Tags lost count as plain text for a viewer without the Tags page's right", async () => {
    auth.capabilities = [];
    render(<ContextHealthPage />);
    expect(await count("health-tags-lost-total")).toHaveTextContent("3");
    expect(screen.getByTestId("health-tags-lost-total").tagName).toBe("SPAN");
    expect(screen.getByTestId("health-tags-lost-term-0").tagName).toBe("SPAN");
  });

  it("a holder of the report's and the table page's rights follows those links too", async () => {
    auth.capabilities = ["glossary_read", "observability", "table_registration"];
    render(<ContextHealthPage />);
    expect(await count("health-drift-lost")).toHaveAttribute(
      "href",
      "/admin/reports?report=ontology_quality",
    );
    expect(screen.getByTestId("health-classification-table-9")).toHaveAttribute(
      "href",
      "/tables?table=9&section=quality",
    );
    expect(await count("health-tags-lost-column-0")).toHaveAttribute(
      "href",
      "/tables?table=9&section=columns&column=home_address",
    );
  });

  it("draws no Context Sync card and reads no syncing for a viewer without its right", async () => {
    render(<ContextHealthPage />);
    await count("health-terms-drafts");
    expect(screen.queryByTestId("health-sync")).not.toBeInTheDocument();
    expect(api.fetchSyncing).not.toHaveBeenCalled();
  });

  it("offers the bulk approval only to a curator", async () => {
    render(<ContextHealthPage />);
    await screen.findByTestId("glossary-stewardship");
    expect(screen.queryByTestId("glossary-bulk-approve-btn")).not.toBeInTheDocument();
  });
});

describe("Context Sync per catalog, for a holder of its right", () => {
  beforeEach(() => {
    auth.capabilities = ["glossary_read", "glossary_rw", "org_settings"];
  });

  it("shows each catalog's differences waiting, its last sync and its failures", async () => {
    render(<ContextHealthPage />);
    const collibra = await screen.findByTestId("health-sync-collibra");
    expect(within(collibra).getByTestId("health-sync-waiting-collibra")).toHaveTextContent(
      "5 differences waiting",
    );
    expect(within(collibra).getByTestId("health-sync-waiting-collibra")).toHaveAttribute(
      "href",
      "/context/sync",
    );
    expect(within(collibra).getByTestId("health-sync-run-collibra")).toHaveTextContent(
      "2 failures",
    );
    // A catalog the review could not read says why, never "no differences".
    const purview = screen.getByTestId("health-sync-purview");
    expect(within(purview).getByTestId("health-sync-failed-purview")).toHaveTextContent(
      "timed out",
    );
    expect(within(purview).queryByTestId("health-sync-waiting-purview")).not.toBeInTheDocument();
    // An ontology source has no catalog differences to read.
    expect(api.fetchReview).not.toHaveBeenCalledWith("owl");
    expect(screen.getByTestId("health-sync-owl")).toBeInTheDocument();
  });

  it("offers the bulk approval to a curator", async () => {
    render(<ContextHealthPage />);
    expect(await screen.findByTestId("glossary-bulk-approve-btn")).toBeInTheDocument();
  });
});
