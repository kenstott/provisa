// Copyright (c) 2026 Kenneth Stott
// Canary: 3e9b6c1a-58d2-4f07-a4b1-c2d7e90f6a35
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1592: the author/expert picker is filled from the org's roster for anyone who curates —
// including a curator who does not manage users. The demo sandbox's visitor is one: its role holds
// glossary_rw but is withheld user_management (REQ-1608), the right the team page's
// /admin/orgs/{id}/members answers to. Read from there, its term page opened under a red
// "fetchOrgMembers failed (403)". The server here answers as it does for that visitor.

import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import { act, render, screen, fireEvent, waitFor, within } from "../test-utils/render";
import { GlossaryTab } from "../components/admin/GlossaryTab";
import type { GlossaryTermDetail, GlossaryTermSummary } from "../api/glossary";

// jsdom draws no canvas: the term page's ontology snippet is tested on its own (TermDag.test.tsx).
vi.mock("../components/graph3d/OntologyGraph", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../components/graph3d/OntologyGraph")>()),
  OntologySnippet: () => null,
}));
// The term list and detail are fixtures; the roster is fetched for real, from the stub below.
vi.mock("../api/glossary", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../api/glossary")>()),
  listGlossaryTerms: vi.fn(),
  fetchGlossaryTerm: vi.fn(),
  fetchGlossaryFindings: vi.fn(async () => []),
  listTermTables: vi.fn(async () => []),
  fetchAppliedHistory: vi.fn(async () => null),
  fetchStewardshipSummary: vi.fn(async () => ({
    agent_visible: [],
    waiting_on_one_approval: [],
    approved_not_grounded: [],
  })),
}));

vi.mock("@mantine/notifications", () => ({ notifications: { show: vi.fn() } }));

// The sandbox visitor: curates the glossary, holds no user_management.
vi.mock("../context/AuthContext", () => ({
  useAuth: () => ({ capabilities: ["glossary_read", "glossary_rw"], activeOrgId: "acme" }),
}));

import { fetchGlossaryTerm, listGlossaryTerms } from "../api/glossary";

const TERM: GlossaryTermSummary = {
  id: 32,
  name: "Exposure",
  definition: "What a group stands to lose.",
  is_abstract: false,
  deprecated: false,
  ref_count: 1,
  table_count: 0,
  identity_gap: false,
  related_to: 0,
  findings: 0,
  status: "approved",
  status_reason: null,
  status_by: null,
  retire_on: null,
  export_excluded: false,
  retired: false,
  grounding: "grounded",
  agent_visible: true,
  domains: [],
};
const DETAIL: GlossaryTermDetail = {
  ...TERM,
  refs: [],
  edges_out: [],
  edges_in: [],
  experts: [],
  tags: [],
  pending_on: [],
};

const json = (status: number, body: unknown) =>
  new Response(JSON.stringify(body), { status, headers: { "Content-Type": "application/json" } });

describe("the term page of a curator without user_management", () => {
  const asked: string[] = [];
  // The roster answers once the term is open, as it does over a real network: the detail's own
  // load clears the page's error, so an answer that beat it would hide the banner it raises.
  let answerRoster: () => void;

  beforeEach(() => {
    asked.length = 0;
    window.localStorage.clear();
    vi.mocked(listGlossaryTerms).mockResolvedValue([TERM]);
    vi.mocked(fetchGlossaryTerm).mockResolvedValue(DETAIL);
    const rosterHeld = new Promise<void>((resolve) => (answerRoster = resolve));
    vi.spyOn(globalThis, "fetch").mockImplementation(async (input) => {
      const url = String(input);
      asked.push(url);
      await rosterHeld;
      if (url.endsWith("/admin/orgs/acme/members")) return json(403, { detail: "forbidden" });
      if (url.endsWith("/admin/glossary/people"))
        return json(200, [{ user_id: "u1", email: "ada@acme.test", display_name: "Ada Lovelace" }]);
      throw new Error(`unexpected request ${url}`);
    });
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  it("opens the term with no error banner", async () => {
    render(<GlossaryTab />, { initialEntries: ["/admin/ontology?term=32"] });
    await screen.findByText("What a group stands to lose.");
    await waitFor(() => expect(asked.length).toBeGreaterThan(0));
    await act(async () => {
      answerRoster();
      await new Promise((resolve) => setTimeout(resolve, 50));
    });
    expect(screen.queryByTestId("glossary-error")).toBeNull();
    expect(screen.queryByText(/fetchOrgMembers failed/)).toBeNull();
  });

  it("fills the expert picker from the glossary's roster, not the team page's", async () => {
    render(<GlossaryTab />, { initialEntries: ["/admin/ontology?term=32"] });
    fireEvent.click(await screen.findByTestId("glossary-edit-btn"));
    await screen.findByTestId("glossary-edit-form");
    act(() => answerRoster());

    const input = screen.getByTestId("glossary-expert-user-input");
    fireEvent.click(input);
    await waitFor(() => {
      if (!input.getAttribute("aria-controls")) throw new Error("dropdown not open");
    });
    const listbox = document.getElementById(input.getAttribute("aria-controls") as string)!;
    expect(await within(listbox).findByText("Ada Lovelace")).toBeInTheDocument();
    expect(asked.some((u) => u.endsWith("/admin/orgs/acme/members"))).toBe(false);
  });
});
