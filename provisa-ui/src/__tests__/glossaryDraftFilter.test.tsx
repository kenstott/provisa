// Copyright (c) 2026 Kenneth Stott
// Canary: 6a2e9f74-1d5b-4c9a-8e3f-2b7d4a1c9e60
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1387: a term is a draft (see the badge next to it in the list) exactly when it is none
// of live, retired or deprecated — the raw output of the semantic layer's derivation, not yet
// curated. The Draft checkbox (checked = show, on by default) narrows the list to admitted
// terms client-side when unchecked, since the three booleans it filters on are already in every
// GlossaryTermSummary the list fetch returns.

import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent } from "../test-utils/render";
import { GlossaryTab } from "../components/admin/GlossaryTab";
import type { GlossaryTermSummary } from "../api/glossary";

// jsdom draws no canvas: the term graph is tested on its own (TermDag.test.tsx).
vi.mock("../components/admin/TermDag", () => ({ TermDag: () => null }));
vi.mock("../api/glossary", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../api/glossary")>()),
  listGlossaryTerms: vi.fn(),
  fetchGlossaryTerm: vi.fn(),
  // A term with no findings: the findings list names none.
  fetchGlossaryFindings: vi.fn(async () => []),
}));

vi.mock("@mantine/notifications", () => ({ notifications: { show: vi.fn() } }));

const auth = { capabilities: ["glossary_read", "glossary_rw"], activeOrgId: "acme" };
vi.mock("../context/AuthContext", () => ({ useAuth: () => auth }));

vi.mock("../api/admin", () => ({ fetchOrgMembers: vi.fn(async () => []) }));

const filter = {
  domains: [],
  setDomains: () => {},
  selectedDomain: "all",
  setSelectedDomain: () => {},
  checkedDomains: new Set<string>(),
  toggleDomain: () => {},
  ensureDomainChecked: () => {},
  domainsEnabled: false,
};
vi.mock("../context/DomainFilterContext", () => ({ useDomainFilter: () => filter }));

import { listGlossaryTerms } from "../api/glossary";
import { storageKey as scopedKey } from "../lib/browserStorage";

const mockList = vi.mocked(listGlossaryTerms);

const LIVE: GlossaryTermSummary = {
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
// A draft: not yet admitted, neither retired nor deprecated.
const DRAFT: GlossaryTermSummary = {
  ...LIVE,
  id: 2,
  name: "Churn Velocity",
  ref_count: 0,
  table_count: 0,
  identity_gap: false,
  findings: 0,
  status: "draft",
  status_reason: null,
  status_by: null,
  retire_on: null,
  live: false,
};
// Not live, but retired: curated away from the draft state, so it must survive the filter too.
const RETIRED: GlossaryTermSummary = {
  ...LIVE,
  id: 3,
  name: "Legacy Metric",
  live: false,
  retired: true,
  status: "retired",
};

describe("glossary draft filter", () => {
  beforeEach(() => {
    mockList.mockReset();
    mockList.mockResolvedValue([LIVE, DRAFT, RETIRED]);
    window.localStorage.clear();
  });

  it("shows draft terms by default", async () => {
    render(<GlossaryTab />);
    await screen.findByTestId("glossary-item-1");
    expect(screen.getByTestId("glossary-item-2")).toBeInTheDocument();
    expect(screen.getByTestId("glossary-item-3")).toBeInTheDocument();
    expect(screen.getByTestId("glossary-show-draft")).toBeChecked();
  });

  it("hides only draft terms when unchecked", async () => {
    render(<GlossaryTab />);
    await screen.findByTestId("glossary-item-1");

    fireEvent.click(screen.getByTestId("glossary-show-draft"));

    expect(screen.getByTestId("glossary-item-1")).toBeInTheDocument();
    expect(screen.queryByTestId("glossary-item-2")).not.toBeInTheDocument();
    expect(screen.getByTestId("glossary-item-3")).toBeInTheDocument();
  });

  it("does not refetch the list — this is a client-side view filter", async () => {
    render(<GlossaryTab />);
    await screen.findByTestId("glossary-item-1");
    const callsBefore = mockList.mock.calls.length;

    fireEvent.click(screen.getByTestId("glossary-show-draft"));

    expect(screen.getByTestId("glossary-item-1")).toBeInTheDocument();
    expect(mockList.mock.calls.length).toBe(callsBefore);
  });

  it("remembers the choice in localStorage and restores it", async () => {
    const { unmount } = render(<GlossaryTab />);
    await screen.findByTestId("glossary-item-1");
    fireEvent.click(screen.getByTestId("glossary-show-draft"));
    expect(window.localStorage.getItem(scopedKey("glossary.showDraft"))).toBe("false");
    unmount();

    render(<GlossaryTab />);
    await screen.findByTestId("glossary-item-1");
    expect(screen.getByTestId("glossary-show-draft")).not.toBeChecked();
    expect(screen.queryByTestId("glossary-item-2")).not.toBeInTheDocument();
  });
});
