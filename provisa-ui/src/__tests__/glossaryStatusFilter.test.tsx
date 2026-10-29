// Copyright (c) 2026 Kenneth Stott
// Canary: 6a2e9f74-1d5b-4c9a-8e3f-2b7d4a1c9e60
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1387, REQ-2144: a term's status (set by people) and its grounding (computed) are shown as
// separate chips and filtered apart, client-side: both are already in every GlossaryTermSummary
// the list fetch returns.

import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent, within } from "../test-utils/render";
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

const APPROVED: GlossaryTermSummary = {
  id: 1,
  name: "Revenue",
  definition: "Money in.",
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
// A draft a steward has not approved, reaching data only through terms not yet approved.
const DRAFT: GlossaryTermSummary = {
  ...APPROVED,
  grounding: "grounded_pending",
  agent_visible: false,
  id: 2,
  name: "Churn Velocity",
  ref_count: 0,
  table_count: 0,
  identity_gap: false,
  related_to: 0,
  findings: 0,
  status: "draft",
  status_reason: null,
  status_by: null,
  retire_on: null,
};
// Retired: out of service, so it reaches no data.
const RETIRED: GlossaryTermSummary = {
  ...APPROVED,
  grounding: "not_grounded",
  agent_visible: false,
  id: 3,
  name: "Legacy Metric",
  retired: true,
  status: "retired",
};

describe("glossary status and grounding filters", () => {
  beforeEach(() => {
    mockList.mockReset();
    mockList.mockResolvedValue([APPROVED, DRAFT, RETIRED]);
    window.localStorage.clear();
  });

  const pick = async (filter: string, option: string) => {
    fireEvent.click(screen.getByTestId(filter));
    fireEvent.click(await screen.findByRole("option", { name: option, hidden: true }));
  };

  it("shows every term by default", async () => {
    render(<GlossaryTab />);
    await screen.findByTestId("glossary-item-1");
    expect(screen.getByTestId("glossary-item-2")).toBeInTheDocument();
    expect(screen.getByTestId("glossary-item-3")).toBeInTheDocument();
    expect(screen.getByTestId("glossary-status-filter")).toHaveValue("");
    expect(screen.getByTestId("glossary-grounding-filter")).toHaveValue("");
  });

  it("marks status and grounding as separate chips", async () => {
    render(<GlossaryTab />);
    await screen.findByTestId("glossary-item-1");
    // An approved term is the ordinary case: the list marks only its grounding.
    expect(screen.queryByTestId("glossary-status-1")).toBeNull();
    // Grounded: the green ground symbol, named by how far it reaches (its one column).
    const grounded = screen.getByTestId("glossary-grounding-1");
    expect(grounded).toHaveAttribute("data-grounding", "grounded");
    expect(grounded).not.toHaveTextContent("grounded");
    const groundedIcon = within(grounded).getByRole("img");
    expect(groundedIcon).toHaveAttribute("data-ground-state", "grounded");
    expect(groundedIcon).toHaveAttribute("aria-label", "Grounded: reaches data in 1 column");
    expect(screen.getByTestId("glossary-status-2")).toHaveAccessibleName("Draft");
    // Pending: the same symbol, muted, saying its path awaits approval.
    const pending = screen.getByTestId("glossary-grounding-2");
    expect(pending).toHaveAttribute("data-grounding", "grounded_pending");
    const pendingIcon = within(pending).getByRole("img");
    expect(pendingIcon).toHaveAttribute("data-ground-state", "grounded_pending");
    expect(pendingIcon).toHaveAttribute(
      "aria-label",
      "Reaches data, but only through terms not yet approved",
    );
    // Not grounded: no chip.
    expect(screen.queryByTestId("glossary-grounding-3")).toBeNull();
  });

  it("narrows the list by status, client-side", async () => {
    render(<GlossaryTab />);
    await screen.findByTestId("glossary-item-1");
    const callsBefore = mockList.mock.calls.length;
    await pick("glossary-status-filter", "Draft");
    expect(screen.queryByTestId("glossary-item-1")).not.toBeInTheDocument();
    expect(screen.getByTestId("glossary-item-2")).toBeInTheDocument();
    expect(screen.queryByTestId("glossary-item-3")).not.toBeInTheDocument();
    expect(mockList.mock.calls.length).toBe(callsBefore);
  });

  it("narrows the list by grounding, in the words findings use", async () => {
    render(<GlossaryTab />);
    await screen.findByTestId("glossary-item-1");
    await pick("glossary-grounding-filter", "Grounded · path pending approval");
    const list = screen.getByTestId("glossary-list");
    expect(within(list).queryByTestId("glossary-item-1")).not.toBeInTheDocument();
    expect(within(list).getByTestId("glossary-item-2")).toBeInTheDocument();
    await pick("glossary-grounding-filter", "Not grounded");
    expect(within(list).getByTestId("glossary-item-3")).toBeInTheDocument();
    expect(within(list).queryByTestId("glossary-item-2")).not.toBeInTheDocument();
  });

  it("remembers the choices in localStorage and restores them", async () => {
    const { unmount } = render(<GlossaryTab />);
    await screen.findByTestId("glossary-item-1");
    await pick("glossary-status-filter", "Approved");
    expect(window.localStorage.getItem(scopedKey("glossary.status"))).toBe("approved");
    unmount();

    render(<GlossaryTab />);
    await screen.findByTestId("glossary-item-1");
    expect(screen.queryByTestId("glossary-item-2")).not.toBeInTheDocument();
  });
});
