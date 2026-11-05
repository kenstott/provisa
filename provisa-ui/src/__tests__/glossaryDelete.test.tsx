// Copyright (c) 2026 Kenneth Stott
// Canary: cd0dde8e-54fc-41a6-be9d-b10c9abff6cd
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2198: a glossary term's Delete works like a save. From the confirmation until the delete
// settles the detail is locked and the term's item in the list is marked as being deleted; a
// delete that went through closes the detail on a list read again without the term. A failed
// delete (it unlocks and says why) is GlossaryTab.test.tsx's.

import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent, waitFor } from "../test-utils/render";
import i18n from "../i18n";
import { GlossaryTab } from "../components/admin/GlossaryTab";
import type { GlossaryTermDetail, GlossaryTermSummary } from "../api/glossary";

const t = i18n.getFixedT("en");

// jsdom draws no canvas: the term graph is tested on its own (TermDag.test.tsx).
vi.mock("../components/admin/TermDag", () => ({ TermDag: () => null }));
vi.mock("../api/glossary", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../api/glossary")>()),
  listAttributablePeople: vi.fn(async () => []),
  listGlossaryTerms: vi.fn(),
  fetchGlossaryTerm: vi.fn(),
  deleteGlossaryTerm: vi.fn(),
  fetchGlossaryFindings: vi.fn(async () => []),
  listTermTables: vi.fn(async () => []),
  fetchAppliedHistory: vi.fn(async () => null),
}));
vi.mock("../components/admin/claimColumns", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../components/admin/claimColumns")>()),
  columnTerms: vi.fn(async () => new Map()),
}));
vi.mock("@mantine/notifications", () => ({ notifications: { show: vi.fn() } }));
// A curator: the tab is mounted holding both glossary rights.
vi.mock("../context/AuthContext", () => ({
  useAuth: () => ({
    capabilities: ["glossary_read", "glossary_rw"],
    activeOrgId: "acme",
    actingRoles: "org_admin",
  }),
}));

import { deleteGlossaryTerm, fetchGlossaryTerm, listGlossaryTerms } from "../api/glossary";
import { notifications } from "@mantine/notifications";

const mockList = vi.mocked(listGlossaryTerms);
const mockFetchTerm = vi.mocked(fetchGlossaryTerm);
const mockDelete = vi.mocked(deleteGlossaryTerm);
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
// No refs, no relationships: a term that can be deleted.
const MARGIN: GlossaryTermSummary = {
  ...REVENUE,
  id: 3,
  name: "Margin",
  definition: "What is left.",
  ref_count: 0,
};
const MARGIN_DETAIL: GlossaryTermDetail = {
  ...MARGIN,
  refs: [],
  edges_out: [],
  edges_in: [],
  experts: [],
  tags: [],
  pending_on: [],
};

describe("GlossaryTab delete (REQ-2198)", () => {
  beforeEach(() => {
    window.localStorage.clear();
    mockList.mockReset();
    mockFetchTerm.mockReset();
    mockDelete.mockReset();
    mockNotify.mockClear();
    mockList.mockResolvedValue([REVENUE, MARGIN]);
    mockFetchTerm.mockImplementation(async (id) => {
      if (id === 3) return MARGIN_DETAIL;
      throw new Error(`no detail fixture for term ${id}`);
    });
  });

  it("deletes a term like a save: locked with a spinner, its item marked, closed on the list read again (REQ-2198)", async () => {
    let finish: () => void = () => {};
    mockDelete.mockImplementation(
      () =>
        new Promise<void>((resolve) => {
          finish = resolve;
        }),
    );
    render(<GlossaryTab />);
    fireEvent.click(await screen.findByTestId("glossary-item-3"));
    await waitFor(() => expect(screen.getByTestId("glossary-delete-btn")).toBeEnabled());

    fireEvent.click(screen.getByTestId("glossary-delete-btn"));
    fireEvent.click(await screen.findByTestId("glossary-delete-confirm-confirm"));
    await waitFor(() => expect(mockDelete).toHaveBeenCalledWith(3));

    // Mid-delete: the detail is locked, the control busy, the term's item offered for nothing.
    await waitFor(() => expect(screen.getByTestId("panel-delete-lock")).toBeDisabled());
    expect(screen.getByTestId("glossary-delete-btn")).toHaveAttribute("aria-busy", "true");
    expect(screen.getByTestId("glossary-delete-btn")).toBeDisabled();
    expect(screen.getByTestId("glossary-edit-btn")).toBeDisabled();
    const item = screen.getByTestId("glossary-item-3");
    expect(item).toHaveAttribute("data-deleting", "true");
    expect(item).toHaveAttribute("data-disabled", "true");
    expect(screen.getByTestId("glossary-detail")).toHaveTextContent(MARGIN.name);

    // Done: the list is read again without the term, then the detail closes.
    const reads = mockList.mock.calls.length;
    mockList.mockResolvedValue([REVENUE]);
    finish();
    await waitFor(() => expect(screen.queryByTestId("glossary-item-3")).not.toBeInTheDocument());
    expect(mockList.mock.calls.length).toBeGreaterThan(reads);
    await waitFor(() =>
      expect(screen.queryByTestId("glossary-delete-btn")).not.toBeInTheDocument(),
    );
    expect(screen.queryByTestId("panel-delete-lock")).not.toBeInTheDocument();
    expect(mockNotify).toHaveBeenCalledWith(
      expect.objectContaining({ message: t("glossaryTab.deleted", { name: MARGIN.name }) }),
    );
  });
});
