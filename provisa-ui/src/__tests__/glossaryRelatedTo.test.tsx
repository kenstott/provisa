// Copyright (c) 2026 Kenneth Stott
// Canary: 0c7f3a95-62d1-4e8b-b9a4-5e13d2f87c40
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1387 (RELATED_TO IS THE LAST RESORT; RELATED_TO HOLDS THE PLACE; THE SHAPES THAT RESOLVE A
// RELATED PAIR): the picker shows each type's test, a term held only by Related to is guided, the
// list filters to the terms on a Related to edge, and the assistant's Related to proposals come back
// with the shapes that may resolve them.

import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent, waitFor, within } from "../test-utils/render";
import i18n from "../i18n";
import { GlossaryTab } from "../components/admin/GlossaryTab";
import type { GlossaryTermDetail, GlossaryTermSummary } from "../api/glossary";

const t = i18n.getFixedT("en");

// jsdom draws no canvas: the term graph is tested on its own (TermDag.test.tsx).
vi.mock("../components/admin/TermDag", () => ({ TermDag: () => null }));
vi.mock("../api/glossary", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../api/glossary")>()),
  listGlossaryTerms: vi.fn(),
  fetchGlossaryTerm: vi.fn(),
  generateGlossaryRelationships: vi.fn(),
  listTermTables: vi.fn(async () => []),
  fetchGlossaryFindings: vi.fn(async () => []),
}));
vi.mock("../hooks/useAdminQueries", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../hooks/useAdminQueries")>()),
  useTables: () => ({ tables: [], loading: false, error: undefined, refetch: vi.fn() }),
  useRelationships: () => ({
    relationships: [],
    loading: false,
    error: undefined,
    refetch: vi.fn(),
  }),
}));
vi.mock("@mantine/notifications", () => ({ notifications: { show: vi.fn() } }));
vi.mock("../context/AuthContext", () => ({
  useAuth: () => ({
    capabilities: ["glossary_read", "glossary_rw"],
    activeOrgId: "acme",
    actingRoles: "org_admin",
  }),
}));
vi.mock("../api/admin", () => ({ fetchOrgMembers: vi.fn(async () => []) }));

import {
  fetchGlossaryTerm,
  generateGlossaryRelationships,
  listGlossaryTerms,
} from "../api/glossary";

const mockList = vi.mocked(listGlossaryTerms);
const mockFetchTerm = vi.mocked(fetchGlossaryTerm);
const mockBulkRelationships = vi.mocked(generateGlossaryRelationships);

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
const CHURN: GlossaryTermSummary = { ...REVENUE, id: 2, name: "Churn", ref_count: 0 };
const detailOf = (term: GlossaryTermSummary): GlossaryTermDetail => ({
  ...term,
  refs: [],
  edges_out: [],
  edges_in: [],
  experts: [],
  tags: [],
  pending_on: [],
});
const REVENUE_DETAIL: GlossaryTermDetail = {
  ...detailOf(REVENUE),
  edges_out: [{ term_id: 2, rel_type: "RELATED_TO", name: "Churn" }],
};
const CHURN_DETAIL: GlossaryTermDetail = {
  ...detailOf(CHURN),
  edges_in: [{ term_id: 1, rel_type: "RELATED_TO", name: "Revenue" }],
};

// Mantine Select in jsdom: the options are found through the input's aria-controls listbox.
async function openSelect(testid: string): Promise<HTMLElement> {
  const input = screen.getByTestId(testid);
  fireEvent.click(input);
  await waitFor(() => {
    if (!input.getAttribute("aria-controls")) throw new Error("dropdown not open");
  });
  return document.getElementById(input.getAttribute("aria-controls") as string) as HTMLElement;
}

async function edit() {
  fireEvent.click(await screen.findByTestId("glossary-edit-btn"));
  return screen.findByTestId("glossary-edit-form");
}

describe("Related to guidance", () => {
  beforeEach(() => {
    window.localStorage.clear();
    mockList.mockReset();
    mockList.mockResolvedValue([REVENUE, CHURN]);
    mockFetchTerm.mockReset();
    mockFetchTerm.mockImplementation(async (id) => (id === 2 ? CHURN_DETAIL : REVENUE_DETAIL));
  });

  // REQ-1387 (RELATED_TO HOLDS THE PLACE): the RELATED_TO edges are the ones that want review.
  it("lists only the terms on a Related to edge when that filter is on", async () => {
    mockList.mockResolvedValue([{ ...REVENUE, related_to: 1 }, CHURN]);
    render(<GlossaryTab />);
    await screen.findByTestId("glossary-item-2");
    // The filter reaches the guidance on resolving a Related to.
    expect(screen.getByTestId("glossary-related-to-guide")).toHaveAttribute(
      "href",
      "/docs-site/ontology/#building-a-glossary-that-grounds",
    );
    fireEvent.click(screen.getByTestId("glossary-related-to-only"));
    await waitFor(() => expect(screen.queryByTestId("glossary-item-2")).toBeNull());
    expect(screen.getByTestId("glossary-item-1")).toBeInTheDocument();
  });

  // REQ-1387 (RELATED_TO IS THE LAST RESORT): guidance on the page, never a block.
  it("guides a term that is not grounded and held only by Related to", async () => {
    mockFetchTerm.mockImplementation(async (id) =>
      id === 2 ? { ...CHURN_DETAIL, grounding: "not_grounded" } : REVENUE_DETAIL,
    );
    render(<GlossaryTab />);
    fireEvent.click(await screen.findByTestId("glossary-item-2"));
    expect(await screen.findByTestId("glossary-related-to-hint")).toHaveTextContent(
      t("glossaryTab.relatedToHint"),
    );
    // Not a block: the term is still edited as any other.
    expect(screen.getByTestId("glossary-edit-btn")).toBeEnabled();
  });

  it("does not guide a grounded term, or one with a grounding link", async () => {
    mockFetchTerm.mockImplementation(async (id) =>
      id === 2
        ? {
            ...CHURN_DETAIL,
            grounding: "not_grounded",
            edges_out: [{ term_id: 3, rel_type: "PART_OF", name: "Margin" }],
          }
        : REVENUE_DETAIL,
    );
    render(<GlossaryTab />);
    fireEvent.click(await screen.findByTestId("glossary-item-2"));
    await screen.findByTestId("glossary-edge-out-link-3");
    expect(screen.queryByTestId("glossary-related-to-hint")).toBeNull();
    fireEvent.click(screen.getByTestId("glossary-item-1"));
    await screen.findByTestId("glossary-edge-out-link-2");
    expect(screen.queryByTestId("glossary-related-to-hint")).toBeNull();
  });

  // REQ-1387: the picker shows each type's test, the grounding types first and Related to last.
  it("shows each type's test in the relationship picker, grounding types first", async () => {
    render(<GlossaryTab />);
    fireEvent.click(await screen.findByTestId("glossary-item-1"));
    await edit();
    const listbox = await openSelect("glossary-edge-rel-select");
    const options = within(listbox)
      .getAllByRole("option", { hidden: true })
      .map((o) => o.getAttribute("value"));
    expect(options.slice(0, 3)).toEqual(["KIND_OF", "PART_OF", "VALID_VALUE_OF"]);
    expect(options[options.length - 1]).toBe("RELATED_TO");
    for (const rt of ["KIND_OF", "PART_OF", "VALID_VALUE_OF", "RELATED_TO"]) {
      expect(within(listbox).getByTestId(`glossary-rel-option-${rt}`)).toHaveTextContent(
        t(`glossaryTab.relTest_${rt}`),
      );
    }
    expect(screen.getByTestId("glossary-edge-guide")).toHaveAttribute(
      "href",
      "/docs-site/ontology/#building-a-glossary-that-grounds",
    );
  });

  // REQ-1387 (THE SHAPES THAT RESOLVE A RELATED PAIR): each Related to the assistant added comes
  // back with what may resolve it, tightest first; the curator chooses, nothing is added.
  it("shows the Related to links the assistant added, with the shapes that may resolve them", async () => {
    mockBulkRelationships.mockResolvedValue({
      added: 1,
      review: [
        {
          from_term_id: 1,
          from: "Revenue",
          to_term_id: 2,
          to: "Churn",
          resolve: [
            { shape: "component_type", candidates: ["retention metric"] },
            { shape: "common_genus", candidates: ["kpi", "business metric"] },
          ],
        },
      ],
    });
    render(<GlossaryTab />);
    await screen.findByTestId("glossary-item-1");
    fireEvent.click(screen.getByTestId("glossary-generate-menu"));
    fireEvent.click(await screen.findByTestId("glossary-bulk-relationships-btn"));
    const pair = await screen.findByTestId("glossary-review-pair-1-2");
    const shapes = within(pair)
      .getAllByTestId(/^glossary-review-shape-/)
      .map((el) => el.getAttribute("data-testid"));
    expect(shapes).toEqual([
      "glossary-review-shape-component_type",
      "glossary-review-shape-common_genus",
    ]);
    expect(within(pair).getByTestId("glossary-review-shape-common_genus")).toHaveTextContent(
      "kpi / business metric",
    );
    fireEvent.click(screen.getByTestId("glossary-review-close"));
    await waitFor(() => expect(screen.queryByTestId("glossary-review-pair-1-2")).toBeNull());
  });
});
