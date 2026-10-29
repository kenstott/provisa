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
  suggestResolvingTerms: vi.fn(),
  createGlossaryTerm: vi.fn(),
  addGlossaryEdge: vi.fn(),
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
// Single-domain mode: a new term declares no domains.
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

import {
  addGlossaryEdge,
  createGlossaryTerm,
  fetchGlossaryTerm,
  generateGlossaryRelationships,
  listGlossaryTerms,
  suggestResolvingTerms,
} from "../api/glossary";

const mockList = vi.mocked(listGlossaryTerms);
const mockFetchTerm = vi.mocked(fetchGlossaryTerm);
const mockBulkRelationships = vi.mocked(generateGlossaryRelationships);
const mockSuggest = vi.mocked(suggestResolvingTerms);
const mockCreate = vi.mocked(createGlossaryTerm);
const mockPostEdge = vi.mocked(addGlossaryEdge);

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
const CAKE: GlossaryTermSummary = { ...REVENUE, id: 5, name: "Cake", domains: ["sales"] };
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
    mockList.mockResolvedValue([REVENUE, CHURN, CAKE]);
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

  // REQ-1387 (CHOOSING RELATED_TO OPENS THE ALTERNATIVES): choosing Related to while adding an
  // edge interrupts with the alternatives first.
  const chooseRelatedTo = async () => {
    fireEvent.click(await screen.findByTestId("glossary-item-1"));
    await edit();
    const terms = await openSelect("glossary-edge-term-select");
    fireEvent.click(within(terms).getByText("Cake"));
    const types = await openSelect("glossary-edge-rel-select");
    fireEvent.click(within(types).getByTestId("glossary-rel-option-RELATED_TO"));
    return screen.findByTestId("glossary-related-to-dialog");
  };

  it("opens the alternatives, tightest first, with the model's names, before adding Related to", async () => {
    mockSuggest.mockResolvedValue({
      resolve: [
        { shape: "component_type", candidates: ["revenue line"] },
        { shape: "common_whole", candidates: ["bakery sale"] },
      ],
    });
    render(<GlossaryTab />);
    const dialog = await chooseRelatedTo();
    expect(dialog).toHaveTextContent(t("glossaryTab.relatedToDialogWhy"));
    expect(mockSuggest).toHaveBeenCalledWith(1, 5);
    await within(dialog).findByText(/revenue line/);
    const shapes = within(dialog)
      .getAllByTestId(/^glossary-related-to-option-/)
      .map((el) => el.getAttribute("data-testid")?.replace("glossary-related-to-option-", ""));
    expect(shapes).toEqual([
      "component_type",
      "specialisation",
      "common_whole",
      "common_genus",
      "common_enumeration",
    ]);
    expect(within(dialog).getByTestId("glossary-related-to-option-common_whole")).toHaveTextContent(
      "Revenue and Cake part of bakery sale",
    );
    // Not added until the person says so: Add anyway is one click.
    expect(screen.queryByTestId("glossary-edge-out-5-RELATED_TO")).toBeNull();
    fireEvent.click(within(dialog).getByTestId("glossary-related-to-anyway"));
    expect(await screen.findByTestId("glossary-edge-out-5-RELATED_TO")).toBeInTheDocument();
  });

  it("adds nothing when the alternatives are cancelled", async () => {
    mockSuggest.mockResolvedValue({ resolve: [] });
    render(<GlossaryTab />);
    const dialog = await chooseRelatedTo();
    fireEvent.click(within(dialog).getByTestId("glossary-related-to-cancel"));
    await waitFor(() => expect(screen.queryByTestId("glossary-related-to-dialog")).toBeNull());
    expect(screen.queryByTestId("glossary-edge-out-5-RELATED_TO")).toBeNull();
  });

  it("starts a shape: the new-term form, prefilled, creates the term and both edges", async () => {
    mockSuggest.mockResolvedValue({
      resolve: [{ shape: "common_whole", candidates: ["bakery sale"] }],
    });
    mockCreate.mockResolvedValue({ id: 9 });
    mockPostEdge.mockResolvedValue(undefined);
    render(<GlossaryTab />);
    const dialog = await chooseRelatedTo();
    await within(dialog).findByText(/bakery sale/);
    fireEvent.click(within(dialog).getByTestId("glossary-related-to-start-common_whole"));
    const form = await screen.findByTestId("glossary-shape-form");
    expect(within(form).getByTestId("glossary-shape-name")).toHaveValue("bakery sale");
    expect(within(form).getByTestId("glossary-shape-edges")).toHaveTextContent(
      `Revenue · ${t("glossaryTab.rel_PART_OF")} · bakery sale`,
    );
    // The person may rename it before it exists.
    fireEvent.change(within(form).getByTestId("glossary-shape-name"), {
      target: { value: "cake sale" },
    });
    fireEvent.click(within(form).getByTestId("glossary-shape-create"));
    await waitFor(() => expect(mockCreate).toHaveBeenCalledWith({ name: "cake sale" }));
    // The edge between the other term and the new one is written; this term's joins the form.
    await waitFor(() => expect(mockPostEdge).toHaveBeenCalledWith(5, 9, "PART_OF"));
    expect(await screen.findByTestId("glossary-edge-out-9-PART_OF")).toHaveTextContent("cake sale");
    expect(screen.queryByTestId("glossary-edge-out-5-RELATED_TO")).toBeNull();
  });

  it("without the model's names, the person types the name in the form", async () => {
    mockSuggest.mockRejectedValue(new Error("no AI model configured"));
    render(<GlossaryTab />);
    const dialog = await chooseRelatedTo();
    expect(await within(dialog).findByTestId("glossary-related-to-no-names")).toHaveTextContent(
      "no AI model configured",
    );
    fireEvent.click(within(dialog).getByTestId("glossary-related-to-start-component_type"));
    const form = await screen.findByTestId("glossary-shape-form");
    expect(within(form).getByTestId("glossary-shape-name")).toHaveValue("");
    expect(within(form).getByTestId("glossary-shape-create")).toBeDisabled();
  });
});
