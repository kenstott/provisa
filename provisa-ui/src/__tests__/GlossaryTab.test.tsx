// Copyright (c) 2026 Kenneth Stott
// Canary: 39d495f3-b737-40b0-9d11-cab151560282
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
  addGlossaryExpert: vi.fn(),
  removeGlossaryExpert: vi.fn(),
  generateGlossaryDefinition: vi.fn(),
  generateAllGlossaryDefinitions: vi.fn(),
  generateGlossaryRelationships: vi.fn(),
}));

vi.mock("@mantine/notifications", () => ({ notifications: { show: vi.fn() } }));

import { notifications } from "@mantine/notifications";
import {
  deleteGlossaryTerm,
  fetchGlossaryTerm,
  generateAllGlossaryDefinitions,
  generateGlossaryDefinition,
  generateGlossaryRelationships,
  listGlossaryTerms,
  updateGlossaryTerm,
} from "../api/glossary";

const mockList = vi.mocked(listGlossaryTerms);
const mockFetchTerm = vi.mocked(fetchGlossaryTerm);
const mockUpdate = vi.mocked(updateGlossaryTerm);
const mockDelete = vi.mocked(deleteGlossaryTerm);
const mockGenerate = vi.mocked(generateGlossaryDefinition);
const mockBulkDefinitions = vi.mocked(generateAllGlossaryDefinitions);
const mockBulkRelationships = vi.mocked(generateGlossaryRelationships);
const mockNotify = vi.mocked(notifications.show);

const REVENUE: GlossaryTermSummary = {
  id: 1,
  name: "Revenue",
  definition: "Money in.",
  is_abstract: false,
  deprecated: false,
  ref_count: 1,
  export_excluded: false,
};
const CHURN: GlossaryTermSummary = {
  id: 2,
  name: "Churn",
  definition: null,
  is_abstract: true,
  deprecated: false,
  ref_count: 0,
  export_excluded: false,
};
const MARGIN: GlossaryTermSummary = {
  id: 3,
  name: "Margin",
  definition: null,
  is_abstract: false,
  deprecated: true,
  ref_count: 0,
  export_excluded: false,
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

describe("GlossaryTab", () => {
  beforeEach(() => {
    mockList.mockReset();
    mockFetchTerm.mockReset();
    mockUpdate.mockReset();
    mockDelete.mockReset();
    mockBulkDefinitions.mockReset();
    mockBulkRelationships.mockReset();
    mockNotify.mockClear();
    mockList.mockResolvedValue([REVENUE, CHURN, MARGIN]);
    mockFetchTerm.mockImplementation(async (id) => {
      if (id === 1) return REVENUE_DETAIL;
      if (id === 2) return CHURN_DETAIL;
      throw new Error(`no detail fixture for term ${id}`);
    });
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  it("renders the term list with abstract/deprecated badges and ref counts", async () => {
    render(<GlossaryTab />);

    expect(await screen.findByTestId("glossary-item-1")).toBeInTheDocument();
    expect(mockList).toHaveBeenCalledWith("", true);

    const revenue = screen.getByTestId("glossary-item-1");
    expect(within(revenue).getByText("Revenue")).toBeInTheDocument();
    expect(within(revenue).getByText("1")).toBeInTheDocument();

    const churn = screen.getByTestId("glossary-item-2");
    expect(within(churn).getByText(t("glossaryTab.abstract"))).toBeInTheDocument();

    const margin = screen.getByTestId("glossary-item-3");
    expect(within(margin).getByText(t("glossaryTab.deprecated"))).toBeInTheDocument();
  });

  it("passes the search text and the deprecated toggle to the list endpoint", async () => {
    render(<GlossaryTab />);
    await screen.findByTestId("glossary-item-1");

    mockList.mockResolvedValue([REVENUE]);
    fireEvent.change(screen.getByTestId("glossary-search"), { target: { value: "rev" } });
    await waitFor(() => expect(mockList).toHaveBeenCalledWith("rev", true));
    await waitFor(() => expect(screen.queryByTestId("glossary-item-3")).toBeNull());
    expect(screen.getByTestId("glossary-item-1")).toBeInTheDocument();

    mockList.mockResolvedValue([REVENUE, CHURN]);
    fireEvent.click(screen.getByTestId("glossary-hide-deprecated"));
    await waitFor(() => expect(mockList).toHaveBeenCalledWith("rev", false));
  });

  it("renames the selected term through the PATCH endpoint and refetches", async () => {
    mockUpdate.mockResolvedValue(undefined);
    render(<GlossaryTab />);

    fireEvent.click(await screen.findByTestId("glossary-item-1"));
    await screen.findByTestId("glossary-name-input");
    expect(screen.getByTestId("glossary-name-input")).toHaveValue("Revenue");

    // Unchanged name keeps the rename disabled.
    expect(screen.getByTestId("glossary-rename-btn")).toBeDisabled();

    fireEvent.change(screen.getByTestId("glossary-name-input"), {
      target: { value: "Net Revenue" },
    });
    fireEvent.click(screen.getByTestId("glossary-rename-btn"));

    await waitFor(() => expect(mockUpdate).toHaveBeenCalledWith(1, { name: "Net Revenue" }));
    // Mutation refetches the detail and the list.
    await waitFor(() => expect(mockFetchTerm).toHaveBeenCalledTimes(2));
  });

  it("drafts a definition with the AI button without persisting it", async () => {
    mockGenerate.mockResolvedValue("Money earned from sales.");
    render(<GlossaryTab />);

    fireEvent.click(await screen.findByTestId("glossary-item-1"));
    await screen.findByTestId("glossary-definition-input");

    fireEvent.click(screen.getByTestId("description-field-generate"));
    await waitFor(() => expect(mockGenerate).toHaveBeenCalledWith(1));

    // The draft lands in the editor only; saving is a separate, explicit PATCH.
    const input = within(screen.getByTestId("glossary-definition-input")).getByRole("textbox");
    await waitFor(() => expect(input).toHaveValue("Money earned from sales."));
    expect(mockUpdate).not.toHaveBeenCalled();
  });

  it("offers exactly the four closed rel_type values in the add-edge form", async () => {
    render(<GlossaryTab />);
    fireEvent.click(await screen.findByTestId("glossary-item-1"));
    await screen.findByTestId("glossary-edge-rel-select");

    const listbox = await openSelect("glossary-edge-rel-select");
    const options = within(listbox).getAllByRole("option", { hidden: true });
    expect(options.map((o) => o.textContent)).toEqual([
      t("glossaryTab.rel_KIND_OF"),
      t("glossaryTab.rel_RELATED_TO"),
      t("glossaryTab.rel_PART_OF"),
      t("glossaryTab.rel_SYNONYM_OF"),
    ]);
  });

  it("disables delete for a term with refs and surfaces the server 400 otherwise", async () => {
    render(<GlossaryTab />);

    // Revenue has a physical ref: delete is disabled.
    fireEvent.click(await screen.findByTestId("glossary-item-1"));
    await screen.findByTestId("glossary-delete-btn");
    expect(screen.getByTestId("glossary-delete-btn")).toBeDisabled();

    // Churn is refless: delete is enabled, and a server 400 lands in the error alert.
    fireEvent.click(screen.getByTestId("glossary-item-2"));
    await waitFor(() => expect(screen.getByTestId("glossary-delete-btn")).toBeEnabled());
    vi.spyOn(window, "confirm").mockReturnValue(true);
    mockDelete.mockRejectedValue(new Error("term 2 still has refs"));

    fireEvent.click(screen.getByTestId("glossary-delete-btn"));
    await waitFor(() => expect(mockDelete).toHaveBeenCalledWith(2));
    expect(await screen.findByTestId("glossary-error")).toHaveTextContent(
      "term 2 still has refs",
    );
  });

  it("toggles export exclusion through the PATCH endpoint", async () => {
    mockUpdate.mockResolvedValue(undefined);
    render(<GlossaryTab />);

    fireEvent.click(await screen.findByTestId("glossary-item-1"));
    const checkbox = await screen.findByTestId("glossary-export-excluded-checkbox");
    expect(checkbox).not.toBeChecked();

    fireEvent.click(checkbox);
    await waitFor(() =>
      expect(mockUpdate).toHaveBeenCalledWith(1, { export_excluded: true }),
    );
  });

  it("bulk-generates definitions and shows a count notification", async () => {
    mockBulkDefinitions.mockResolvedValue({ generated: 3 });
    render(<GlossaryTab />);
    await screen.findByTestId("glossary-item-1");

    fireEvent.click(screen.getByTestId("glossary-bulk-definitions-btn"));
    await waitFor(() => expect(mockBulkDefinitions).toHaveBeenCalled());
    await waitFor(() =>
      expect(mockNotify).toHaveBeenCalledWith(
        expect.objectContaining({ message: t("glossaryTab.definitionsGenerated_other", { count: 3 }) }),
      ),
    );
  });

  it("bulk-generates relationships and shows a count notification", async () => {
    mockBulkRelationships.mockResolvedValue({ added: 1 });
    render(<GlossaryTab />);
    await screen.findByTestId("glossary-item-1");

    fireEvent.click(screen.getByTestId("glossary-bulk-relationships-btn"));
    await waitFor(() => expect(mockBulkRelationships).toHaveBeenCalled());
    await waitFor(() =>
      expect(mockNotify).toHaveBeenCalledWith(
        expect.objectContaining({ message: t("glossaryTab.relationshipsGenerated", { count: 1 }) }),
      ),
    );
  });
});
