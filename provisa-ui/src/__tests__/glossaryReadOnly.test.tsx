// Copyright (c) 2026 Kenneth Stott
// Canary: 7f1c2d8a-63b4-4d1e-9a07-5c4e08b2f931
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1590: what a holder of `glossary_read` alone is shown. An analyst comes to the glossary to
// find out what a column means, so the whole surface still renders — terms, definitions, refs,
// relationships, experts. Every control that writes is WITHHELD rather than disabled: the endpoint
// behind it answers 403, and a greyed-out button says nothing about why it is greyed out. The
// curator's view of the same tab is GlossaryTab.test.tsx.

import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent, within } from "../test-utils/render";
import i18n from "../i18n";
import { GlossaryTab } from "../components/admin/GlossaryTab";
import type { GlossaryTermDetail, GlossaryTermSummary } from "../api/glossary";

const t = i18n.getFixedT("en");

vi.mock("../api/glossary", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../api/glossary")>()),
  listGlossaryTerms: vi.fn(),
  fetchGlossaryTerm: vi.fn(),
}));

vi.mock("@mantine/notifications", () => ({ notifications: { show: vi.fn() } }));

// The right under test. Mutating it per-case is what makes "withheld from a reader, present for a
// curator" a single assertion pair rather than two suites that could drift apart.
const auth = { capabilities: ["glossary_read"] as string[] };
vi.mock("../context/AuthContext", () => ({ useAuth: () => auth }));

import { fetchGlossaryTerm, listGlossaryTerms } from "../api/glossary";

const mockList = vi.mocked(listGlossaryTerms);
const mockFetchTerm = vi.mocked(fetchGlossaryTerm);

const REVENUE: GlossaryTermSummary = {
  id: 1,
  name: "Revenue",
  definition: "Money in.",
  is_abstract: false,
  deprecated: false,
  ref_count: 1,
  export_excluded: false,
  retired: false,
  live: true,
  domains: [],
};
const CHURN: GlossaryTermSummary = {
  ...REVENUE,
  id: 2,
  name: "Churn",
  definition: null,
  ref_count: 0,
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
  edges_in: [{ term_id: 2, rel_type: "PART_OF", name: "Churn" }],
  experts: [{ user_id: "u1", kind: "expert" }],
};

/** Mount the tab and open Revenue. */
async function openRevenue() {
  render(<GlossaryTab />);
  fireEvent.click(await screen.findByTestId("glossary-item-1"));
  // The name input is in both views, so it is what proves the detail arrived — waiting on the
  // panel itself would pass before the fetch resolved and make every absence assertion vacuous.
  await screen.findByTestId("glossary-name-input");
}

// Every control that reaches a `glossary_rw` endpoint.
const WRITE_CONTROLS = [
  "glossary-new-btn",
  "glossary-bulk-definitions-btn",
  "glossary-bulk-relationships-btn",
  "glossary-rename-btn",
  "glossary-retire-btn",
  "glossary-delete-btn",
  "glossary-definition-save-btn",
  "glossary-move-select-10-revenue",
  "glossary-edge-out-rel-2",
  "glossary-edge-in-rel-2",
  "glossary-edge-rel-select",
  "glossary-edge-term-select",
  "glossary-edge-add-btn",
  "glossary-expert-user-input",
  "glossary-expert-kind-select",
  "glossary-expert-add-btn",
];

describe("glossary read-only mode", () => {
  beforeEach(() => {
    auth.capabilities = ["glossary_read"];
    mockList.mockReset();
    mockFetchTerm.mockReset();
    mockList.mockResolvedValue([REVENUE, CHURN]);
    mockFetchTerm.mockResolvedValue(REVENUE_DETAIL);
  });

  it("withholds every control that writes", async () => {
    await openRevenue();
    for (const testid of WRITE_CONTROLS) {
      expect(screen.queryByTestId(testid), testid).toBeNull();
    }
  });

  it("shows the same controls to a curator", async () => {
    // The other half of the assertion above: withheld because the right is missing, not because
    // the testid moved or the control was deleted outright.
    auth.capabilities = ["glossary_read", "glossary_rw"];
    await openRevenue();
    for (const testid of WRITE_CONTROLS) {
      expect(screen.queryByTestId(testid), testid).not.toBeNull();
    }
    expect(screen.queryByTestId("glossary-read-only")).toBeNull();
  });

  it("says why the curation controls are absent", async () => {
    await openRevenue();
    expect(screen.getByTestId("glossary-read-only")).toHaveTextContent(t("glossaryTab.readOnly"));
  });

  it("still renders the term, its definition, refs, relationships and experts", async () => {
    await openRevenue();

    expect(screen.getByTestId("glossary-name-input")).toHaveValue("Revenue");
    expect(screen.getByTestId("glossary-detail")).toHaveTextContent("Money in.");
    expect(screen.getByTestId("glossary-ref-10-revenue")).toBeInTheDocument();
    expect(screen.getByTestId("glossary-edge-out-2-RELATED_TO")).toBeInTheDocument();
    expect(screen.getByTestId("glossary-edge-in-2-PART_OF")).toBeInTheDocument();
    expect(screen.getByTestId("glossary-expert-u1")).toBeInTheDocument();
  });

  it("reads a relationship as the sentence the picker would have shown", async () => {
    // The picker is gone, so the type has to be spelled out — an unlabelled "RELATED_TO" would be
    // the one part of the page a reader came for that the read-only view lost.
    await openRevenue();
    const outgoing = screen.getByTestId("glossary-edge-out-2-RELATED_TO");
    expect(within(outgoing).getByText(t("glossaryTab.rel_RELATED_TO"))).toBeInTheDocument();
    const incoming = screen.getByTestId("glossary-edge-in-2-PART_OF");
    expect(within(incoming).getByText(t("glossaryTab.rel_PART_OF_reverse"))).toBeInTheDocument();
  });

  it("leaves the name and the export toggle unwritable", async () => {
    // These two are inputs rather than buttons — there is nothing to withhold without taking the
    // value itself off the page, so they render read-only instead.
    await openRevenue();
    expect(screen.getByTestId("glossary-name-input")).toHaveAttribute("readonly");
    expect(screen.getByTestId("glossary-export-excluded-checkbox")).toBeDisabled();
  });

  it("keeps searching and filtering, which write nothing", async () => {
    await openRevenue();
    expect(screen.getByTestId("glossary-search")).toBeInTheDocument();
    expect(screen.getByTestId("glossary-hide-deprecated")).toBeInTheDocument();
  });
});
