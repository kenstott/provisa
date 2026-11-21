// Copyright (c) 2026 Kenneth Stott
// Canary: 4d1e9a37-58c2-4f0b-a6e3-9b7c2d5f8e10
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2259: the Context group's Metrics, Tags and Classification pages are read-only views of the
// components that edit them. Asserted here: no control that writes; a table or column the viewer
// does not read is counted, never named; a tag shows what depends on it; a link to another page is
// drawn only when the viewer holds that page's right.

import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, within } from "../test-utils/render";
import userEvent from "@testing-library/user-event";
import type { Metric, Tag } from "../types/admin";

const auth = vi.hoisted(() => ({
  capabilities: [] as string[],
  role: { id: "analyst" },
}));
vi.mock("../context/AuthContext", () => ({ useAuth: () => auth }));

const contextApi = vi.hoisted(() => ({
  fetchContextTags: vi.fn(),
  fetchContextClassification: vi.fn(),
}));
vi.mock("../api/contextViews", () => contextApi);

const profilerApi = vi.hoisted(() => ({
  fetchClassificationKinds: vi.fn(),
  setEnabledClassificationKinds: vi.fn(),
}));
vi.mock("../api/profiler", () => profilerApi);

const listGlossaryTerms = vi.hoisted(() => vi.fn(async () => []));
vi.mock("../api/glossary", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../api/glossary")>()),
  listGlossaryTerms,
}));

const tag = (id: string, extra: Partial<Tag> = {}): Tag => ({
  id,
  description: `${id} description`,
  appliesTo: ["source", "table", "column"],
  isSystem: false,
  derived: false,
  reasonPolicy: "optional",
  expiresPolicy: "optional",
  paramPolicy: "none",
  requiresCitation: false,
  required: false,
  allowedDomains: [],
  citations: [],
  needsReview: false,
  paramValues: [],
  ...extra,
});
const TAGS: Tag[] = [tag("pii", { isSystem: true }), tag("finance")];

vi.mock("../hooks/useTagQueries", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../hooks/useTagQueries")>()),
  useTags: () => ({ tags: TAGS, loading: false, refetch: vi.fn() }),
  useTagAssignments: () => ({ tagAssignments: [], loading: false }),
}));

const METRICS: Metric[] = [
  {
    name: "revenue",
    expression: "SUM(orders.amount)",
    datatype: "numeric",
    description: "Total revenue",
    aiContext: null,
    visibleTo: null,
    fromFact: "orders",
    term: null,
  },
];
vi.mock("../hooks/useAdminQueries", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../hooks/useAdminQueries")>()),
  useMetrics: () => ({ metrics: METRICS, loading: false, error: undefined, refetch: vi.fn() }),
  useTables: () => ({ tables: [], loading: false, error: undefined, refetch: vi.fn() }),
  useDomains: () => ({ domains: [], loading: false, error: undefined, refetch: vi.fn() }),
}));

import { TagsTab } from "../components/admin/TagsTab";
import { ClassificationKindsTab } from "../components/admin/ClassificationKindsTab";
import { MetricsPage } from "../pages/MetricsPage";

const USES = {
  tags: [
    {
      id: "pii",
      usages: [{ tableId: 2, tableRef: "crm.public.customers", column: "email" }],
      hiddenUsages: 3,
      rowFilters: 0,
      classificationKinds: ["us_ssn"],
    },
    {
      id: "finance",
      usages: [{ tableId: 1, tableRef: "erp.public.ledger", column: null }],
      hiddenUsages: 0,
      rowFilters: 2,
      classificationKinds: [],
    },
  ],
};

const KINDS = {
  kinds: [
    {
      id: "us_ssn",
      sensitive: true,
      threshold: 0.9,
      maskPreset: "ssn_last4",
      enabled: true,
      detections: [{ tableId: 2, tableRef: "crm.public.customers", column: "ssn" }],
      hiddenDetections: 4,
    },
  ],
};

beforeEach(() => {
  vi.clearAllMocks();
  auth.capabilities = ["glossary_read"];
  contextApi.fetchContextTags.mockResolvedValue(USES);
  contextApi.fetchContextClassification.mockResolvedValue(KINDS);
});

describe("Context Tags (read-only TagsTab)", () => {
  it("withholds every control that writes", async () => {
    render(<TagsTab readOnly />);
    await screen.findByTestId("tags-where-finance");
    expect(screen.queryByTestId("tags-create-button")).toBeNull();
    expect(screen.queryByTestId("tags-delete-finance")).toBeNull();
    expect(screen.queryByLabelText("Edit tag finance")).toBeNull();
    expect(screen.queryByRole("checkbox")).toBeNull();
  });

  it("names the places the viewer reads and only counts the rest", async () => {
    render(<TagsTab readOnly />);
    const pii = await screen.findByTestId("tags-where-pii");
    expect(pii).toHaveTextContent("crm.public.customers.email");
    expect(within(pii).getByTestId("tags-where-pii-hidden")).toHaveTextContent(
      "3 more you cannot read",
    );
  });

  it("shows the row filters and the classification that depend on a tag", async () => {
    render(<TagsTab readOnly />);
    expect(await screen.findByTestId("tags-dependents-finance")).toHaveTextContent("2 row filters");
    expect(screen.getByTestId("tags-dependents-pii")).toHaveTextContent(
      "Assigned by classification: US social security number",
    );
  });

  it("draws links only when the viewer holds the target page's right", async () => {
    render(<TagsTab readOnly />);
    await screen.findByTestId("tags-where-finance");
    expect(screen.getByTestId("tags-link-finance")).not.toHaveAttribute("href");
    expect(screen.getByTestId("tags-where-finance-place")).not.toHaveAttribute("href");
    // The classification page is glossary_read's: the viewer may follow that link.
    expect(screen.getByTestId("tags-classification-pii-us_ssn")).toHaveAttribute(
      "href",
      "/context/classification",
    );
  });

  it("links the tag and its tables to their editors for a viewer who edits them", async () => {
    auth.capabilities = ["glossary_read", "table_registration"];
    render(<TagsTab readOnly />);
    await screen.findByTestId("tags-where-finance");
    expect(screen.getByTestId("tags-link-finance")).toHaveAttribute(
      "href",
      "/admin/tags?tag=finance",
    );
    expect(screen.getByTestId("tags-where-finance-place")).toHaveAttribute(
      "href",
      "/tables?table=1",
    );
  });

  it("the Admin page still edits", () => {
    auth.capabilities = ["table_registration"];
    render(<TagsTab />);
    expect(screen.getByTestId("tags-create-button")).toBeInTheDocument();
    expect(contextApi.fetchContextTags).not.toHaveBeenCalled();
  });
});

describe("Context Classification (read-only ClassificationKindsTab)", () => {
  it("lists the kinds and where each was detected, without the switch", async () => {
    render(<ClassificationKindsTab readOnly />);
    const row = await screen.findByTestId("classification-kind-us_ssn");
    expect(screen.queryByRole("switch")).toBeNull();
    expect(screen.queryByTestId("classification-kind-enabled-us_ssn")).toBeNull();
    expect(within(row).getByTestId("classification-kind-state-us_ssn")).toHaveTextContent(
      "Enabled",
    );
    const detected = within(row).getByTestId("classification-kind-detected-us_ssn");
    expect(detected).toHaveTextContent("crm.public.customers.ssn");
    expect(detected).toHaveTextContent("4 more you cannot read");
    expect(profilerApi.fetchClassificationKinds).not.toHaveBeenCalled();
  });

  it("links a detection to its column only with the table page's right", async () => {
    render(<ClassificationKindsTab readOnly />);
    const place = await screen.findByTestId("classification-kind-detected-us_ssn-place");
    expect(place).not.toHaveAttribute("href");
  });
});

describe("Context Metrics (read-only MetricsPage)", () => {
  it("withholds create, import, bulk delete and the panel's edits", async () => {
    render(<MetricsPage readOnly />);
    expect(screen.queryByTestId("metrics-new-button")).toBeNull();
    expect(screen.queryByTestId("metrics-bulk")).toBeNull();
    expect(screen.queryByRole("checkbox")).toBeNull();
    await userEvent.click(screen.getByTestId("metrics-row-revenue"));
    expect(screen.getByTestId("metric-detail-revenue")).toBeInTheDocument();
    expect(screen.queryByTestId("metric-detail-edit-revenue")).toBeNull();
    expect(screen.queryByTestId("metric-detail-delete-revenue")).toBeNull();
    expect(screen.queryByTestId("metric-detail-duplicate-revenue")).toBeNull();
    expect(listGlossaryTerms).not.toHaveBeenCalled();
  });

  it("links the metric to its editor only with the editor's right", async () => {
    render(<MetricsPage readOnly />);
    await userEvent.click(screen.getByTestId("metrics-row-revenue"));
    expect(screen.getByTestId("metric-link-revenue")).not.toHaveAttribute("href");
  });
});
