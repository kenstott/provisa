// Copyright (c) 2026 Kenneth Stott
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2208 (amended 2026-11-26): the Tags list shows one line per tag with counts; the detail panel
// lists where it is applied and what depends on it, grouped and collapsible, each name a link.

import { beforeEach, describe, expect, it, vi } from "vitest";
import { fireEvent, render, screen, within } from "../../../test-utils/render";
import type { Tag } from "../../../types/admin";
import type { ContextTagUse } from "../../../api/contextViews";

const auth = vi.hoisted(() => ({
  capabilities: [] as string[],
  role: { id: "analyst" },
}));
vi.mock("../../../context/AuthContext", () => ({ useAuth: () => auth }));

const contextApi = vi.hoisted(() => ({
  fetchContextTags: vi.fn(),
  fetchContextClassification: vi.fn(),
}));
vi.mock("../../../api/contextViews", () => contextApi);

const tag = (id: string): Tag => ({
  id,
  description: `${id} description`,
  appliesTo: ["source", "table", "column"],
  isSystem: false,
  derived: false,
  reasonPolicy: "required",
  expiresPolicy: "optional",
  paramPolicy: "none",
  requiresCitation: false,
  required: false,
  allowedDomains: [],
  citations: [],
  needsReview: false,
  paramValues: [],
});

vi.mock("../../../hooks/useTagQueries", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../../hooks/useTagQueries")>()),
  useTags: () => ({ tags: [tag("pii"), tag("unused")], loading: false, refetch: vi.fn() }),
  useTagAssignments: () => ({ tagAssignments: [], loading: false }),
}));
vi.mock("../../../hooks/useAdminQueries", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../../hooks/useAdminQueries")>()),
  useDomains: () => ({ domains: [], loading: false, error: undefined, refetch: vi.fn() }),
}));

import { TagsTab } from "../TagsTab";

const KINDS = ["us_ssn", "email_address", "iban"];

const PII: ContextTagUse = {
  id: "pii",
  usages: [
    { tableId: 1, tableRef: "crm.public.customers", column: null },
    { tableId: 1, tableRef: "crm.public.customers", column: "email" },
    { tableId: 1, tableRef: "crm.public.customers", column: "ssn" },
    { tableId: 2, tableRef: "erp.public.vendors", column: "tax_id" },
    { tableId: 3, tableRef: "hr.core.staff", column: null },
    { tableId: 3, tableRef: "hr.core.staff", column: "phone" },
  ],
  hiddenUsages: 0,
  rowFilters: 1,
  classificationKinds: KINDS,
};
const UNUSED: ContextTagUse = {
  id: "unused",
  usages: [],
  hiddenUsages: 0,
  rowFilters: 0,
  classificationKinds: [],
};

beforeEach(() => {
  vi.clearAllMocks();
  window.localStorage.clear();
  auth.capabilities = [
    "glossary_read",
    "table_registration",
    "access_config",
    "source_registration",
  ];
  contextApi.fetchContextTags.mockResolvedValue({ tags: [PII, UNUSED] });
});

const openPii = async () => {
  render(<TagsTab readOnly title="Tags" />);
  await screen.findByTestId("tags-where-summary-pii");
  fireEvent.click(screen.getByTestId("tags-row-pii"));
};

describe("the Tags list: one line per tag with counts", () => {
  it("counts the objects and their sources, and the dependents by why", async () => {
    render(<TagsTab readOnly title="Tags" />);
    expect(await screen.findByTestId("tags-where-summary-pii")).toHaveTextContent(
      "6 objects in 3 sources",
    );
    expect(screen.getByTestId("tags-dependents-summary-pii")).toHaveTextContent(
      "3 classifications · 1 row rule",
    );
    // No name is listed in the row.
    const row = screen.getByTestId("tags-row-pii");
    expect(row).not.toHaveTextContent("customers");
    expect(within(row).queryByRole("link")).toBeNull();
  });

  it("says nowhere and nothing for an unused tag", async () => {
    render(<TagsTab readOnly title="Tags" />);
    expect(await screen.findByTestId("tags-where-summary-unused")).toHaveTextContent("Nowhere");
    expect(screen.getByTestId("tags-dependents-summary-unused")).toHaveTextContent("Nothing");
  });

  it("keeps the reason policy out of the ID cell", async () => {
    render(<TagsTab readOnly title="Tags" />);
    const row = await screen.findByTestId("tags-row-pii");
    expect(row).not.toHaveTextContent("Reason");
  });
});

describe("the tag's panel: grouped, collapsible, each name a link", () => {
  it("groups where it is applied by object kind, then source", async () => {
    await openPii();
    expect(screen.getByTestId("tags-where-pii-tables-toggle")).toHaveTextContent("Tables (2)");
    expect(screen.getByTestId("tags-where-pii-columns-toggle")).toHaveTextContent("Columns (4)");
    const tables = screen.getByTestId("tags-where-pii-tables");
    expect(within(tables).getByTestId("tags-where-pii-tables-source-crm")).toHaveTextContent(
      "public.customers",
    );
    expect(within(tables).getByTestId("tags-where-pii-tables-source-hr")).toHaveTextContent(
      "core.staff",
    );
    const crmColumns = screen.getByTestId("tags-where-pii-columns-source-crm");
    expect(crmColumns).toHaveTextContent("public.customers.email");
    expect(crmColumns).toHaveTextContent("public.customers.ssn");
  });

  it("links each source, table and column", async () => {
    await openPii();
    const crm = screen.getByTestId("tags-where-pii-columns-source-crm");
    expect(within(crm).getByRole("link", { name: "crm" })).toHaveAttribute(
      "href",
      "/sources?expanded=crm",
    );
    expect(within(crm).getByRole("link", { name: "public.customers.email" })).toHaveAttribute(
      "href",
      expect.stringContaining("column=email"),
    );
    const tables = screen.getByTestId("tags-where-pii-tables-source-hr");
    expect(within(tables).getByRole("link", { name: "core.staff" })).toHaveAttribute(
      "href",
      "/tables?table=3",
    );
  });

  it("groups what depends on it by why, each header with its count", async () => {
    await openPii();
    expect(screen.getByTestId("tags-dependents-pii-classification-toggle")).toHaveTextContent(
      "Assigned by classification (3)",
    );
    expect(screen.getByTestId("tags-dependents-pii-row-rules-toggle")).toHaveTextContent(
      "Row rules (1)",
    );
    expect(screen.getByTestId("tags-dependents-pii-row-rules-link")).toHaveAttribute(
      "href",
      "/security/rls",
    );
  });

  it("holds the classifications closed until opened, then filters them", async () => {
    await openPii();
    const toggle = screen.getByTestId("tags-dependents-pii-classification-toggle");
    expect(toggle).toHaveAttribute("aria-expanded", "false");
    // The other groups open as default.
    expect(screen.getByTestId("tags-dependents-pii-row-rules-toggle")).toHaveAttribute(
      "aria-expanded",
      "true",
    );
    fireEvent.click(toggle);
    expect(toggle).toHaveAttribute("aria-expanded", "true");
    expect(screen.getByTestId("tags-classification-pii-us_ssn")).toHaveAttribute(
      "href",
      "/context/classification?kind=us_ssn",
    );
    fireEvent.change(screen.getByTestId("tags-classifications-filter-pii"), {
      target: { value: "social" },
    });
    expect(screen.getByTestId("tags-classification-pii-us_ssn")).toBeInTheDocument();
    expect(screen.queryByTestId("tags-classification-pii-iban")).toBeNull();
    expect(screen.queryByTestId("tags-classification-pii-email_address")).toBeNull();
  });

  it("collapses a group of where it is applied", async () => {
    await openPii();
    const toggle = screen.getByTestId("tags-where-pii-columns-toggle");
    expect(toggle).toHaveAttribute("aria-expanded", "true");
    fireEvent.click(toggle);
    expect(toggle).toHaveAttribute("aria-expanded", "false");
  });

  it("shows the reason policy in its template section", async () => {
    await openPii();
    const template = screen.getByTestId("tags-template-read-pii");
    expect(template).toHaveTextContent("Reason");
    expect(template).toHaveTextContent("Required");
  });
});
