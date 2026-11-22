// Copyright (c) 2026 Kenneth Stott
// Canary: 3c1e7b52-9a4d-4f08-8e6b-2d5f0a7c9e13
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1387: a drafted definition's result names the bound tables that gave no samples. After one
// term's draft and after "define all", the curator is told which; nothing is said when none.

import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent, waitFor, within } from "../test-utils/render";
import { notifications } from "@mantine/notifications";
import i18n from "../i18n";
import { GlossaryTab } from "../components/admin/GlossaryTab";
import { TermEditForm } from "../components/admin/TermEditForm";
import type { GlossaryTermDetail, GlossaryTermSummary } from "../api/glossary";

vi.mock("../components/graph3d/OntologyGraph", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../components/graph3d/OntologyGraph")>()),
  OntologySnippet: () => null,
  OntologyView: () => <div data-testid="ontology-view" />,
}));
vi.mock("../components/admin/FindTermsDialog", () => ({
  FindTermsDialog: () => <div data-testid="find-terms-dialog" />,
}));
vi.mock("../api/glossary", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../api/glossary")>()),
  listAttributablePeople: vi.fn(async () => []),
  listGlossaryTerms: vi.fn(),
  fetchGlossaryTerm: vi.fn(),
  fetchGlossaryFindings: vi.fn(async () => []),
  listTermTables: vi.fn(async () => []),
  generateGlossaryDefinition: vi.fn(),
  generateAllGlossaryDefinitions: vi.fn(),
  generateGlossaryRelationships: vi.fn(),
}));
vi.mock("../hooks/useTagQueries", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../hooks/useTagQueries")>()),
  useUnassignTag: () => ({ unassignTag: vi.fn(), loading: false }),
  useAssignTag: () => ({ assignTag: vi.fn(), loading: false }),
}));
vi.mock("@mantine/notifications", () => ({ notifications: { show: vi.fn() } }));
vi.mock("../context/AuthContext", () => ({
  useAuth: () => ({ capabilities: ["glossary_read", "glossary_rw"], activeOrgId: "acme" }),
}));
const filter = {
  domains: ["sales", "hr"],
  setDomains: () => {},
  selectedDomain: "all",
  setSelectedDomain: () => {},
  checkedDomains: new Set<string>(["sales"]),
  toggleDomain: () => {},
  ensureDomainChecked: () => {},
  domainsEnabled: true,
};
vi.mock("../context/DomainFilterContext", () => ({ useDomainFilter: () => filter }));

import {
  generateAllGlossaryDefinitions,
  generateGlossaryDefinition,
  listGlossaryTerms,
} from "../api/glossary";

const t = i18n.getFixedT("en");

const TERM = {
  id: 1,
  name: "customer",
  definition: "",
  status: "draft",
  is_abstract: false,
  domains: ["sales", "hr"],
  refs: [],
  edges_out: [],
  edges_in: [],
  experts: [],
  tags: [],
  pending_on: [],
} as unknown as GlossaryTermDetail;

function renderForm() {
  render(
    <TermEditForm
      term={TERM}
      otherTermOptions={[]}
      relTypeOptions={[]}
      kindOptions={[]}
      memberOptions={[]}
      domainOptions={[]}
      domainsEnabled
      relLabel={(_options, relType) => relType}
      navigateToTerm={() => {}}
      onSaved={async () => {}}
      onCancel={() => {}}
      onTermCreated={() => {}}
    />,
  );
}

async function defineAll() {
  render(<GlossaryTab />);
  await screen.findByText("customer");
  fireEvent.click(screen.getByTestId("glossary-generate-menu"));
  fireEvent.click(await screen.findByTestId("glossary-bulk-definitions-btn"));
}

beforeEach(() => {
  vi.clearAllMocks();
  filter.checkedDomains = new Set(["sales"]);
  vi.mocked(listGlossaryTerms).mockResolvedValue([
    { id: 1, name: "customer", status: "draft" } as GlossaryTermSummary,
  ]);
});

describe("a term's drafted definition names the tables that gave no samples", () => {
  it("names them under the definition", async () => {
    vi.mocked(generateGlossaryDefinition).mockResolvedValue({
      definition: "One who buys.",
      tables_without_samples: ["sales.orders", "sales.refunds"],
    });
    renderForm();
    fireEvent.click(await screen.findByTestId("description-field-generate"));
    expect(await screen.findByTestId("glossary-definition-unsampled")).toHaveTextContent(
      t("glossaryTab.tablesWithoutSamples", { tables: "sales.orders, sales.refunds" }),
    );
  });

  it("says nothing when every table gave samples", async () => {
    vi.mocked(generateGlossaryDefinition).mockResolvedValue({
      definition: "One who buys.",
      tables_without_samples: [],
    });
    renderForm();
    fireEvent.click(await screen.findByTestId("description-field-generate"));
    const input = within(screen.getByTestId("glossary-definition-input")).getByRole("textbox");
    await waitFor(() => expect(input).toHaveValue("One who buys."));
    expect(screen.queryByTestId("glossary-definition-unsampled")).toBeNull();
  });
});

describe('"define all" names the tables that gave no samples', () => {
  it("names them in a notice that stays", async () => {
    vi.mocked(generateAllGlossaryDefinitions).mockResolvedValue({
      generated: 2,
      tables_without_samples: ["hr.people", "sales.orders"],
    });
    await defineAll();
    await waitFor(() =>
      expect(notifications.show).toHaveBeenCalledWith({
        color: "yellow",
        autoClose: false,
        message: t("glossaryTab.tablesWithoutSamples", { tables: "hr.people, sales.orders" }),
      }),
    );
  });

  it("shows no such notice when every table gave samples", async () => {
    vi.mocked(generateAllGlossaryDefinitions).mockResolvedValue({
      generated: 2,
      tables_without_samples: [],
    });
    await defineAll();
    await waitFor(() => expect(notifications.show).toHaveBeenCalled());
    expect(notifications.show).toHaveBeenCalledTimes(1);
    expect(notifications.show).not.toHaveBeenCalledWith(
      expect.objectContaining({ color: "yellow" }),
    );
  });
});
