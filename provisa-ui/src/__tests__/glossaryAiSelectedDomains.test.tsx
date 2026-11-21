// Copyright (c) 2026 Kenneth Stott
// Canary: 8d2f6a13-4e7b-4c90-b5a1-6f3e9c0d2b74
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2253: the ontology's AI assistants are asked for the domains selected in the navbar filter,
// so the server limits what it sends the model and writes to them. Every domain checked asks for
// no narrowing (null), as the term list does.

import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent, waitFor } from "../test-utils/render";
import { GlossaryTab } from "../components/admin/GlossaryTab";
import { TermEditForm } from "../components/admin/TermEditForm";
import type { GlossaryTermDetail, GlossaryTermSummary } from "../api/glossary";

vi.mock("../components/graph3d/OntologyGraph", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../components/graph3d/OntologyGraph")>()),
  OntologySnippet: () => null,
  OntologyView: () => <div data-testid="ontology-view" />,
}));
vi.mock("../components/admin/StewardshipSummary", () => ({
  StewardshipSummary: () => <div data-testid="summary" />,
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
  generateGlossaryRelationships,
  listGlossaryTerms,
} from "../api/glossary";

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

async function openGenerateMenu() {
  render(<GlossaryTab />);
  await screen.findByText("customer");
  fireEvent.click(screen.getByTestId("glossary-generate-menu"));
}

beforeEach(() => {
  vi.clearAllMocks();
  filter.checkedDomains = new Set(["sales"]);
  vi.mocked(listGlossaryTerms).mockResolvedValue([
    { id: 1, name: "customer", status: "draft" } as GlossaryTermSummary,
  ]);
  vi.mocked(generateGlossaryRelationships).mockResolvedValue({ added: 0, review: [] });
  vi.mocked(generateAllGlossaryDefinitions).mockResolvedValue({ generated: 0, tables_without_samples: [] });
  vi.mocked(generateGlossaryDefinition).mockResolvedValue({
    definition: "One who buys.",
    tables_without_samples: [],
  });
});

describe("the ontology's AI assistants ask for the selected domains", () => {
  it("proposes relationships for the selected domains only", async () => {
    await openGenerateMenu();
    fireEvent.click(await screen.findByTestId("glossary-bulk-relationships-btn"));
    await waitFor(() =>
      expect(generateGlossaryRelationships).toHaveBeenCalledWith(new Set(["sales"])),
    );
  });

  it("fills definitions for the selected domains only", async () => {
    await openGenerateMenu();
    fireEvent.click(await screen.findByTestId("glossary-bulk-definitions-btn"));
    await waitFor(() =>
      expect(generateAllGlossaryDefinitions).toHaveBeenCalledWith(new Set(["sales"])),
    );
  });

  it("asks for no narrowing when every domain is checked", async () => {
    filter.checkedDomains = new Set(["sales", "hr"]);
    await openGenerateMenu();
    fireEvent.click(await screen.findByTestId("glossary-bulk-relationships-btn"));
    await waitFor(() => expect(generateGlossaryRelationships).toHaveBeenCalledWith(null));
  });

  it("drafts a term's definition from the selected domains only", async () => {
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
    fireEvent.click(await screen.findByTestId("description-field-generate"));
    await waitFor(() =>
      expect(generateGlossaryDefinition).toHaveBeenCalledWith(1, new Set(["sales"])),
    );
  });
});
