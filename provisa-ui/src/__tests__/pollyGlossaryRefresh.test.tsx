// Copyright (c) 2026 Kenneth Stott
// Canary: 5e2c8a17-4b9d-4f36-a1e8-7c3d9b2f6a40
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// A glossary change Polly makes refreshes the open term's panel and the list the way a person's
// edit does, whether she made it with a server tool (add_glossary_term_edge) or with admin_request
// to a glossary route; and read_screen reports the term on screen by its id and domains, with what
// its panel shows, so she checks her change against that term and not another of its name
// (REQ-1591: two domains may each hold a term named neighborhood).

import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { render, screen, waitFor } from "../test-utils/render";
import { GlossaryTab } from "../components/admin/GlossaryTab";
import type { GlossaryTermDetail, GlossaryTermSummary } from "../api/glossary";
import { executeClientTool, type ClientToolContext } from "../mcp/clientTools";
import { adminRequestSent, serverToolRan } from "../mcp/pollyWrites";

vi.mock("../components/graph3d/OntologyGraph", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../components/graph3d/OntologyGraph")>()),
  OntologySnippet: () => null,
}));
vi.mock("../api/glossary", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../api/glossary")>()),
  listAttributablePeople: vi.fn(async () => []),
  listGlossaryTerms: vi.fn(),
  fetchGlossaryTerm: vi.fn(),
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
// read_screen names the data node; this UI is not coordinator-served.
vi.mock("../lib/dataAddress", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../lib/dataAddress")>()),
  currentDataAddress: async () => null,
}));

import { fetchGlossaryTerm, listGlossaryTerms } from "../api/glossary";

const mockList = vi.mocked(listGlossaryTerms);
const mockFetchTerm = vi.mocked(fetchGlossaryTerm);

const summary = (
  id: number,
  name: string,
  domains: string[],
  status: GlossaryTermSummary["status"],
  definition: string | null,
): GlossaryTermSummary => ({
  id,
  name,
  definition,
  calculation_method: null,
  is_abstract: false,
  deprecated: false,
  ref_count: 1,
  table_count: 0,
  identity_gap: false,
  related_to: 0,
  findings: 0,
  status,
  status_reason: null,
  status_by: null,
  retire_on: null,
  export_excluded: false,
  retired: false,
  grounding: "grounded",
  agent_visible: true,
  domains,
});
const detailOf = (term: GlossaryTermSummary): GlossaryTermDetail => ({
  ...term,
  refs: [],
  command_refs: [],
  edges_out: [],
  edges_in: [],
  experts: [],
  tags: [],
  pending_on: [],
  curatable: true,
  iri: null,
});

// The city's neighborhood, a draft with no definition, is the term on screen; the enterprise's
// neighborhood (94) is another term of that name.
const CITY = summary(41, "neighborhood", ["city"], "draft", null);
const ENTERPRISE = summary(94, "neighborhood", ["*"], "approved", "An area of a city.");
const LOCATION = summary(7, "location", ["*"], "approved", "A place.");

let details: Map<number, GlossaryTermDetail>;

const ctx = (changed: (path: string) => void): ClientToolContext => ({
  navigate: vi.fn(),
  confirm: vi.fn(async () => true),
  runMutation: vi.fn(),
  presentChoice: vi.fn(),
  runAdminMutation: vi.fn(),
  chooseFile: vi.fn(),
  changed,
});

async function openCity() {
  render(<GlossaryTab />, { initialEntries: ["/context/ontology?term=41"] });
  await waitFor(() =>
    expect(screen.getByTestId("glossary-detail-status")).toHaveTextContent(/draft/i),
  );
  expect(screen.getByText("No definition.")).toBeInTheDocument();
}

async function readScreenInputs() {
  const result = await executeClientTool("read_screen", {}, ctx(vi.fn()));
  expect(result.success).toBe(true);
  return JSON.parse(result.message).inputs as Record<string, unknown>;
}

beforeEach(() => {
  window.localStorage.clear();
  // read_screen reads the route from the window; the page's router is a MemoryRouter.
  window.history.pushState({}, "", "/context/ontology?term=41");
  // jsdom lays nothing out; every element counts as drawn.
  HTMLElement.prototype.checkVisibility = () => true;
  details = new Map([
    [41, detailOf(CITY)],
    [94, detailOf(ENTERPRISE)],
    [7, detailOf(LOCATION)],
  ]);
  mockList.mockReset();
  mockList.mockImplementation(async () => [CITY, ENTERPRISE, LOCATION]);
  mockFetchTerm.mockReset();
  mockFetchTerm.mockImplementation(async (id: number) => {
    const term = details.get(id);
    if (term === undefined) throw new Error(`no term ${id}`);
    return term;
  });
});

afterEach(() => vi.restoreAllMocks());

describe("a Polly glossary write refreshes the open term", () => {
  it("reads the open term and the list again after add_glossary_term_edge", async () => {
    await openCity();
    const reads = mockFetchTerm.mock.calls.length;
    const lists = mockList.mock.calls.length;

    // Polly's server tool adds neighborhood (41) KIND_OF location, server-side.
    details.set(41, {
      ...detailOf(CITY),
      edges_out: [{ term_id: 7, rel_type: "KIND_OF", name: "location", domains: ["*"] }],
    });
    serverToolRan("add_glossary_term_edge");

    await waitFor(() =>
      expect(screen.getByTestId("glossary-edge-out-link-7")).toHaveTextContent("location"),
    );
    expect(mockFetchTerm.mock.calls.slice(reads).map(([id]) => id)).toContain(41);
    expect(mockList.mock.calls.length).toBeGreaterThan(lists);
  });

  it("reads the open term again after admin_request changes it through a glossary route", async () => {
    await openCity();
    const refetchActive = vi.fn();
    vi.spyOn(globalThis, "fetch").mockImplementation(async () => {
      // The route defines and approves the city's neighborhood.
      details.set(41, {
        ...detailOf({ ...CITY, status: "approved", definition: "A district of the city." }),
      });
      return new Response("{}", { status: 200, statusText: "OK" });
    });

    const result = await executeClientTool(
      "admin_request",
      {
        method: "PATCH",
        path: "/admin/glossary/terms/41",
        body: { definition: "A district of the city." },
        summary: "Define neighborhood.",
      },
      ctx((path) => adminRequestSent(path, refetchActive)),
    );

    expect(result.success).toBe(true);
    expect(refetchActive).toHaveBeenCalled();
    await waitFor(() =>
      expect(screen.getByTestId("glossary-detail-status")).toHaveTextContent(/approved/i),
    );
    expect(screen.getByText("A district of the city.")).toBeInTheDocument();
  });

  it("reads the Apollo queries again, and not the glossary, after a change to another route", () => {
    const refetchActive = vi.fn();
    const heard = vi.fn();
    window.addEventListener("provisa:glossary-changed", heard);
    adminRequestSent("/admin/discover/candidates/7/accept", refetchActive);
    adminRequestSent("/admin/glossaryish", refetchActive);
    window.removeEventListener("provisa:glossary-changed", heard);
    expect(refetchActive).toHaveBeenCalledTimes(2);
    expect(heard).not.toHaveBeenCalled();
  });
});

describe("read_screen names the term on screen", () => {
  it("by its id and domains, with the status and definition its panel shows", async () => {
    await openCity();
    await waitFor(async () =>
      expect(await readScreenInputs()).toMatchObject({
        term: 41,
        termName: "neighborhood",
        termDomains: ["city"],
        termStatus: "draft",
        termDefinition: null,
      }),
    );

    // A change to the enterprise's neighborhood (94) leaves the term on screen as it was.
    details.set(94, { ...detailOf(ENTERPRISE), edges_out: [] });
    serverToolRan("add_glossary_term_edge");
    await waitFor(() => expect(mockFetchTerm).toHaveBeenLastCalledWith(41));
    expect(await readScreenInputs()).toMatchObject({ term: 41, termStatus: "draft" });
  });
});
