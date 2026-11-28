// Copyright (c) 2026 Kenneth Stott
// Canary: 5685eb19-209b-4fe5-aa22-a1071b37438a
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1923/REQ-2234: a Wikidata source just created opens on Register tables. A starter table
// chosen and Continue pressed goes on to the Tables page to be created, carrying its SPARQL query,
// name and columns: the registration is never dropped on the way, and the panel never just closes.

import { beforeEach, describe, it, expect, vi } from "vitest";
import { renderWithProviders, screen, waitFor, within } from "../../test-utils/render";
import userEvent from "@testing-library/user-event";
import { Route, Routes } from "react-router-dom";
import type { MutationResult, Source } from "../../types/admin";

const wikidataApi = vi.hoisted(() => ({
  fetchWikidataStarters: vi.fn(),
  fetchWikidataBrowse: vi.fn(),
  searchWikidata: vi.fn(),
  fetchWikidataClass: vi.fn(),
  buildWikidataClassTable: vi.fn(),
  fetchSparqlColumns: vi.fn(),
}));
vi.mock("../../api/wikidata", () => wikidataApi);

vi.mock("../../api/admin", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../api/admin")>()),
  fetchSettings: vi.fn().mockResolvedValue({
    redirect: { enabled: false, threshold: 10000, default_format: "json", ttl: 3600 },
    sampling: { default_sample_size: 1000 },
    cache: { default_ttl: 300 },
    naming: { domain_prefix: false, convention: "none" },
  }),
  fetchSourceReach: vi.fn().mockResolvedValue(null),
  fetchTableUniqueConstraints: vi.fn().mockResolvedValue([]),
}));

vi.mock("../../context/AuthContext", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../context/AuthContext")>()),
  useAuth: () => ({
    role: "admin",
    actingRoles: "admin",
    selectedRoles: ["admin"],
    capabilities: ["admin"],
    domainAccess: ["*"],
  }),
}));

vi.mock("../../context/DomainFilterContext", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../context/DomainFilterContext")>()),
  useDomainFilter: () => ({ checkedDomains: ["sales"], domainsEnabled: true }),
}));

const ok: MutationResult = { success: true, message: "" };
const source = (over: Partial<Source>): Source =>
  ({
    id: "sales-pg",
    type: "postgresql",
    host: "db",
    port: 5432,
    database: "sales",
    username: "svc",
    path: "",
    cacheEnabled: true,
    cacheTtl: null,
    changeSignal: "ttl",
    mappingJson: null,
    passwordRef: "pw",
    allowedDomains: [],
    ...over,
    setupOutcomes: { tables: null, relationships: null, query: null, decidedAt: null },
  }) as unknown as Source;

let SOURCES: Source[] = [];
const createSource = vi.fn();
const refetch = vi.fn();

vi.mock("../../hooks/useAdminQueries", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../hooks/useAdminQueries")>()),
  useSources: () => ({ sources: SOURCES, loading: false, refetch }),
  useDomains: () => ({ domains: [{ id: "sales" }], loading: false, refetch: vi.fn() }),
  useCreateSource: () => ({ createSource }),
  useUpdateSourceCache: () => ({ updateSourceCache: vi.fn().mockResolvedValue(ok) }),
  useUpdateSourceReplicate: () => ({ updateSourceReplicate: vi.fn().mockResolvedValue(ok) }),
  useUpdateSourceLoadProtection: () => ({
    updateSourceLoadProtection: vi.fn().mockResolvedValue(ok),
  }),
  useUpdateSourceNaming: () => ({ updateSourceNaming: vi.fn().mockResolvedValue(ok) }),
  useUpdateSourceAllowedDomains: () => ({
    updateSourceAllowedDomains: vi.fn().mockResolvedValue(ok),
  }),
  useSuggestTableAlias: () => ({ suggestTableAlias: async () => "" }),
  useRoles: () => ({ roles: [{ id: "admin" }], loading: false }),
  useTableDetails: () => ({ tables: [], loading: false }),
  useAvailableSchemas: () => ({ schemas: ["public"], loading: false }),
  useAvailableTables: () => ({
    tables: [
      { name: "accounts", comment: null },
      { name: "orders", comment: null },
    ],
    loading: false,
  }),
}));

const readColumns = async () => [
  { name: "id", dataType: "integer", comment: null, nativeFilterType: null, isPrimaryKey: true },
];
vi.mock("../../hooks/useAvailableColumnsMetadataLazy", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../hooks/useAvailableColumnsMetadataLazy")>()),
  useReadColumns: () => readColumns,
}));

// What the server answers to registering the picked tables: each registered, under its own id.
const registerTables = vi.fn();
vi.mock("../../hooks/useTableQueries", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../hooks/useTableQueries")>()),
  useRegisterTables: () => ({ registerTables, loading: false }),
}));

import { SourcesPage } from "../SourcesPage";
import { forgetSourceKinds } from "../sources/sourceKind";

const COUNTRIES = {
  name: "countries",
  query: "SELECT ?id ?name WHERE { }",
  columns: [
    { name: "id", data_type: "text" },
    { name: "name", data_type: "text" },
  ],
};
const CURRENCIES = {
  name: "currencies",
  query: "SELECT ?id ?name ?iso_4217 WHERE { }",
  columns: [
    { name: "id", data_type: "text" },
    { name: "name", data_type: "text" },
    { name: "iso_4217", data_type: "text" },
  ],
};
const MOUNTAINS =
  "SELECT ?peak (SAMPLE(?h) AS ?height) WHERE { ?peak wdt:P2044 ?h } GROUP BY ?peak";

function TablesProbe() {
  return <div data-testid="tables-page" />;
}

const panel = () => within(screen.getByTestId("sources-split-panel"));

beforeEach(() => {
  SOURCES = [source({})];
  createSource.mockReset().mockResolvedValue(ok);
  refetch.mockReset().mockResolvedValue({ data: { sources: SOURCES } });
  registerTables.mockReset().mockImplementation(async (inputs: { tableName: string }[]) =>
    inputs.map((input, at) => ({
      tableName: input.tableName,
      result: { success: true, message: "", params: { id: 10 + at } },
    })),
  );
  forgetSourceKinds();
  wikidataApi.fetchWikidataStarters.mockReset().mockResolvedValue([COUNTRIES, CURRENCIES]);
  wikidataApi.fetchWikidataBrowse.mockReset().mockResolvedValue([]);
  wikidataApi.fetchSparqlColumns.mockReset().mockResolvedValue([
    { name: "peak", data_type: "text" },
    { name: "height", data_type: "text" },
  ]);
  vi.spyOn(globalThis, "fetch").mockImplementation(async (url) => {
    if (String(url).includes("/admin/source-kinds/")) {
      return new Response(
        JSON.stringify({ cache_ttl_required: false, sentinel_path: true, tables_land: false }),
      );
    }
    return new Response(JSON.stringify({ configured: true, organisation_mailboxes: true }));
  });
});

// What the server lists once the Wikidata source is created: a SPARQL source branded Wikidata.
const WIKIDATA_MADE = source({
  id: "test",
  type: "sparql",
  host: "https://query.wikidata.org/sparql",
  port: 443,
  database: "",
  username: "",
  passwordRef: null,
  cacheTtl: 86400,
  federationHintsJson: JSON.stringify({ brand: "wikidata" }),
} as Partial<Source>);

async function pickStarters(...labels: RegExp[]): Promise<void> {
  const field = await panel().findByTestId("wikidata-starters-field");
  await waitFor(() => expect(wikidataApi.fetchWikidataStarters).toHaveBeenCalled());
  const input = within(field).getByRole("textbox", { hidden: true });
  for (const label of labels) {
    await userEvent.click(input);
    await waitFor(() => expect(input.getAttribute("aria-controls")).toBeTruthy());
    const listbox = document.getElementById(input.getAttribute("aria-controls") as string);
    await userEvent.click(
      within(listbox as HTMLElement).getByRole("option", { name: label, hidden: true }),
    );
  }
}

async function createWikidataSource(): Promise<void> {
  renderWithProviders(
    <Routes>
      <Route path="/sources" element={<SourcesPage />} />
      <Route path="/tables" element={<TablesProbe />} />
    </Routes>,
    { initialEntries: ["/sources"] },
  );
  await userEvent.click(await screen.findByTestId("sources-add-toggle"));
  await userEvent.click((await screen.findAllByTestId("source-type-option-wikidata"))[0]);
  await userEvent.type(await screen.findByTestId("sources-id-input"), "test");
  refetch.mockImplementation(async () => {
    SOURCES = [...SOURCES, WIKIDATA_MADE];
    return { data: { sources: SOURCES } };
  });
  await userEvent.click(screen.getByTestId("sources-submit"));
  await waitFor(() => expect(createSource).toHaveBeenCalled());
  await screen.findByTestId("source-setup-finish-later");
}

const registered = () =>
  registerTables.mock.calls[0][0] as {
    tableName: string;
    schemaName: string;
    domainId: string;
    queryTemplate: string;
    columns: { name: string }[];
  }[];

describe("a Wikidata source, created", () => {
  it("registers every starter chosen at once when Continue is pressed, with its query and columns", async () => {
    await createWikidataSource();
    await pickStarters(/Countries/, /Currencies/);
    await userEvent.selectOptions(panel().getByTestId("register-table-domain-select"), "sales");
    await userEvent.click(screen.getByTestId("register-table-submit"));

    await waitFor(() => expect(registerTables).toHaveBeenCalledTimes(1));
    const inputs = registered();
    expect(inputs.map((i) => i.tableName)).toEqual(["countries", "currencies"]);
    expect(inputs[0]).toMatchObject({
      sourceId: "test",
      domainId: "sales",
      schemaName: "sparql",
      queryTemplate: COUNTRIES.query,
    });
    expect(inputs[1].columns.map((c) => c.name)).toEqual(["id", "name", "iso_4217"]);
    // REQ-2234 (amended 2026-11-27): the step goes on to Review samples of what was registered.
    expect(await screen.findByTestId("review-samples-step")).toBeInTheDocument();
    expect(screen.queryByTestId("tables-page")).toBeNull();
  });

  it("registers a hand-written SPARQL table with the starters, its columns read from its SELECT", async () => {
    await createWikidataSource();
    await pickStarters(/Countries/);
    await userEvent.selectOptions(panel().getByTestId("register-table-domain-select"), "sales");
    await userEvent.click(
      within(panel().getByTestId("wikidata-custom-mode")).getByText("SPARQL query"),
    );
    await userEvent.type(panel().getByTestId("register-table-sparql-table-name"), "mountains");
    // fireEvent-free: the query has braces userEvent would read as key names.
    const query = panel().getByTestId("register-table-sparql-query") as HTMLTextAreaElement;
    await userEvent.click(query);
    await userEvent.paste(MOUNTAINS);
    await userEvent.click(screen.getByTestId("register-table-submit"));

    await waitFor(() => expect(registerTables).toHaveBeenCalledTimes(1));
    const inputs = registered();
    expect(inputs.map((i) => i.tableName)).toEqual(["countries", "mountains"]);
    expect(inputs[1].queryTemplate).toBe(MOUNTAINS);
    expect(inputs[1].columns.map((c) => c.name)).toEqual(["peak", "height"]);
    expect(wikidataApi.fetchSparqlColumns).toHaveBeenCalledWith(MOUNTAINS);
  });

  it("asks for a domain before Continue registers anything", async () => {
    await createWikidataSource();
    await pickStarters(/Countries/);
    // REQ-1968 (amended 2026-11-22): Continue is not disabled; pressed, it marks Domain inline.
    const submit = screen.getByTestId("register-table-submit");
    expect(submit).toBeEnabled();
    await userEvent.click(submit);
    const domain = panel().getByTestId("register-table-domain-select");
    await waitFor(() => expect(domain).toHaveAttribute("aria-invalid", "true"));
    expect(domain).toHaveFocus();
    expect(registerTables).not.toHaveBeenCalled();
  });

  it("says what is missing when Continue is pressed with no table chosen", async () => {
    await createWikidataSource();
    await panel().findByTestId("wikidata-starters-field");
    await userEvent.selectOptions(panel().getByTestId("register-table-domain-select"), "sales");

    await userEvent.click(screen.getByTestId("register-table-submit"));

    expect(await screen.findByTestId("form-problem")).toBeVisible();
    expect(screen.getByTestId("source-setup-finish-later")).toBeInTheDocument();
    expect(registerTables).not.toHaveBeenCalled();
  });
});
