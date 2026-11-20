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
import { Route, Routes, useLocation } from "react-router-dom";
import type { MutationResult, Source } from "../../types/admin";
import type { Registration } from "../tables/registrationDefaults";

const wikidataApi = vi.hoisted(() => ({
  fetchWikidataStarters: vi.fn(),
  searchWikidata: vi.fn(),
  fetchWikidataClass: vi.fn(),
  buildWikidataClassTable: vi.fn(),
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
    { name: "id", data_type: "varchar" },
    { name: "name", data_type: "varchar" },
  ],
};

// The Tables page, as a probe of what it was handed.
function TablesProbe() {
  const location = useLocation();
  const handed = (location.state as { createTable?: Registration } | null)?.createTable ?? null;
  return <div data-testid="tables-page" data-handed={JSON.stringify(handed)} />;
}
const handedTo = (page: HTMLElement): Registration | null =>
  JSON.parse(page.getAttribute("data-handed") as string) as Registration | null;

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
  wikidataApi.fetchWikidataStarters.mockReset().mockResolvedValue([COUNTRIES]);
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

async function pickStarter(label: RegExp): Promise<void> {
  const input = await panel().findByTestId("wikidata-starter");
  await waitFor(() => expect(wikidataApi.fetchWikidataStarters).toHaveBeenCalled());
  await userEvent.click(input);
  await waitFor(() => expect(input.getAttribute("aria-controls")).toBeTruthy());
  const listbox = document.getElementById(input.getAttribute("aria-controls") as string);
  await userEvent.click(
    within(listbox as HTMLElement).getByRole("option", { name: label, hidden: true }),
  );
}

describe("a Wikidata source, created", () => {
  it("goes on to create the starter table chosen when Continue is pressed", async () => {
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
    await screen.findByTestId("register-tables-skip");

    await pickStarter(/Countries/);
    await userEvent.selectOptions(panel().getByTestId("register-table-domain-select"), "sales");
    await userEvent.click(screen.getByTestId("register-table-submit"));

    const handed = handedTo(await screen.findByTestId("tables-page"));
    expect(handed).not.toBeNull();
    expect(handed!.input).toMatchObject({
      sourceId: "test",
      domainId: "sales",
      schemaName: "sparql",
      tableName: "countries",
      queryTemplate: COUNTRIES.query,
    });
  });

  it("says what is missing when Continue is pressed with no table chosen", async () => {
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
    await screen.findByTestId("register-tables-skip");
    await panel().findByTestId("wikidata-starter");

    await userEvent.click(screen.getByTestId("register-table-submit"));

    expect(await screen.findByTestId("form-problem")).toBeVisible();
    expect(screen.getByTestId("register-tables-skip")).toBeInTheDocument();
    expect(screen.queryByTestId("tables-page")).toBeNull();
  });
});
