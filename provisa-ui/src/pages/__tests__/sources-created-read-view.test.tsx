// Copyright (c) 2026 Kenneth Stott
// Canary: 3d7b1e58-92c4-4a6f-b0e3-5f8a6c1d2e47
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// A source that was created is saved: its detail panel shows it read, in its sections, once its
// tables are registered and Your data is closed, and equally when registering them is skipped.
// Nothing is offered to save, and its edit form opens only when the person presses Edit.

import { beforeEach, describe, it, expect, vi } from "vitest";
import { render, screen, waitFor, within } from "../../test-utils/render";
import userEvent from "@testing-library/user-event";
import type { MutationResult, Source } from "../../types/admin";

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
  // The source setup's reads: no table registered before, none related.
  useTables: () => ({ tables: [], loading: false, refetch: async () => ({}) }),
  useRelationships: () => ({ relationships: [], loading: false, refetch: async () => ({}) }),
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
// REQ-2262: what the setup flow records for the source, as the server takes it.
const setupWrites: { sourceId: string; outcomes: Record<string, string | null> }[] = [];
vi.mock("../../hooks/useSourceSetupOutcomes", () => ({
  useSetSourceSetupOutcomes:
    () => async (sourceId: string, outcomes: Record<string, string | null>) => {
      setupWrites.push({ sourceId, outcomes });
    },
}));

const registerTables = vi.fn();
vi.mock("../../hooks/useTableQueries", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../hooks/useTableQueries")>()),
  useRegisterTables: () => ({ registerTables, loading: false }),
}));

import { SourcesPage } from "../SourcesPage";
import { forgetSourceKinds } from "../sources/sourceKind";

const panel = () => within(screen.getByTestId("sources-split-panel"));

beforeEach(() => {
  setupWrites.length = 0;
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
  vi.spyOn(globalThis, "fetch").mockImplementation(async (url) => {
    if (String(url).includes("/admin/source-kinds/")) {
      return new Response(
        JSON.stringify({ cache_ttl_required: false, sentinel_path: true, tables_land: false }),
      );
    }
    return new Response(JSON.stringify({ configured: true, organisation_mailboxes: true }));
  });
});

/** "+ Source", a type from the picker, what that type needs, Save: the source as the server
 *  then lists it. The panel is left on its Register tables step. */
async function create(type: string, made: Source, fill: () => Promise<void>): Promise<void> {
  render(<SourcesPage />);
  await userEvent.click(await screen.findByTestId("sources-add-toggle"));
  await userEvent.click((await screen.findAllByTestId(`source-type-option-${type}`))[0]);
  await userEvent.type(await screen.findByTestId("sources-id-input"), made.id);
  await fill();
  refetch.mockImplementation(async () => {
    SOURCES = [...SOURCES.filter((s) => s.id !== made.id), made];
    return { data: { sources: SOURCES } };
  });
  await userEvent.click(screen.getByTestId("sources-submit"));
  await waitFor(() => expect(createSource).toHaveBeenCalled());
  expect(createSource.mock.calls[0][0]).toMatchObject({ id: made.id, type });
  // REQ-2234 / REQ-2262: the source's setup, its strip at Register tables, the form its content.
  await screen.findByTestId("source-setup-finish-later");
  expect(panel().getByTestId("source-setup-stepper")).toHaveAttribute("data-active", "1");
  expect(panel().getByTestId("register-tables-field")).toBeInTheDocument();
}

/** The tables that need no input are picked already: one action registers them. */
async function registerPicked(): Promise<void> {
  const submit = await panel().findByTestId("register-many-tables-submit");
  await waitFor(() => expect(submit).toHaveTextContent("Register 2 tables"));
  await userEvent.click(submit);
  await waitFor(() => expect(registerTables).toHaveBeenCalledTimes(1));
  expect(await panel().findByTestId("your-data-step")).toBeInTheDocument();
  // Your data is step 2 of the same strip.
  expect(panel().getByTestId("source-setup-stepper")).toHaveAttribute("data-active", "2");
}

function expectReadView(id: string): void {
  expect(screen.getByTestId("sources-split-title")).toHaveTextContent(id);
  // Read: the panel's own actions in its head, its sections, and no form to save.
  expect(panel().getByTestId("source-detail-edit")).toBeInTheDocument();
  expect(panel().getByTestId("source-section-connection")).toBeInTheDocument();
  expect(screen.queryByTestId("sources-edit-submit")).toBeNull();
  expect(screen.queryByTestId("sources-edit-cancel")).toBeNull();
  const read = panel().getByTestId("source-read");
  expect(read.querySelectorAll("input, select, textarea")).toHaveLength(0);
}

const KINDS: { name: string; type: string; made: Source; fill: () => Promise<void> }[] = [
  {
    name: "a file source",
    type: "files",
    made: source({
      id: "team-files",
      type: "files",
      host: "",
      port: 0,
      database: "",
      username: "",
      passwordRef: null,
      path: "/data/team",
    } as Partial<Source>),
    fill: async () => {
      await userEvent.type(panel().getByLabelText(/^Path/), "/data/team");
    },
  },
  {
    name: "a database source",
    type: "postgresql",
    made: source({ id: "crm", host: "db2", database: "crm" }),
    fill: async () => {
      await userEvent.type(panel().getByLabelText(/^Host/), "db2");
      await userEvent.type(panel().getByLabelText(/^Database/), "crm");
    },
  },
];

describe.each(KINDS)("$name, created and its tables registered", ({ type, made, fill }) => {
  it("leaves Your data by Finish later for the source read, and opens its edit form on Edit alone", async () => {
    await create(type, made, fill);
    await registerPicked();
    await waitFor(() =>
      expect(setupWrites).toContainEqual({
        sourceId: made.id,
        outcomes: { tables: "registered", relationships: null, query: null },
      }),
    );
    await userEvent.click(panel().getByTestId("source-setup-finish-later"));
    await waitFor(() => expect(screen.queryByTestId("your-data-step")).toBeNull());
    await panel().findByTestId("source-read");
    expectReadView(made.id);
    // The person's own action opens the edit form, on the same sections.
    await userEvent.click(panel().getByTestId("source-detail-edit"));
    expect(await panel().findByTestId("sources-edit-submit")).toBeInTheDocument();
    expect(panel().getByTestId("source-section-connection")).toBeInTheDocument();
  });

  it("shows the source read when the panel is closed on Your data and the source opened again", async () => {
    await create(type, made, fill);
    await registerPicked();
    await userEvent.click(screen.getByTestId("sources-split-close"));
    await userEvent.click(await screen.findByTestId(`sources-row-${made.id}`));
    await panel().findByTestId("source-read");
    expectReadView(made.id);
  });
});

describe.each(KINDS)(
  "$name, created and its setup left at Register tables",
  ({ type, made, fill }) => {
    it("offers no Skip; Finish later records nothing, shows the source read, and Edit alone opens its form", async () => {
      await create(type, made, fill);
      // REQ-2234: the later steps read the tables it registers, so it cannot be skipped.
      expect(screen.queryByTestId("source-setup-skip")).toBeNull();
      await userEvent.click(panel().getByTestId("source-setup-finish-later"));
      await waitFor(() => expect(screen.queryByTestId("source-setup-finish-later")).toBeNull());
      await panel().findByTestId("source-read");
      expect(setupWrites).toEqual([]);
      expectReadView(made.id);
      await userEvent.click(panel().getByTestId("source-detail-edit"));
      expect(await panel().findByTestId("sources-edit-submit")).toBeInTheDocument();
      expect(panel().getByTestId("source-section-connection")).toBeInTheDocument();
    });

    it("shows the source read when the panel is closed on Register tables and the source opened again", async () => {
      await create(type, made, fill);
      await userEvent.click(screen.getByTestId("sources-split-close"));
      // The step has the source chosen: leaving it by the panel's close asks first, as any form does.
      await userEvent.click(await screen.findByTestId("discard-changes-confirm"));
      await waitFor(() => expect(screen.queryByTestId("sources-split-panel")).toBeNull());
      await userEvent.click(await screen.findByTestId(`sources-row-${made.id}`));
      await panel().findByTestId("source-read");
      expectReadView(made.id);
    });
  },
);
