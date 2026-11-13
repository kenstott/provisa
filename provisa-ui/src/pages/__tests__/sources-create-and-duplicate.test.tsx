// Copyright (c) 2026 Kenneth Stott
// Canary: bcc69730-880a-4091-93b9-aa1ea2459546
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// "+ Source" opens an empty form: the picked type's defaults, nothing of the source selected in
// the list or last edited. Duplicating a source is asked for, from its panel's head: the create
// form opens prefilled from it under "<id>-copy", its credentials carried as the references it
// uses (never a secret's value), its tables not copied; once saved, the new source's panel opens
// on registering its tables, as for any new source.

import { beforeEach, describe, expect, it, vi } from "vitest";
import { render, screen, waitFor, within } from "../../test-utils/render";
import userEvent from "@testing-library/user-event";
import type { MutationResult, Source } from "../../types/admin";
import { expectHoldsNoneOf } from "../../test-utils/createForm";

vi.mock("../../api/admin", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../api/admin")>()),
  fetchSettings: vi.fn().mockResolvedValue({
    redirect: { enabled: false, threshold: 10000, default_format: "json", ttl: 3600 },
    sampling: { default_sample_size: 1000 },
    cache: { default_ttl: 300 },
    naming: { domain_prefix: false, convention: "none" },
  }),
  fetchSourceReach: vi.fn().mockResolvedValue(null),
}));

vi.mock("../../context/AuthContext", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../context/AuthContext")>()),
  useAuth: () => ({
    role: "admin",
    selectedRoles: ["admin"],
    capabilities: ["admin"],
    domainAccess: ["*"],
  }),
}));

const ok: MutationResult = { success: true, message: "" };
const source = (over: Partial<Source>): Source =>
  ({
    id: "sales-pg",
    type: "postgresql",
    host: "sales-db.internal",
    port: 6543,
    database: "sales",
    username: "svc_sales",
    path: "",
    cacheEnabled: true,
    cacheTtl: 120,
    changeSignal: "ttl",
    mappingJson: null,
    passwordRef: "${secret:sales_pw}",
    description: "The sales warehouse",
    allowedDomains: [],
    ...over,
  }) as unknown as Source;

let SOURCES: Source[] = [];
const createSource = vi.fn();
const refetch = vi.fn();

vi.mock("../../hooks/useAdminQueries", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../hooks/useAdminQueries")>()),
  useSources: () => ({ sources: SOURCES, loading: false, refetch }),
  useDomains: () => ({ domains: [], loading: false, refetch: vi.fn() }),
  useTables: () => ({
    tables: [{ id: 1, sourceId: "sales-pg", schemaName: "public", tableName: "orders" }],
    loading: false,
    refetch: vi.fn(),
  }),
  useCreateSource: () => ({ createSource }),
  useUpdateSourceCache: () => ({ updateSourceCache: vi.fn().mockResolvedValue(ok) }),
  useUpdateSourceLoadProtection: () => ({
    updateSourceLoadProtection: vi.fn().mockResolvedValue(ok),
  }),
  useUpdateSourceAllowedDomains: () => ({
    updateSourceAllowedDomains: vi.fn().mockResolvedValue(ok),
  }),
  // REQ-2234: what the new source's Register tables step lists.
  useAvailableSchemas: () => ({ schemas: ["public"], loading: false }),
  useAvailableTables: () => ({ tables: [], loading: false }),
}));

import { SourcesPage } from "../SourcesPage";
import { forgetSourceKinds } from "../sources/sourceKind";

const panel = () => screen.getByTestId("sources-split-panel");
/** The selected source's own values: none of them is a default a new source has. */
const SALES = ["sales-pg", "sales-db.internal", "6543", "sales", "svc_sales", "${secret:sales_pw}"];

beforeEach(() => {
  SOURCES = [
    source({}),
    source({ id: "events", type: "kafka", host: "broker", port: 9092, database: "" }),
  ];
  createSource.mockReset().mockResolvedValue(ok);
  refetch.mockReset().mockResolvedValue({ data: { sources: SOURCES } });
  forgetSourceKinds();
  vi.spyOn(globalThis, "fetch").mockImplementation(async (url) => {
    const asked = String(url);
    if (asked.includes("/admin/source-kinds/")) {
      return new Response(
        JSON.stringify({ cache_ttl_required: false, sentinel_path: true, tables_land: false }),
      );
    }
    return new Response(JSON.stringify({}));
  });
});

/** "+ Source", then a type from the picker it opens. */
async function addSource(type: string): Promise<void> {
  await userEvent.click(await screen.findByTestId("sources-add-toggle"));
  await userEvent.click((await screen.findAllByTestId(`source-type-option-${type}`))[0]);
  await screen.findByTestId("sources-submit");
}

const field = (label: RegExp) => within(panel()).getByLabelText(label) as HTMLInputElement;

describe("+ Source opens the create form empty", () => {
  for (const type of ["postgresql", "mysql"]) {
    it(`after a source was selected: the ${type} form holds its defaults and nothing of the source`, async () => {
      render(<SourcesPage />);
      await userEvent.click(await screen.findByTestId("sources-row-sales-pg"));
      await screen.findByTestId("source-detail-edit");
      await addSource(type);
      expect(screen.getByTestId("sources-type-select")).toHaveValue(type);
      expect(screen.getByTestId("sources-id-input")).toHaveValue("");
      expect(field(/^Host/)).toHaveValue("");
      expect(field(/^Database/)).toHaveValue("");
      expect(field(/^Username/)).toHaveValue("");
      expect(field(/^Password/)).toHaveValue("");
      expect(field(/^Port/)).toHaveValue(type === "mysql" ? "3306" : "5432");
      expectHoldsNoneOf(panel(), SALES);
    });
  }

  it("after a source was edited and the edit cancelled: the same", async () => {
    render(<SourcesPage />);
    await userEvent.click(await screen.findByTestId("sources-row-sales-pg"));
    await userEvent.click(await screen.findByTestId("source-detail-edit"));
    await userEvent.click(await screen.findByTestId("sources-edit-cancel"));
    await addSource("postgresql");
    expect(screen.getByTestId("sources-id-input")).toHaveValue("");
    expect(field(/^Host/)).toHaveValue("");
    expect(field(/^Password/)).toHaveValue("");
    expectHoldsNoneOf(panel(), SALES);
  });

  it("does not show the duplicate's note on an empty form", async () => {
    render(<SourcesPage />);
    await addSource("postgresql");
    expect(screen.queryByTestId("sources-duplicate-note")).toBeNull();
  });
});

describe("Duplicate, in a source's panel head", () => {
  it("is beside Edit and Delete, named Duplicate, its tip naming the source", async () => {
    render(<SourcesPage />);
    await userEvent.click(await screen.findByTestId("sources-row-sales-pg"));
    const action = await within(panel()).findByTestId("source-detail-duplicate");
    expect(action).toHaveAccessibleName("Duplicate");
    expect(action).toHaveAttribute("data-variant", "subtle");
    await userEvent.hover(action);
    expect(await screen.findByRole("tooltip")).toHaveTextContent("Duplicate sales-pg");
  });

  it("opens the create form prefilled from the source, as <id>-copy, its reference carried", async () => {
    render(<SourcesPage />);
    await userEvent.click(await screen.findByTestId("sources-row-sales-pg"));
    await userEvent.click(await screen.findByTestId("source-detail-duplicate"));
    // The create form (its own Save), not the source's edit form.
    await screen.findByTestId("sources-submit");
    expect(screen.queryByTestId("sources-edit-submit")).toBeNull();
    expect(screen.getByTestId("sources-id-input")).toHaveValue("sales-pg-copy");
    expect(screen.getByTestId("sources-type-select")).toHaveValue("postgresql");
    expect(field(/^Host/)).toHaveValue("sales-db.internal");
    expect(field(/^Port/)).toHaveValue("6543");
    expect(field(/^Database/)).toHaveValue("sales");
    expect(field(/^Username/)).toHaveValue("svc_sales");
    // The same reference the source uses: what it names is resolved when the source is used.
    expect(field(/^Password/)).toHaveValue("${secret:sales_pw}");
    expect(screen.queryByTestId("sources-duplicate-note")).toBeNull();
  });

  it("leaves a credential that is not a reference empty, and says it must be entered", async () => {
    SOURCES = [source({ passwordRef: "hunter2" })];
    render(<SourcesPage />);
    await userEvent.click(await screen.findByTestId("sources-row-sales-pg"));
    await userEvent.click(await screen.findByTestId("source-detail-duplicate"));
    await screen.findByTestId("sources-submit");
    expect(field(/^Password/)).toHaveValue("");
    expect(within(panel()).getByTestId("sources-duplicate-note")).toHaveTextContent(/not copied/i);
    expectHoldsNoneOf(panel(), ["hunter2"]);
  });

  it("saves a new source with the copied settings and no tables, then registers its tables", async () => {
    render(<SourcesPage />);
    await userEvent.click(await screen.findByTestId("sources-row-sales-pg"));
    await userEvent.click(await screen.findByTestId("source-detail-duplicate"));
    await screen.findByTestId("sources-submit");
    const made = source({ id: "sales-pg-copy" });
    refetch.mockImplementation(async () => {
      SOURCES = [...SOURCES, made];
      return { data: { sources: SOURCES } };
    });
    await userEvent.click(screen.getByTestId("sources-submit"));
    await waitFor(() => expect(createSource).toHaveBeenCalledTimes(1));
    const sent = createSource.mock.calls[0][0] as Record<string, unknown>;
    expect(sent).toMatchObject({
      id: "sales-pg-copy",
      type: "postgresql",
      host: "sales-db.internal",
      port: 6543,
      database: "sales",
      username: "svc_sales",
      password: "${secret:sales_pw}",
      cacheTtl: 120,
      description: "The sales warehouse",
      registerTableDraft: false,
    });
    // No table goes with it: the original's tables stay the original's.
    expect(Object.keys(sent).some((k) => /tables?$/i.test(k))).toBe(false);
    // REQ-2234: as for any new source, its panel opens on registering its tables.
    expect(await within(panel()).findByTestId("register-tables-skip")).toBeInTheDocument();
    expect(screen.getByTestId("sources-split-title")).toHaveTextContent("sales-pg-copy");
  });

  it("is refused on save under an id that exists, by the create's own validation", async () => {
    createSource.mockResolvedValue({ success: false, message: "Source 'events' already exists" });
    render(<SourcesPage />);
    await userEvent.click(await screen.findByTestId("sources-row-sales-pg"));
    await userEvent.click(await screen.findByTestId("source-detail-duplicate"));
    const id = await screen.findByTestId("sources-id-input");
    await userEvent.clear(id);
    await userEvent.type(id, "events");
    await userEvent.click(screen.getByTestId("sources-submit"));
    await waitFor(() => expect(createSource).toHaveBeenCalledTimes(1));
    expect(createSource.mock.calls[0][0]).toMatchObject({ id: "events" });
    expect(await screen.findByTestId("sources-error")).toHaveTextContent(/already exists/);
    // The form stays open with what was entered.
    expect(screen.getByTestId("sources-submit")).toBeInTheDocument();
    expect(screen.getByTestId("sources-id-input")).toHaveValue("events");
  });

  it("then + Source opens empty again", async () => {
    render(<SourcesPage />);
    await userEvent.click(await screen.findByTestId("sources-row-sales-pg"));
    await userEvent.click(await screen.findByTestId("source-detail-duplicate"));
    await userEvent.click(await screen.findByTestId("sources-cancel"));
    await addSource("postgresql");
    expect(screen.getByTestId("sources-id-input")).toHaveValue("");
    expect(screen.queryByTestId("sources-duplicate-note")).toBeNull();
    expectHoldsNoneOf(panel(), SALES);
  });
});
