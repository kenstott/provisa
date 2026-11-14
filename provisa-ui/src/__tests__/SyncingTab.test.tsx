// Copyright (c) 2026 Kenneth Stott
// Canary: 7a2c9e41-3f68-4b05-8d1e-c4b2a9f07e36
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2247: the Syncing page, which replaces the Catalogs panel and the Metadata Export screen
// (REQ-2122, REQ-1074). One list under three headings by type; an entry opens in the detail
// panel, where it is created, edited, tested and, for a data catalog that exports, published now.
//
// What is asserted is what a wrong page gets wrong silently: an entry listed under the wrong
// heading, sending back a credential the user never typed (which would overwrite the stored one
// with an empty string), offering Publish now for an entry whose Export is off or for a type that
// has no publish, losing an entry's export settings, and hiding the reason a run was refused.

import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, waitFor, fireEvent, within } from "../test-utils/render";
import { SyncingTab } from "../components/admin/syncing/SyncingTab";
import type { PublishOutcome, SyncEntry, SyncingState } from "../api/syncing";

vi.mock("../api/syncing", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../api/syncing")>()),
  fetchSyncing: vi.fn(),
  setSyncing: vi.fn(),
  checkEntry: vi.fn(),
  publishEntry: vi.fn(),
}));

vi.mock("../hooks/useAdminQueries", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../hooks/useAdminQueries")>()),
  useDomains: () => ({
    domains: ["sales", "meta", "ontology", "ops", "finance"].map((id) => ({ id })),
    loading: false,
    error: undefined,
    refetch: vi.fn(),
  }),
}));

import { checkEntry, fetchSyncing, publishEntry, setSyncing } from "../api/syncing";

const mockFetch = vi.mocked(fetchSyncing);
const mockSet = vi.mocked(setSyncing);
const mockCheck = vi.mocked(checkEntry);
const mockPublish = vi.mocked(publishEntry);

const MARQUEZ: SyncEntry = {
  id: "marquez",
  type: "data_catalog",
  direction: "export",
  provider: "openlineage",
  endpoint: "http://marquez:5000",
  auth_mode: "api_key",
  username: "",
  entra_tenant_id: "",
  entra_client_id: "",
  gcp_project: "",
  gcp_location: "",
  aws_region: "",
  datazone_domain_id: "",
  timeout_seconds: 30,
  domains: ["sales"],
  role: "steward",
  datazone_project_id: "",
  delta_share: false,
  api_key_set: true,
  token_set: false,
  entra_client_secret_set: false,
  google_service_account_json_set: false,
  aws_access_key_id_set: false,
  aws_secret_access_key_set: false,
  last_run: null,
};

const TERMS: SyncEntry = {
  ...MARQUEZ,
  id: "terms",
  type: "ontology",
  direction: "import",
  provider: "",
  endpoint: "",
  domains: [],
  role: null,
  api_key_set: false,
};

const OWN: SyncEntry = { ...TERMS, id: "own", type: "custom", direction: "both" };

function state(entries: SyncEntry[] = [MARQUEZ], over: Partial<SyncingState> = {}): SyncingState {
  return {
    entitled: true,
    required_tier: "premium",
    providers: ["atlan", "atlas", "datazone", "openlineage", "unity_catalog"],
    beta_providers: ["atlan"],
    publishable_roles: ["analyst", "steward"],
    reconcile_cron: "0 * * * *",
    entries,
    ...over,
  };
}

const SAVED = { success: true, entries: ["marquez"] };

/** Picks ``option`` in the Select whose test id is ``testId``. */
async function pick(testId: string, option: string) {
  fireEvent.click(screen.getByTestId(testId));
  fireEvent.click(await screen.findByRole("option", { name: option, hidden: true }));
}

async function open(id: string) {
  fireEvent.click(await screen.findByTestId(`syncing-row-${id}`));
}

describe("SyncingTab", () => {
  beforeEach(() => {
    mockFetch.mockReset();
    mockSet.mockReset();
    mockCheck.mockReset();
    mockPublish.mockReset();
    mockSet.mockResolvedValue(SAVED);
  });

  it("lists every entry under the heading of its type", async () => {
    mockFetch.mockResolvedValue(state([MARQUEZ, { ...MARQUEZ, id: "marquez-lab" }, TERMS, OWN]));
    render(<SyncingTab />);

    const catalogs = await screen.findByTestId("syncing-section-data_catalog");
    expect(within(catalogs).getByRole("heading", { name: "Data Catalog" })).toBeInTheDocument();
    expect(within(catalogs).getByTestId("syncing-row-marquez")).toBeInTheDocument();
    expect(within(catalogs).getByTestId("syncing-row-marquez-lab")).toBeInTheDocument();
    const ontology = screen.getByTestId("syncing-section-ontology");
    expect(within(ontology).getByRole("heading", { name: "Ontology" })).toBeInTheDocument();
    expect(within(ontology).getByTestId("syncing-row-terms")).toBeInTheDocument();
    const custom = screen.getByTestId("syncing-section-custom");
    expect(within(custom).getByRole("heading", { name: "Custom" })).toBeInTheDocument();
    expect(within(custom).getByTestId("syncing-row-own")).toBeInTheDocument();
    // Each row says its direction.
    expect(screen.getByTestId("syncing-row-terms")).toHaveTextContent("Import");
    expect(screen.getByTestId("syncing-row-own")).toHaveTextContent("Export and Import");
  });

  it("says a heading with no entries is empty", async () => {
    mockFetch.mockResolvedValue(state([MARQUEZ]));
    render(<SyncingTab />);

    expect(await screen.findByTestId("syncing-none-ontology")).toBeInTheDocument();
    expect(screen.getByTestId("syncing-none-custom")).toBeInTheDocument();
  });

  it("names a provider not yet proven against a live catalog as beta", async () => {
    mockFetch.mockResolvedValue(state([{ ...MARQUEZ, id: "atlan-prod", provider: "atlan" }]));
    render(<SyncingTab />);

    expect(await screen.findByTestId("syncing-row-atlan-prod")).toHaveTextContent("atlan (beta)");
  });

  it("shows the organisation-wide publish schedule once and saves it", async () => {
    mockFetch.mockResolvedValue(state([MARQUEZ, TERMS]));
    render(<SyncingTab />);

    const cron = await screen.findByTestId("syncing-reconcile-cron");
    expect(screen.getAllByTestId("syncing-reconcile-cron")).toHaveLength(1);
    fireEvent.change(cron, { target: { value: "*/5 * * * *" } });
    fireEvent.click(screen.getByTestId("syncing-reconcile-save"));

    await waitFor(() => expect(mockSet).toHaveBeenCalledTimes(1));
    const body = mockSet.mock.calls[0][0];
    expect(body.reconcile_cron).toBe("*/5 * * * *");
    // The entries go back as they are, so saving the schedule removes none.
    expect(body.entries.map((e) => e.id)).toEqual(["marquez", "terms"]);
  });

  // --- creating each type -----------------------------------------------------------------------

  it("creates a data catalog with its connection, direction and export settings", async () => {
    mockFetch.mockResolvedValue(state([]));
    render(<SyncingTab />);

    fireEvent.click(await screen.findByTestId("syncing-add"));
    fireEvent.change(screen.getByTestId("syncing-name"), { target: { value: "om" } });
    await pick("syncing-provider", "openlineage");
    fireEvent.change(screen.getByTestId("syncing-endpoint"), {
      target: { value: "http://om:8585" },
    });
    await pick("syncing-direction", "Export and Import");
    await pick("syncing-role", "analyst");
    fireEvent.click(screen.getByTestId("syncing-save"));

    await waitFor(() => expect(mockSet).toHaveBeenCalledTimes(1));
    const [sent] = mockSet.mock.calls[0][0].entries;
    expect([sent.id, sent.type, sent.direction, sent.provider, sent.endpoint, sent.role]).toEqual([
      "om",
      "data_catalog",
      "both",
      "openlineage",
      "http://om:8585",
      "analyst",
    ]);
    expect("last_run" in sent).toBe(false);
  });

  it.each([
    ["Ontology", "ontology"],
    ["Custom", "custom"],
  ])("creates an %s entry with no provider or export settings", async (label, type) => {
    mockFetch.mockResolvedValue(state([MARQUEZ]));
    render(<SyncingTab />);

    fireEvent.click(await screen.findByTestId("syncing-add"));
    fireEvent.change(screen.getByTestId("syncing-name"), { target: { value: "terms" } });
    await pick("syncing-type", label);
    expect(screen.queryByTestId("syncing-provider")).toBeNull();
    expect(screen.queryByTestId("syncing-endpoint")).toBeNull();
    await pick("syncing-direction", "Export");
    // Export of an ontology or custom entry is not a data catalog's publish: no role is asked.
    expect(screen.queryByTestId("syncing-role")).toBeNull();
    fireEvent.click(screen.getByTestId("syncing-save"));

    await waitFor(() => expect(mockSet).toHaveBeenCalledTimes(1));
    const entries = mockSet.mock.calls[0][0].entries;
    expect(entries.map((e) => e.id)).toEqual(["marquez", "terms"]);
    const made = entries[1];
    expect([made.type, made.direction, made.provider, made.role]).toEqual([
      type,
      "export",
      "",
      null,
    ]);
  });

  it.each([
    ["Ontology", "ontology"],
    ["Custom", "custom"],
  ])(
    "%s: a new entry whose direction is not chosen starts as Import, not a data catalog's Export",
    async (label, type) => {
      mockFetch.mockResolvedValue(state([]));
      render(<SyncingTab />);

      fireEvent.click(await screen.findByTestId("syncing-add"));
      fireEvent.change(screen.getByTestId("syncing-name"), { target: { value: "terms" } });
      await pick("syncing-type", label);
      fireEvent.click(screen.getByTestId("syncing-save"));

      await waitFor(() => expect(mockSet).toHaveBeenCalledTimes(1));
      const [made] = mockSet.mock.calls[0][0].entries;
      expect([made.type, made.direction]).toEqual([type, "import"]);
    },
  );

  it("keeps a direction the user chose when the type is changed afterwards", async () => {
    mockFetch.mockResolvedValue(state([]));
    render(<SyncingTab />);

    fireEvent.click(await screen.findByTestId("syncing-add"));
    fireEvent.change(screen.getByTestId("syncing-name"), { target: { value: "terms" } });
    await pick("syncing-direction", "Export and Import");
    await pick("syncing-type", "Ontology");
    fireEvent.click(screen.getByTestId("syncing-save"));

    await waitFor(() => expect(mockSet).toHaveBeenCalledTimes(1));
    const [made] = mockSet.mock.calls[0][0].entries;
    expect([made.type, made.direction]).toEqual(["ontology", "both"]);
  });

  // --- editing each type ------------------------------------------------------------------------

  it("edits a data catalog without sending a credential the user did not type", async () => {
    mockFetch.mockResolvedValue(state([MARQUEZ, { ...MARQUEZ, id: "marquez-lab" }]));
    render(<SyncingTab />);

    await open("marquez");
    fireEvent.click(await screen.findByTestId("syncing-edit"));
    expect(screen.getByTestId("syncing-api-key")).toHaveValue("");
    fireEvent.change(screen.getByTestId("syncing-endpoint"), {
      target: { value: "http://marquez:5001" },
    });
    fireEvent.click(screen.getByTestId("syncing-save"));

    await waitFor(() => expect(mockSet).toHaveBeenCalledTimes(1));
    const [sent, other] = mockSet.mock.calls[0][0].entries;
    expect([sent.id, sent.endpoint, sent.domains, sent.role]).toEqual([
      "marquez",
      "http://marquez:5001",
      ["sales"],
      "steward",
    ]);
    expect("api_key" in sent).toBe(false);
    expect("api_key" in other).toBe(false);
  });

  it("sends a credential the user did type, for that entry only", async () => {
    mockFetch.mockResolvedValue(state([MARQUEZ, { ...MARQUEZ, id: "marquez-lab" }]));
    render(<SyncingTab />);

    await open("marquez-lab");
    fireEvent.click(await screen.findByTestId("syncing-edit"));
    fireEvent.change(screen.getByTestId("syncing-api-key"), { target: { value: "typed" } });
    fireEvent.click(screen.getByTestId("syncing-save"));

    await waitFor(() => expect(mockSet).toHaveBeenCalledTimes(1));
    const byId = Object.fromEntries(mockSet.mock.calls[0][0].entries.map((e) => [e.id, e]));
    expect(byId["marquez-lab"].api_key).toBe("typed");
    expect("api_key" in byId.marquez).toBe(false);
  });

  it.each([
    ["ontology", TERMS],
    ["custom", OWN],
  ])("edits the direction of a %s entry", async (_, entry) => {
    mockFetch.mockResolvedValue(state([MARQUEZ, entry]));
    render(<SyncingTab />);

    await open(entry.id);
    fireEvent.click(await screen.findByTestId("syncing-edit"));
    // A kept entry's name and type are its own.
    expect(screen.getByTestId("syncing-name")).toBeDisabled();
    expect(screen.getByTestId("syncing-type")).toBeDisabled();
    await pick("syncing-direction", "Export and Import");
    fireEvent.click(screen.getByTestId("syncing-save"));

    await waitFor(() => expect(mockSet).toHaveBeenCalledTimes(1));
    const sent = mockSet.mock.calls[0][0].entries.find((e) => e.id === entry.id);
    expect([sent?.type, sent?.direction]).toEqual([entry.type, "both"]);
  });

  it("offers the user's domains for an entry, never a system domain", async () => {
    mockFetch.mockResolvedValue(state([]));
    render(<SyncingTab />);

    fireEvent.click(await screen.findByTestId("syncing-add"));
    fireEvent.click(screen.getByTestId("syncing-domains"));
    expect(await screen.findByRole("option", { name: "sales", hidden: true })).toBeInTheDocument();
    for (const system of ["meta", "ontology", "ops"])
      expect(screen.queryByRole("option", { name: system, hidden: true })).toBeNull();
  });

  // --- what each provider asks for ----------------------------------------------------------------

  it("asks for a username and a password when the catalog authenticates with HTTP basic", async () => {
    // Stock Apache Atlas answers a bearer token with 401, so its deployments run `basic`.
    mockFetch.mockResolvedValue(
      state([{ ...MARQUEZ, id: "atlas", provider: "atlas", auth_mode: "basic" }]),
    );
    render(<SyncingTab />);

    await open("atlas");
    fireEvent.click(await screen.findByTestId("syncing-edit"));
    expect(screen.queryByTestId("syncing-api-key")).toBeNull();
    fireEvent.change(screen.getByTestId("syncing-username"), { target: { value: "admin" } });
    fireEvent.change(screen.getByTestId("syncing-token"), { target: { value: "secret" } });
    fireEvent.click(screen.getByTestId("syncing-save"));

    await waitFor(() => expect(mockSet).toHaveBeenCalledTimes(1));
    const [sent] = mockSet.mock.calls[0][0].entries;
    expect([sent.username, sent.token]).toEqual(["admin", "secret"]);
  });

  it("asks a DataZone catalog that exports for its region, domain, keys and owning project", async () => {
    mockFetch.mockResolvedValue(
      state([{ ...MARQUEZ, id: "datazone", provider: "datazone", endpoint: "" }]),
    );
    render(<SyncingTab />);

    await open("datazone");
    fireEvent.click(await screen.findByTestId("syncing-edit"));
    expect(screen.queryByTestId("syncing-endpoint")).toBeNull();
    expect(screen.queryByTestId("syncing-auth-mode")).toBeNull();
    for (const [field, value] of [
      ["aws-region", "us-east-1"],
      ["datazone-domain-id", "dzd_1"],
      ["aws-access-key-id", "AKIA-TYPED"],
      ["datazone-project-id", "prj_1"],
    ]) {
      fireEvent.change(screen.getByTestId(`syncing-${field}`), { target: { value } });
    }
    fireEvent.click(screen.getByTestId("syncing-save"));

    await waitFor(() => expect(mockSet).toHaveBeenCalledTimes(1));
    const [sent] = mockSet.mock.calls[0][0].entries;
    expect([sent.aws_region, sent.datazone_domain_id, sent.datazone_project_id]).toEqual([
      "us-east-1",
      "dzd_1",
      "prj_1",
    ]);
    expect(sent.aws_access_key_id).toBe("AKIA-TYPED");
  });

  it("asks a DataZone catalog that only imports for no owning project", async () => {
    mockFetch.mockResolvedValue(
      state([
        { ...MARQUEZ, id: "datazone", provider: "datazone", endpoint: "", direction: "import" },
      ]),
    );
    render(<SyncingTab />);

    await open("datazone");
    fireEvent.click(await screen.findByTestId("syncing-edit"));
    expect(screen.queryByTestId("syncing-datazone-project-id")).toBeNull();
    expect(screen.queryByTestId("syncing-role")).toBeNull();
  });

  it("offers a Unity Catalog catalog its Delta Share and no endpoint", async () => {
    mockFetch.mockResolvedValue(
      state([{ ...MARQUEZ, id: "uc", provider: "unity_catalog", endpoint: "" }]),
    );
    render(<SyncingTab />);

    await open("uc");
    fireEvent.click(await screen.findByTestId("syncing-edit"));
    expect(screen.queryByTestId("syncing-endpoint")).toBeNull();
    fireEvent.click(screen.getByTestId("syncing-delta-share"));
    fireEvent.click(screen.getByTestId("syncing-save"));

    await waitFor(() => expect(mockSet).toHaveBeenCalledTimes(1));
    expect(mockSet.mock.calls[0][0].entries[0].delta_share).toBe(true);
  });

  // --- read, test, Publish now and the last result ---------------------------------------------------

  it("reads an entry with its credential as set, never its value", async () => {
    mockFetch.mockResolvedValue(state());
    render(<SyncingTab />);

    await open("marquez");
    const read = await screen.findByTestId("syncing-entry-read-marquez");
    expect(read).toHaveTextContent("http://marquez:5000");
    expect(read).toHaveTextContent("steward");
    expect(read).toHaveTextContent("Set");
  });

  it("reports the reason a catalog refused the connection test", async () => {
    mockFetch.mockResolvedValue(state());
    mockCheck.mockResolvedValue({
      id: "marquez",
      provider: "openlineage",
      ok: false,
      error: "ConnectError: Name or service not known",
    });
    render(<SyncingTab />);

    await open("marquez");
    fireEvent.click(await screen.findByTestId("syncing-test"));

    await waitFor(() =>
      expect(screen.getByTestId("syncing-health")).toHaveTextContent("Name or service not known"),
    );
    expect(mockCheck).toHaveBeenCalledWith("marquez");
  });

  it("publishes a data catalog now and shows the result of its last publish", async () => {
    const outcome: PublishOutcome = {
      id: "marquez",
      provider: "openlineage",
      ok: false,
      published: { dataset: 2 },
      total_published: 2,
      errors: [{ asset: "wh.public.orders", message: "422 unknown field type" }],
    };
    mockFetch
      .mockResolvedValueOnce(state())
      .mockResolvedValueOnce(state([{ ...MARQUEZ, last_run: outcome }]));
    mockPublish.mockResolvedValue({ ok: false, entries: [outcome] });
    render(<SyncingTab />);

    await open("marquez");
    expect(await screen.findByTestId("syncing-never-run")).toBeInTheDocument();
    fireEvent.click(screen.getByTestId("syncing-publish"));

    await waitFor(() => expect(mockPublish).toHaveBeenCalledWith("marquez"));
    const last = await screen.findByTestId("syncing-last-run");
    expect(within(last).getByTestId("syncing-last-run-badge")).toHaveTextContent(
      "2 published, 1 rejected",
    );
    expect(within(last).getByTestId("syncing-last-run-errors")).toHaveTextContent(
      "422 unknown field type",
    );
    // The list shows it too.
    expect(screen.getByTestId("syncing-row-last-run-marquez")).toHaveTextContent("1 rejected");
  });

  it("shows the result of a publish made before this page load", async () => {
    mockFetch.mockResolvedValue(
      state([
        {
          ...MARQUEZ,
          last_run: {
            id: "marquez",
            provider: "openlineage",
            ok: true,
            published: { dataset: 4 },
            total_published: 4,
            errors: [],
          },
        },
      ]),
    );
    render(<SyncingTab />);

    await open("marquez");
    expect(await screen.findByTestId("syncing-last-run-badge")).toHaveTextContent(
      "4 assets published",
    );
  });

  it("offers no Publish now for a data catalog whose Export is off", async () => {
    mockFetch.mockResolvedValue(state([{ ...MARQUEZ, direction: "import", role: null }]));
    render(<SyncingTab />);

    await open("marquez");
    expect(await screen.findByTestId("syncing-test")).toBeInTheDocument();
    expect(screen.queryByTestId("syncing-publish")).toBeNull();
  });

  it.each([
    ["ontology", TERMS],
    ["custom", OWN],
  ])("offers a %s entry no run and no connection test", async (_, entry) => {
    mockFetch.mockResolvedValue(state([{ ...entry, direction: "both" }]));
    render(<SyncingTab />);

    await open(entry.id);
    expect(await screen.findByTestId(`syncing-entry-read-${entry.id}`)).toBeInTheDocument();
    expect(screen.queryByTestId("syncing-publish")).toBeNull();
    expect(screen.queryByTestId("syncing-test")).toBeNull();
  });

  it("below the plan's tier says which plan opens publishing and offers no Publish now", async () => {
    mockFetch.mockResolvedValue(state([MARQUEZ], { entitled: false }));
    render(<SyncingTab />);

    expect(await screen.findByTestId("syncing-not-entitled")).toHaveTextContent("premium");
    await open("marquez");
    expect(await screen.findByTestId("syncing-test")).toBeInTheDocument();
    expect(screen.queryByTestId("syncing-publish")).toBeNull();
  });

  it("says why a save was refused", async () => {
    mockFetch.mockResolvedValue(state());
    mockSet.mockRejectedValue(new Error("catalog 'marquez' has no endpoint"));
    render(<SyncingTab />);

    await open("marquez");
    fireEvent.click(await screen.findByTestId("syncing-edit"));
    fireEvent.change(screen.getByTestId("syncing-endpoint"), { target: { value: "" } });
    fireEvent.click(screen.getByTestId("syncing-save"));

    expect(await screen.findByText("catalog 'marquez' has no endpoint")).toBeInTheDocument();
  });
});
