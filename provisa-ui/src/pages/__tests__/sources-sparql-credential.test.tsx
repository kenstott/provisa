// Copyright (c) 2026 Kenneth Stott
// Canary: 7d2e4b91-3c5a-4f08-9e1d-6a8b2c4f0e13
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2138: a sparql source declares the credential its endpoint is read as -- bearer or basic,
// entered as a value or a ${secret:...} reference -- and the Save sends it in the source's
// username and password with its kind in the mapping; an edited source opens with it as held.

import { beforeEach, describe, expect, it, vi } from "vitest";
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
    id: "kyc-graph",
    type: "sparql",
    host: "https://provisa.example.test/data/sparql",
    port: 0,
    database: "",
    username: "",
    path: "",
    cacheEnabled: true,
    cacheTtl: 120,
    changeSignal: "ttl",
    mappingJson: null,
    passwordRef: "",
    description: "",
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
  useDomains: () => ({ domains: [], loading: false, refetch: vi.fn() }),
  useTables: () => ({
    tables: [],
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

beforeEach(() => {
  SOURCES = [];
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

const ENDPOINT = "https://provisa.example.test/data/sparql";

/** "+ Source", sparql, its id and endpoint, and the credential kind named. */
async function newSparqlSource(kind: "No Auth" | "Bearer Token" | "Basic Auth"): Promise<void> {
  await userEvent.click(await screen.findByTestId("sources-add-toggle"));
  await userEvent.click((await screen.findAllByTestId("source-type-option-sparql"))[0]);
  await userEvent.type(await screen.findByTestId("sources-id-input"), "kyc-graph");
  await userEvent.type(screen.getByTestId("sparql-endpoint-input"), ENDPOINT);
  await userEvent.click(screen.getByTestId("sparql-auth-type-select"));
  await userEvent.click(await screen.findByRole("option", { name: kind, hidden: true }));
}

async function saved(): Promise<Record<string, unknown>> {
  await userEvent.click(screen.getByTestId("sources-submit"));
  await waitFor(() => expect(createSource).toHaveBeenCalledTimes(1));
  return createSource.mock.calls[0][0] as Record<string, unknown>;
}

describe("a sparql source's credential", () => {
  it("sends a bearer token, as its reference, in the password with the kind in the mapping", async () => {
    render(<SourcesPage />);
    await newSparqlSource("Bearer Token");
    // userEvent.type reads "{" as a key descriptor; "{{" types one.
    await userEvent.type(screen.getByTestId("sparql-token-input"), "${{secret:kyc_token}");
    const sent = await saved();
    expect(sent).toMatchObject({
      id: "kyc-graph",
      type: "sparql",
      host: ENDPOINT,
      username: "",
      password: "${secret:kyc_token}",
    });
    expect(JSON.parse(sent.mappingJson as string)).toEqual({ auth: "bearer" });
  });

  it("sends a basic username and password, with the kind in the mapping", async () => {
    render(<SourcesPage />);
    await newSparqlSource("Basic Auth");
    await userEvent.type(within(panel()).getByLabelText(/^Username/), "svc-reasoner");
    await userEvent.type(within(panel()).getByLabelText(/^Password/), "${{secret:kyc_pw}");
    const sent = await saved();
    expect(sent).toMatchObject({ username: "svc-reasoner", password: "${secret:kyc_pw}" });
    expect(JSON.parse(sent.mappingJson as string)).toEqual({ auth: "basic" });
  });

  it("sends no credential and no kind when none is declared", async () => {
    render(<SourcesPage />);
    await newSparqlSource("No Auth");
    const sent = await saved();
    expect(sent).toMatchObject({ username: "", password: "" });
    expect(JSON.parse(sent.mappingJson as string)).toEqual({});
  });

  it("an edited source opens with the credential it holds", async () => {
    SOURCES = [
      source({
        mappingJson: JSON.stringify({ auth: "bearer" }),
        passwordRef: "${secret:kyc_token}",
      }),
    ];
    render(<SourcesPage />);
    await userEvent.click(await screen.findByTestId("sources-row-kyc-graph"));
    await userEvent.click(await screen.findByTestId("source-detail-edit"));
    await userEvent.click(await screen.findByTestId("source-section-authentication"));
    expect(screen.getByTestId("sparql-auth-type-select")).toHaveValue("Bearer Token");
    expect(await screen.findByTestId("sparql-token-input")).toHaveValue("${secret:kyc_token}");
  });
});
