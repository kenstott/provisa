// Copyright (c) 2026 Kenneth Stott
// Canary: b43bd0ac-06e1-4aa2-bf20-1f320df5ee73
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// #208: the server judges a source's change signal against the Cache TTL in the SAME input that
// saves the source. The form used to send the Cache TTL only in a second call, so a mail source
// (whose every table lands, and must therefore state its Cache TTL) could not be saved from the
// form although the field was filled in. These submit the form through the page, against a
// stand-in that applies the server's rule to the input it is given.
//
// These drive the EDIT path. Create and edit build one payload, so the Cache TTL is covered for
// both, but nothing here clicks through creating a source.
//
// And what the form shows of a refusal is the reader's wording of its code, never the server's
// own text, which may name a requirement.

import { beforeEach, describe, it, expect, vi } from "vitest";
import { render, screen, waitFor } from "../../test-utils/render";
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

const MAIL_KINDS = ["microsoft_365", "google_workspace"];
const ok: MutationResult = { success: true, message: "" };

/** The server's rule (_landing_ttl.landing_ttl_refusal), on the input the mutation is given. */
function serverSave(input: {
  id: string;
  type: string;
  changeSignal?: string;
  cacheTtl?: number | null;
}) {
  const clocked = ["ttl", "ttl_probe"].includes(input.changeSignal ?? "ttl");
  if (MAIL_KINDS.includes(input.type) && clocked && input.cacheTtl == null) {
    return {
      success: false,
      // The server's own text names a requirement; the form must not show it.
      message: `Source '${input.id}': change_signal 'ttl' is TTL-based (REQ-1907)`,
      code: "schema.source_cache_ttl_required",
      params: { source: input.id, signal: input.changeSignal ?? "ttl" },
    } satisfies MutationResult;
  }
  return ok;
}

let SOURCES: Source[] = [];
// Whether the form has been told which kinds must state a Cache TTL. When it has, it stops an
// empty one itself; a form that has not (the ask failed) is stopped by the server instead.
let formKnows = true;
const updateSource = vi.fn(async (input: never) => serverSave(input));
const updateSourceCache = vi.fn().mockResolvedValue(ok);

vi.mock("../../hooks/useAdminQueries", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../hooks/useAdminQueries")>()),
  useSources: () => ({ sources: SOURCES, loading: false, refetch: vi.fn().mockResolvedValue({}) }),
  useDomains: () => ({ domains: [], loading: false, refetch: vi.fn() }),
  useUpdateSource: () => ({ updateSource }),
  useRenameSource: () => ({ renameSource: vi.fn().mockResolvedValue(ok) }),
  useUpdateSourceCache: () => ({ updateSourceCache }),
  useUpdateSourceReplicate: () => ({ updateSourceReplicate: vi.fn().mockResolvedValue(ok) }),
  useUpdateSourceLoadProtection: () => ({
    updateSourceLoadProtection: vi.fn().mockResolvedValue(ok),
  }),
  useUpdateSourceNaming: () => ({ updateSourceNaming: vi.fn().mockResolvedValue(ok) }),
  useUpdateSourceAllowedDomains: () => ({
    updateSourceAllowedDomains: vi.fn().mockResolvedValue(ok),
  }),
}));

import { SourcesPage } from "../SourcesPage";
import { forgetSourceKinds } from "../sources/sourceKind";

function source(overrides: Partial<Source>): Source {
  return {
    id: "mail",
    type: "microsoft_365",
    host: "",
    port: 0,
    database: "",
    username: "",
    path: "",
    cacheEnabled: true,
    cacheTtl: 300,
    changeSignal: "ttl",
    mappingJson: JSON.stringify({ resources: ["mail"], mailboxes: { everyone: true } }),
    ...overrides,
  } as unknown as Source;
}

async function openForEdit(id: string): Promise<void> {
  render(<SourcesPage />);
  await userEvent.click(await screen.findByText(id));
  await userEvent.click(await screen.findByTestId("source-detail-edit"));
}

async function save(): Promise<{ type: string; cacheTtl: number | null }> {
  await userEvent.click(screen.getByRole("button", { name: /save/i }));
  await waitFor(() => expect(updateSource).toHaveBeenCalled());
  return updateSource.mock.calls.at(-1)![0] as never;
}

describe("saving a source sends its Cache TTL with the source", () => {
  beforeEach(() => {
    updateSource.mockClear();
    updateSourceCache.mockClear();
    formKnows = true;
    forgetSourceKinds();
    vi.spyOn(globalThis, "fetch").mockImplementation(async (url) => {
      const asked = String(url);
      if (asked.includes("/admin/source-kinds/")) {
        const kind = decodeURIComponent(asked.split("/admin/source-kinds/")[1]);
        const mail = formKnows && MAIL_KINDS.includes(kind);
        return new Response(
          JSON.stringify({ cache_ttl_required: mail, sentinel_path: !mail, tables_land: mail }),
        );
      }
      return new Response(JSON.stringify({ configured: true, organisation_mailboxes: true }));
    });
  });

  it("saves a mail source whose Cache TTL is set", async () => {
    SOURCES = [source({})];
    await openForEdit("mail");
    const sent = await save();
    expect(sent.type).toBe("microsoft_365");
    expect(sent.cacheTtl).toBe(300); // judged by the server in this input, so it rides here
    await waitFor(() => expect(updateSourceCache).toHaveBeenCalledWith("mail", true, 300));
    expect(screen.queryByText(/needs a Cache TTL/)).toBeNull(); // the save went through
  });

  it("stops a mail source with no Cache TTL in the form, before anything is sent", async () => {
    SOURCES = [source({ id: "mail-b", cacheTtl: null })];
    await openForEdit("mail-b");
    // The Cache TTL is a Load setting: its section's tab.
    await userEvent.click(screen.getByTestId("source-section-load"));
    await screen.findByTestId("cache-ttl-input");
    await waitFor(() => expect(screen.getByTestId("cache-ttl-input")).toBeRequired());
    await userEvent.click(screen.getByRole("button", { name: /save/i }));
    expect(
      await screen.findByText(/judges staleness by the Cache TTL, so set a Cache TTL/),
    ).toBeInTheDocument();
    expect(updateSource).not.toHaveBeenCalled();
  });

  it("shows the reader's wording of a refusal, never the server's own text", async () => {
    formKnows = false; // the form lets it through; the server refuses
    SOURCES = [source({ id: "mail-c", cacheTtl: null })];
    await openForEdit("mail-c");
    const sent = await save();
    expect(sent.cacheTtl).toBeNull();
    expect(await screen.findByText(/Source 'mail-c' needs a Cache TTL/)).toBeInTheDocument();
    expect(screen.queryByText(/REQ-\d+/)).toBeNull();
    expect(updateSourceCache).not.toHaveBeenCalled(); // nothing is saved after a refused save
  });

  it.each(["pw-as-typed", "${secret:my_warehouse_password}"])(
    "shows a source's password field again as it was entered (%s) and sends it back unchanged",
    async (entered) => {
      SOURCES = [
        source({
          id: "sales-pg",
          type: "postgresql",
          host: "db",
          port: 5432,
          database: "sales",
          username: "u",
          cacheTtl: null,
          mappingJson: null,
          passwordRef: entered,
        } as Partial<Source>),
      ];
      await openForEdit("sales-pg");
      await userEvent.click(await screen.findByTestId("source-section-authentication"));
      const field = await screen.findByLabelText(/^Password/);
      expect(field).toHaveValue(entered);
      expect(field).toHaveAttribute("type", "password"); // behind the reveal control
      const sent = (await save()) as unknown as { password: string };
      expect(sent.password).toBe(entered);
    },
  );

  it("saves a database source with or without a Cache TTL, as before", async () => {
    SOURCES = [
      source({
        id: "sales-pg",
        type: "postgresql",
        host: "db",
        port: 5432,
        database: "sales",
        username: "u",
        cacheTtl: null,
        mappingJson: null,
      } as Partial<Source>),
    ];
    await openForEdit("sales-pg");
    const sent = await save();
    expect(sent.type).toBe("postgresql");
    expect(sent.cacheTtl).toBeNull();
    await waitFor(() => expect(updateSourceCache).toHaveBeenCalledWith("sales-pg", true, null));
  });
});
