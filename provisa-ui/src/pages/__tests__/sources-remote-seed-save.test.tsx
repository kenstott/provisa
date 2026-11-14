// Copyright (c) 2026 Kenneth Stott
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// An OpenAPI source keeps its spec and address in a registration of its own, read after its edit
// form opens. A Save before that read answered sent the form's empty spec path, and the server
// refused the save ("Is a directory: '.'"): a source just added, whose Cache TTL was set at once,
// could not be saved. Save waits for the source's stored settings.

import { describe, it, expect, vi } from "vitest";
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

const ok: MutationResult = { success: true, message: "" };
const SOURCES = [
  {
    id: "api",
    type: "openapi",
    host: "",
    port: 0,
    database: "",
    username: "",
    path: "https://pets.test/openapi.json",
    cacheEnabled: true,
    cacheTtl: 300,
    changeSignal: "ttl",
    mappingJson: null,
  } as unknown as Source,
];

vi.mock("../../hooks/useAdminQueries", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../hooks/useAdminQueries")>()),
  useSources: () => ({ sources: SOURCES, loading: false, refetch: vi.fn().mockResolvedValue({}) }),
  useDomains: () => ({ domains: [], loading: false, refetch: vi.fn() }),
  useUpdateSource: () => ({ updateSource: vi.fn().mockResolvedValue(ok) }),
  useRenameSource: () => ({ renameSource: vi.fn().mockResolvedValue(ok) }),
}));

import { SourcesPage } from "../SourcesPage";

describe("an OpenAPI source's edit saves its stored spec", () => {
  it("offers Save only once the registration is read, and sends its spec path", async () => {
    let answerList: () => void = () => {};
    const listed = new Promise<void>((resolve) => {
      answerList = resolve;
    });
    const registered: unknown[] = [];
    vi.spyOn(globalThis, "fetch").mockImplementation(async (url, init) => {
      const asked = String(url);
      if (asked.includes("/admin/openapi/list")) {
        await listed;
        return new Response(
          JSON.stringify([
            {
              source_id: "api",
              spec_path: "https://pets.test/openapi.json",
              base_url: "https://pets.test",
              cache_ttl: 300,
              auth_config: null,
            },
          ]),
        );
      }
      if (asked.includes("/admin/openapi/register")) {
        registered.push(JSON.parse(String(init?.body)));
        return new Response(JSON.stringify({ success: true }));
      }
      if (asked.includes("/admin/source-kinds/")) {
        return new Response(JSON.stringify({ cache_ttl_required: false }));
      }
      return new Response(JSON.stringify({}));
    });

    render(<SourcesPage />);
    await userEvent.click(await screen.findByText("api"));
    await userEvent.click(await screen.findByTestId("source-detail-edit"));
    const save = await screen.findByTestId("sources-edit-submit");
    expect(save).toBeDisabled();

    answerList();
    await waitFor(() => expect(save).toBeEnabled());
    await userEvent.click(save);
    await waitFor(() => expect(registered).toHaveLength(1));
    expect(registered[0]).toMatchObject({ spec_path: "https://pets.test/openapi.json" });
  });
});
