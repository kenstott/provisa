// Copyright (c) 2026 Kenneth Stott
// Canary: 0d6b3f27-85c4-4e9a-b1f2-6a4c7e90d318
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2225 (amended 2026-11-24): Spark is a built-in source type like every other, listed in the
// Code group beside Python, Node, Go and Rust, never in a separate Connectors section, and made
// through the same source form: its cluster as fields. A Spark source has no script (amended
// 2026-11-25).

import { beforeEach, describe, expect, it, vi } from "vitest";
import { fireEvent, render, screen, waitFor, within } from "../../test-utils/render";
import userEvent from "@testing-library/user-event";
import type { Connector } from "../../api/connectors";
import type { MutationResult } from "../../types/admin";

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
    activeOrgId: "acme",
  }),
}));

const ok: MutationResult = { success: true, message: "" };
const createSource = vi.fn();
const refetch = vi.fn();

vi.mock("../../hooks/useAdminQueries", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../hooks/useAdminQueries")>()),
  useSources: () => ({ sources: [], loading: false, refetch }),
  useDomains: () => ({ domains: [], loading: false, refetch: vi.fn() }),
  useTables: () => ({ tables: [], loading: false, refetch: vi.fn() }),
  useCreateSource: () => ({ createSource }),
  useUpdateSourceCache: () => ({ updateSourceCache: vi.fn().mockResolvedValue(ok) }),
  useUpdateSourceLoadProtection: () => ({
    updateSourceLoadProtection: vi.fn().mockResolvedValue(ok),
  }),
  useUpdateSourceAllowedDomains: () => ({
    updateSourceAllowedDomains: vi.fn().mockResolvedValue(ok),
  }),
  useAvailableSchemas: () => ({ schemas: [], loading: false }),
  useAvailableTables: () => ({ tables: [], loading: false }),
}));

import { SourcesPage } from "../SourcesPage";
import { forgetSourceKinds } from "../sources/sourceKind";

const SPARK_HOST: Connector = {
  name: "spark",
  git: "/checkout",
  ref: "main",
  commit: "a".repeat(40),
  language: "python",
  image: "",
  digest: "",
  code_hash: "c".repeat(64),
  arch: "amd64",
  requires: [
    { name: "cluster", org_default: "spark_cluster" },
  ],
  description: "A connection to your Spark cluster through Spark Connect.",
  icon: "",
  settings: [{ name: "replicate", default_value: 0 }],
  native: true,
};

const CRM: Connector = {
  ...SPARK_HOST,
  name: "crm",
  description: "Our CRM",
  requires: [{ name: "region" }],
  settings: [],
  native: undefined,
  image: "registry.example/data/crm",
  digest: `sha256:${"b".repeat(64)}`,
};

let offered: Connector[] = [];

beforeEach(() => {
  offered = [];
  createSource.mockReset().mockResolvedValue(ok);
  refetch.mockReset().mockResolvedValue({ data: { sources: [] } });
  forgetSourceKinds();
  vi.spyOn(globalThis, "fetch").mockImplementation(async (url) => {
    const asked = String(url);
    if (asked.includes("/admin/source-kinds/")) {
      return new Response(
        JSON.stringify({ cache_ttl_required: false, sentinel_path: true, tables_land: false }),
      );
    }
    if (asked.endsWith("/admin/orgs/acme/connectors")) {
      return new Response(JSON.stringify({ connectors: offered }));
    }
    if (asked.endsWith("/admin/orgs/acme/builtin-sources/spark")) {
      return new Response(
        JSON.stringify({
          host: SPARK_HOST,
          default_cluster: {
            origin: "local_test",
            label: "local test cluster",
            target: "127.0.0.1",
          },
        }),
      );
    }
    return new Response(JSON.stringify({}));
  });
});

async function openPicker(): Promise<HTMLElement> {
  await userEvent.click(await screen.findByTestId("sources-add-toggle"));
  return screen.findByTestId("source-type-picker");
}

describe("the add-source picker", () => {
  it("lists Spark in the Code group, with no Connectors section when none is offered", async () => {
    render(<SourcesPage />);
    const picker = await openPicker();
    await waitFor(() =>
      expect(within(picker).getAllByTestId("source-type-option-spark")).not.toHaveLength(0),
    );
    expect(within(picker).queryByTestId("source-type-picker-cat-Connectors")).toBeNull();
    fireEvent.click(within(picker).getByTestId("source-type-picker-cat-Code"));
    const code = ["node", "python", "go", "rust", "spark"].map(
      (t) => within(picker).queryAllByTestId(`source-type-option-${t}`).length,
    );
    expect(code).toEqual([1, 1, 1, 1, 1]);
  });

  it("lists an offered connector under Connectors, and Spark still only in Code", async () => {
    offered = [CRM];
    render(<SourcesPage />);
    const picker = await openPicker();
    fireEvent.click(await within(picker).findByTestId("source-type-picker-cat-Connectors"));
    expect(within(picker).getAllByTestId("source-type-option-connector:crm")).not.toHaveLength(0);
    expect(within(picker).queryByTestId("source-type-option-connector:spark")).toBeNull();
    expect(within(picker).queryByTestId("source-type-option-spark")).toBeNull();
  });
});

describe("a Spark source", () => {
  it("is created through the normal form, joining the Spark host", async () => {
    render(<SourcesPage />);
    const picker = await openPicker();
    await userEvent.click((await within(picker).findAllByTestId("source-type-option-spark"))[0]);
    await screen.findByTestId("sources-submit");
    expect(screen.getByTestId("sources-type-select")).toHaveValue("spark");
    fireEvent.change(screen.getByTestId("sources-id-input"), { target: { value: "spark_jobs" } });
    expect(await screen.findByTestId("spark-source-cluster-default")).toHaveTextContent(
      "local test cluster (127.0.0.1)",
    );
    expect(screen.queryByTestId("connector-require-script")).toBeNull();
    fireEvent.change(screen.getByTestId("spark-source-cluster-host"), {
      target: { value: "spark.acme.test" },
    });
    // Spark replicates by default (its host's settings): a replicated ttl source states its TTL.
    fireEvent.change(screen.getByTestId("cache-ttl-input"), { target: { value: "300" } });
    await userEvent.click(screen.getByTestId("sources-submit"));
    await waitFor(() => expect(createSource).toHaveBeenCalled());
    const sent = createSource.mock.calls[0][0] as { type: string; mappingJson: string };
    expect(sent.type).toBe("spark");
    expect(JSON.parse(sent.mappingJson)).toEqual({
      connector_join: { connector: "spark" },
      secrets: {},
      settings: { cluster: "sc://spark.acme.test:15002" },
    });
  });
});
