// Copyright (c) 2026 Kenneth Stott
// Canary: 0fdbfc87-ea9a-4143-88fc-3f612bad7fec
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// The Sources type list is gated by the reach of the engine serving the org, read from the source
// kinds -- which whoever registers sources may read -- never from the platform's engine settings,
// which a sandbox visitor or an org administrator is refused (the page showed that refusal). A
// refusal of the reach itself is still shown.

import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, waitFor } from "../../test-utils/render";

const fetchSourceReach = vi.fn();
const fetchFederationEngine = vi.fn();

vi.mock("../../api/admin", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../api/admin")>()),
  fetchSettings: vi.fn().mockResolvedValue({
    redirect: { enabled: false, threshold: 10000, default_format: "json", ttl: 3600 },
    sampling: { default_sample_size: 1000 },
    cache: { default_ttl: 300 },
    naming: { domain_prefix: false, convention: "none" },
  }),
  fetchSourceReach: (...a: unknown[]) => fetchSourceReach(...a),
  fetchFederationEngine: (...a: unknown[]) => fetchFederationEngine(...a),
}));

vi.mock("../../context/AuthContext", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../context/AuthContext")>()),
  useAuth: () => ({
    role: "visitor",
    selectedRoles: ["visitor"],
    capabilities: ["source_registration"],
    domainAccess: ["*"],
  }),
}));

vi.mock("../../hooks/useAdminQueries", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../hooks/useAdminQueries")>()),
  useSources: () => ({ sources: [], loading: false, refetch: vi.fn().mockResolvedValue({}) }),
  useDomains: () => ({ domains: [], loading: false, refetch: vi.fn() }),
  useTables: () => ({ tables: [], loading: false, refetch: vi.fn() }),
}));

import { SourcesPage } from "../SourcesPage";

describe("the Sources page's engine reach", () => {
  beforeEach(() => {
    fetchSourceReach.mockReset();
    fetchFederationEngine.mockReset();
  });

  it("is read from the source kinds, never the platform's engine settings", async () => {
    fetchSourceReach.mockResolvedValue({
      engine: "duckdb",
      engines: [
        {
          key: "duckdb",
          label: "DuckDB",
          live_source_types: ["postgresql"],
          reachable_source_types: ["postgresql"],
        },
      ],
    });
    render(<SourcesPage />);
    await waitFor(() => expect(fetchSourceReach).toHaveBeenCalledTimes(1));
    expect(fetchFederationEngine).not.toHaveBeenCalled();
    expect(screen.queryByText(/fetch failed/)).toBeNull();
  });

  it("shows a refusal of the reach rather than dropping it", async () => {
    fetchSourceReach.mockRejectedValue(new Error("Source reach fetch failed (403)"));
    render(<SourcesPage />);
    expect(await screen.findByText(/Source reach fetch failed \(403\)/)).toBeTruthy();
  });
});
