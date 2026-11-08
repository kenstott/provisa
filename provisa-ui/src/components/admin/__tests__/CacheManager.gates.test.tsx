// Copyright (c) 2026 Kenneth Stott
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1327: on the Cache page the org's panels show to every holder of the page and the platform's
// store panels only to a platform administrator.

import { describe, expect, it, vi } from "vitest";
import { fireEvent, render, screen } from "../../../test-utils/render";

const settings = vi.hoisted(() => ({ deployment: false }));
vi.mock("../../../context/AuthContext", () => ({
  useAuth: () => ({ capabilities: ["org_settings"] }),
}));
vi.mock("../../../api/admin", async (orig) => ({
  ...(await orig<typeof import("../../../api/admin")>()),
  fetchSettings: vi.fn(async () => ({ features: { deployment_settings: settings.deployment } })),
}));
vi.mock("../../../hooks/useAdminOpsQueries", async (orig) => ({
  ...(await orig<typeof import("../../../hooks/useAdminOpsQueries")>()),
  useCacheStats: () => ({
    cacheStats: {
      storeType: "memory",
      totalKeys: 0,
      hitCount: 0,
      missCount: 0,
      usedMemoryBytes: null,
      maxMemoryBytes: null,
      evictedKeys: 0,
      expiredKeys: 0,
      connectedClients: 0,
      opsPerSec: 0,
    },
    refetch: vi.fn(),
  }),
  useCacheTableStats: () => ({ cacheTableStats: [], refetch: vi.fn() }),
  usePurgeCacheByTable: () => ({ purgeCacheByTable: vi.fn() }),
  useHotTables: () => ({ hotTables: [], loading: false, error: undefined, refetch: vi.fn() }),
}));
vi.mock("../../../hooks/useAdminQueries", async (orig) => ({
  ...(await orig<typeof import("../../../hooks/useAdminQueries")>()),
  useTables: () => ({ tables: [], loading: false, refetch: vi.fn() }),
  usePurgeCache: () => ({ purgeCache: vi.fn() }),
}));
vi.mock("../../../hooks/useRegionQueries", async (orig) => ({
  ...(await orig<typeof import("../../../hooks/useRegionQueries")>()),
  useRegionChoices: () => ({ regions: [], connected: null }),
}));
vi.mock("../CacheStorageTab", () => ({
  ResponseCacheSettingsPanel: () => <div data-testid="org-ttl-panel" />,
  HotCacheStoreSettingsPanel: () => <div data-testid="platform-store-panel" />,
  HotTablesSettingsPanel: () => <div data-testid="org-hot-panel" />,
  MaterializedSettingsPanel: () => null,
}));

import { CacheManager } from "../CacheManager";

describe("the Cache page's gates", () => {
  it("shows an org administrator the org's panels and no store connection", async () => {
    settings.deployment = false;
    render(<CacheManager />);
    expect(await screen.findByTestId("org-ttl-panel")).toBeInTheDocument();
    expect(screen.queryByTestId("platform-store-panel")).toBeNull();
    fireEvent.click(screen.getByTestId("cache-tab-hot"));
    expect(await screen.findByTestId("org-hot-panel")).toBeInTheDocument();
  });

  it("shows a platform administrator the store connection too", async () => {
    settings.deployment = true;
    render(<CacheManager />);
    expect(await screen.findByTestId("platform-store-panel")).toBeInTheDocument();
    expect(screen.getByTestId("org-ttl-panel")).toBeInTheDocument();
  });
});
