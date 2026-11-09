// Copyright (c) 2026 Kenneth Stott
// Canary: c4068402-4b1c-4011-8d0b-e8f21e20b968
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1327: each panel of the Cache page shows only to the right that holds it: the org's settings
// to org_settings, the materialized views' operations to table_registration. The platform's store
// panels are on the Platform page, never here.

import { beforeEach, describe, expect, it, vi } from "vitest";
import { fireEvent, render, screen } from "../../../test-utils/render";

const auth = vi.hoisted(() => ({
  capabilities: [] as string[],
  demonstrated: [] as string[],
}));
vi.mock("../../../context/AuthContext", () => ({ useAuth: () => auth }));
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
  useMVList: () => ({ mvList: [], loading: false, error: undefined, refetch: vi.fn() }),
  useMaterializeStoreInfo: () => ({ materializeStoreInfo: null }),
  useRefreshMV: () => ({ refreshMV: vi.fn() }),
  useToggleMV: () => ({ toggleMV: vi.fn() }),
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
  MaterializedSettingsPanel: () => <div data-testid="platform-materialize-panel" />,
}));

import { CacheManager } from "../CacheManager";

const tabs = () =>
  ["response", "hot", "materialized", "redirect"].filter((k) =>
    screen.queryByTestId(`cache-tab-${k}`),
  );

describe("the Cache page's gates", () => {
  beforeEach(() => {
    auth.capabilities = [];
    auth.demonstrated = [];
  });

  it("shows a holder of org_settings the org's panels and not the views' operations", async () => {
    auth.capabilities = ["org_settings"];
    render(<CacheManager />);
    expect(await screen.findByTestId("org-ttl-panel")).toBeInTheDocument();
    expect(tabs()).toEqual(["response", "hot", "redirect"]);
    fireEvent.click(screen.getByTestId("cache-tab-hot"));
    expect(await screen.findByTestId("org-hot-panel")).toBeInTheDocument();
  });

  it("opens a holder of table_registration alone on the views' operations", async () => {
    auth.capabilities = ["table_registration"];
    render(<CacheManager />);
    expect(tabs()).toEqual(["materialized"]);
    expect(await screen.findByTestId("mv-view-btn")).toBeInTheDocument();
    expect(screen.queryByTestId("org-ttl-panel")).toBeNull();
  });

  it("shows a holder of both rights every panel of the page", async () => {
    auth.capabilities = ["org_settings", "table_registration"];
    render(<CacheManager />);
    expect(await screen.findByTestId("org-ttl-panel")).toBeInTheDocument();
    expect(tabs()).toEqual(["response", "hot", "materialized", "redirect"]);
  });

  it("keeps a panel a role is shown rather than given on the page", async () => {
    auth.capabilities = ["table_registration"];
    auth.demonstrated = ["org_settings"];
    render(<CacheManager />);
    expect(tabs()).toEqual(["response", "hot", "materialized", "redirect"]);
  });

  it("holds no store connection, whichever rights the caller has", async () => {
    auth.capabilities = ["org_settings", "table_registration", "platform_settings", "cross_org"];
    render(<CacheManager />);
    await screen.findByTestId("org-ttl-panel");
    expect(screen.queryByTestId("platform-store-panel")).toBeNull();
    fireEvent.click(screen.getByTestId("cache-tab-materialized"));
    await screen.findByTestId("mv-view-btn");
    expect(screen.queryByTestId("platform-materialize-panel")).toBeNull();
  });
});
