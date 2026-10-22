// Copyright (c) 2026 Kenneth Stott
// Canary: 1a976757-74f3-460c-8fbb-6455318cd891
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// The response cache's per-table list follows the page's one search: a table is listed when any
// word of the search is in its name or its domain.

import { describe, expect, it, vi } from "vitest";
import { fireEvent, render, screen } from "../../../test-utils/render";

const table = (id: number, tableName: string, domainId: string) => ({
  id,
  tableName,
  alias: null,
  domainId,
  sourceId: "pg",
});

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
}));
vi.mock("../../../hooks/useAdminQueries", async (orig) => ({
  ...(await orig<typeof import("../../../hooks/useAdminQueries")>()),
  useTables: () => ({
    tables: [
      table(1, "orders", "sales"),
      table(2, "customers", "crm"),
      table(3, "invoices", "finance"),
    ],
    loading: false,
    refetch: vi.fn(),
  }),
  usePurgeCache: () => ({ purgeCache: vi.fn() }),
}));
vi.mock("../../../hooks/useRegionQueries", async (orig) => ({
  ...(await orig<typeof import("../../../hooks/useRegionQueries")>()),
  useRegionChoices: () => ({ regions: [], connected: null }),
}));
vi.mock("../../../api/admin", async (orig) => ({
  ...(await orig<typeof import("../../../api/admin")>()),
  fetchSettings: vi.fn(async () => ({ features: {} })),
}));
vi.mock("../CacheStorageTab", () => ({
  ResponseCacheSettingsPanel: () => null,
  HotTablesSettingsPanel: () => null,
  MaterializedSettingsPanel: () => null,
}));

import { CacheManager } from "../CacheManager";

describe("the response cache's table search", () => {
  it("lists the tables matching any word of the search", async () => {
    render(<CacheManager />);
    await screen.findByTestId("purge-table-btn-2");
    fireEvent.change(screen.getByTestId("filter-input"), { target: { value: "orders finance" } });
    expect(screen.getByTestId("purge-table-btn-1")).toBeInTheDocument();
    expect(screen.getByTestId("purge-table-btn-3")).toBeInTheDocument();
    expect(screen.queryByTestId("purge-table-btn-2")).toBeNull();
  });
});
