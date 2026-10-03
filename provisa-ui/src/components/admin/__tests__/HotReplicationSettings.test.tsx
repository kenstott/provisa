// Copyright (c) 2026 Kenneth Stott
// Canary: d27a2c85-9b4a-4559-a63a-54cc6fc24c7e
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-826: the three settings that decide when a busy table is replicated — the Default
// threshold, the interval the count is taken over, and the size ceiling — are shown and saved on
// the Hot Tables settings panel. The engine's filesystem read cache (REQ-238) stays beside them.

import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent, waitFor } from "../../../test-utils/render";
import * as api from "../../../api/admin";
import type { CacheStorageState } from "../../../api/admin";
import { HotTablesSettingsPanel } from "../CacheStorageTab";

vi.mock("../../../api/admin", async (orig) => ({
  ...(await orig<typeof import("../../../api/admin")>()),
  fetchCacheStorage: vi.fn(),
  setCacheStorage: vi.fn(),
}));

const STATE: CacheStorageState = {
  cache: { enabled: true, redis_url: "", default_ttl: 300 },
  hot_tables: { auto_threshold: 1000, max_rows: 1000, max_bytes: 1048576, refresh_interval: null },
  replication: { hot_threshold: 100, hot_interval: 60, hot_max_rows: 10000000 },
  warm_tables: {
    fs_cache_enabled: false,
    fs_cache_directories: "/tmp/engine-cache",
    fs_cache_max_sizes: "10GB",
  },
  materialized_views: { default_ttl: 300 },
  materialize: { store_url: "", default_store_url: "" },
  restart_required_note: "",
};

beforeEach(() => {
  vi.mocked(api.fetchCacheStorage).mockReset().mockResolvedValue(STATE);
  vi.mocked(api.setCacheStorage)
    .mockReset()
    .mockResolvedValue({ success: true, updated: [], restart_required: false });
  window.localStorage.clear();
});

async function openPanel() {
  render(<HotTablesSettingsPanel />);
  const panel = await screen.findByTestId("hot-tables-settings");
  fireEvent.click(panel.querySelector("button") as HTMLButtonElement);
  return screen.findByTestId("replication-hot-threshold");
}

describe("Hot replication settings", () => {
  it("shows the Default threshold, the interval and the size ceiling", async () => {
    await openPanel();
    expect(screen.getByText("Replicate when busy")).toBeInTheDocument();
    expect(screen.getByTestId("replication-hot-threshold")).toHaveValue("100");
    expect(screen.getByTestId("replication-hot-interval")).toHaveValue("60");
    expect(screen.getByTestId("replication-hot-max-rows")).toHaveValue("10000000");
    expect(screen.getByText("Default threshold (statements per interval)")).toBeInTheDocument();
    // the read cache is still here, under its own heading
    expect(screen.getByText("Engine filesystem read cache (SSD)")).toBeInTheDocument();
  });

  it("saves the replication block with the edited values", async () => {
    const threshold = await openPanel();
    fireEvent.change(threshold, { target: { value: "250" } });
    fireEvent.change(screen.getByTestId("replication-hot-interval"), { target: { value: "120" } });
    fireEvent.click(screen.getByTestId("cache-storage-save"));
    await waitFor(() => expect(api.setCacheStorage).toHaveBeenCalledTimes(1));
    const sent = vi.mocked(api.setCacheStorage).mock.calls[0][0];
    expect(sent.replication).toEqual({
      hot_threshold: 250,
      hot_interval: 120,
      hot_max_rows: 10000000,
    });
    expect(Object.keys(sent).sort()).toEqual(["hot_tables", "replication", "warm_tables"]);
    expect(sent.warm_tables).toEqual(STATE.warm_tables);
  });
});
