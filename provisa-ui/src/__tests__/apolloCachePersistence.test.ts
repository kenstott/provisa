// Copyright (c) 2026 Kenneth Stott
// Canary: 8f1c04b7-59ae-4d3a-9f61-2c7d5ea0b913
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

/**
 * The persisted admin snapshot lives in IndexedDB, not localStorage.
 *
 * localStorage is a ~5 MB budget shared by the whole origin, and this snapshot grows with the
 * catalog: on a 54-table model the write threw QuotaExceededError out of a setInterval, where
 * nothing could catch it, and the snapshot already on disk went on occupying the budget — which is
 * why the guided tour hung, its own localStorage write being the one that threw.
 */

import { describe, it, expect, beforeEach, vi } from "vitest";
import { get as idbGet, set as idbSet, del as idbDel } from "idb-keyval";

const CACHE_KEY = "apollo-cache";
const SNAPSHOT = { ROOT_QUERY: { __typename: "Query", roles: [{ __typename: "Role", id: "r1" }] } };

async function loadClientModule() {
  vi.resetModules();
  return await import("../apolloClient");
}

describe("persisted Apollo cache (IndexedDB)", () => {
  beforeEach(async () => {
    localStorage.clear();
    await idbDel(CACHE_KEY);
  });

  it("restores a snapshot stored under the current version", async () => {
    const { CACHE_VERSION } = await import("../apolloClient");
    await idbSet(CACHE_KEY, { version: CACHE_VERSION, data: SNAPSHOT });

    const { cacheRestored, client } = await loadClientModule();
    await cacheRestored;

    expect(client.cache.extract()).toMatchObject(SNAPSHOT);
  });

  it("refuses a snapshot from another schema version and discards it", async () => {
    await idbSet(CACHE_KEY, { version: "not-the-current-version", data: SNAPSHOT });

    const { cacheRestored, client } = await loadClientModule();
    await cacheRestored;

    expect(client.cache.extract()).toEqual({});
    // Left in place it would be re-examined on every load, and it can be megabytes.
    expect(await idbGet(CACHE_KEY)).toBeUndefined();
  });

  it("clearing the session drops the snapshot from both stores", async () => {
    const { CACHE_VERSION, clearPersistedAdminCache } = await loadClientModule();
    await idbSet(CACHE_KEY, { version: CACHE_VERSION, data: SNAPSHOT });
    // What a browser that ran the pre-IndexedDB build still holds.
    localStorage.setItem(CACHE_KEY, JSON.stringify(SNAPSHOT));

    clearPersistedAdminCache();

    await vi.waitFor(async () => expect(await idbGet(CACHE_KEY)).toBeUndefined());
    expect(localStorage.getItem(CACHE_KEY)).toBeNull();
  });
});
