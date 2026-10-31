// Copyright (c) 2026 Kenneth Stott
// Canary: 2d9c4f17-6ab3-4c0e-8e75-b1f3a7d05c28
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// Large lists live in IndexedDB. Copies an earlier version left in localStorage move across and give
// the space back; a full IndexedDB gives up the largest old lists; a refused one is logged, not fatal.

import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

const idb = vi.hoisted(() => ({
  held: new Map<string, unknown>(),
  capacity: Infinity,
  refuse: false,
  hang: false,
  calls: 0,
}));

vi.mock("idb-keyval", () => {
  const size = (v: unknown) => JSON.stringify(v)?.length ?? 0;
  const used = () => [...idb.held.values()].reduce<number>((n, v) => n + size(v), 0);
  const guard = async () => {
    idb.calls += 1;
    if (idb.hang) await new Promise(() => undefined);
    if (idb.refuse) throw new DOMException("denied", "SecurityError");
  };
  return {
    get: async (k: string) => (await guard(), idb.held.get(k)),
    getMany: async (ks: string[]) => ks.map((k) => idb.held.get(k)),
    keys: async () => [...idb.held.keys()],
    del: async (k: string) => void idb.held.delete(k),
    set: async (k: string, v: unknown) => {
      await guard();
      if (used() - size(idb.held.get(k) ?? null) + size(v) > idb.capacity) {
        throw new DOMException("full", "QuotaExceededError");
      }
      idb.held.set(k, v);
    },
  };
});

type Bulk = typeof import("../lib/bulkStorage");

const SCOPE = "test-cp:test-org:";
let bulk: Bulk;
let errors: ReturnType<typeof vi.spyOn>;

/** A fresh page load: the move runs once per load. */
async function load(): Promise<void> {
  vi.resetModules();
  bulk = await import("../lib/bulkStorage");
  const { bindStorageScope } = await import("../lib/browserStorage");
  bindStorageScope({ controlPlaneId: "test-cp", orgId: "test-org" });
}

beforeEach(async () => {
  idb.held.clear();
  idb.capacity = Infinity;
  idb.refuse = false;
  idb.hang = false;
  idb.calls = 0;
  localStorage.clear();
  errors = vi.spyOn(console, "error").mockImplementation(() => undefined);
  await load();
});

afterEach(() => errors.mockRestore());

describe("copies left in localStorage", () => {
  it("move across, from every scope, and give the space back", async () => {
    localStorage.setItem(`${SCOPE}provisa.graph.state`, JSON.stringify({ frames: [1] }));
    localStorage.setItem("other::provisa.sparql.state", JSON.stringify({ frames: [2] }));
    localStorage.setItem(`${SCOPE}nl-question`, "stays");
    await bulk.startBulkMove();
    expect(idb.held.get(`${SCOPE}provisa.graph.state`)).toEqual({ frames: [1] });
    expect(idb.held.get("other::provisa.sparql.state")).toEqual({ frames: [2] });
    expect(localStorage.getItem(`${SCOPE}provisa.graph.state`)).toBeNull();
    expect(localStorage.getItem("other::provisa.sparql.state")).toBeNull();
    expect(localStorage.getItem(`${SCOPE}nl-question`)).toBe("stays");
  });

  it("are read back on the first read, without a separate start", async () => {
    localStorage.setItem(`${SCOPE}nl-branches`, JSON.stringify({ query: "q" }));
    expect(await bulk.readBulk("nl-branches")).toEqual({ query: "q" });
  });

  it("do not overwrite what IndexedDB already holds", async () => {
    idb.held.set(`${SCOPE}provisa.graph.state`, { frames: ["newer"] });
    localStorage.setItem(`${SCOPE}provisa.graph.state`, JSON.stringify({ frames: ["older"] }));
    await bulk.startBulkMove();
    expect(idb.held.get(`${SCOPE}provisa.graph.state`)).toEqual({ frames: ["newer"] });
    expect(localStorage.getItem(`${SCOPE}provisa.graph.state`)).toBeNull();
  });

  it("stay where they are when IndexedDB refuses, to be evicted if room is needed", async () => {
    idb.refuse = true;
    localStorage.setItem(`${SCOPE}provisa.graph.state`, "{}");
    await bulk.startBulkMove();
    expect(localStorage.getItem(`${SCOPE}provisa.graph.state`)).toBe("{}");
    expect(errors).toHaveBeenCalled();
  });

  it("are dropped when corrupt, logged", async () => {
    localStorage.setItem(`${SCOPE}provisa.graph.state`, "{not json");
    await bulk.startBulkMove();
    expect(localStorage.getItem(`${SCOPE}provisa.graph.state`)).toBeNull();
    expect(idb.held.size).toBe(0);
    expect(errors).toHaveBeenCalled();
  });
});

describe("reads and writes", () => {
  it("run in the order asked", async () => {
    await bulk.writeBulk("nl-branches", "old");
    const before = bulk.readBulk("nl-branches");
    void bulk.writeBulk("nl-branches", "new");
    const after = bulk.readBulk("nl-branches");
    expect(await before).toBe("old");
    expect(await after).toBe("new");
  });

  it("are scoped as settings are", async () => {
    await bulk.writeBulk("nl-branches", "mine");
    expect(idb.held.get(`${SCOPE}nl-branches`)).toBe("mine");
  });

  it("make only the last of the writes queued behind a slow one", async () => {
    const seen: unknown[] = [];
    void bulk.startBulkMove();
    const realSet = Map.prototype.set;
    const spy = vi.spyOn(idb.held, "set").mockImplementation(function (this: Map<string, unknown>, k, v) {
      seen.push(v);
      return realSet.call(this, k, v) as Map<string, unknown>;
    });
    void bulk.writeBulk("nl-branches", 1);
    void bulk.writeBulk("nl-branches", 2);
    await bulk.writeBulk("nl-branches", 3);
    spy.mockRestore();
    expect(seen).toEqual([3]);
  });

  it("read as nothing saved when IndexedDB refuses, and a write goes on, logged", async () => {
    idb.refuse = true;
    expect(await bulk.readBulk("nl-branches")).toBeNull();
    await expect(bulk.writeBulk("nl-branches", "x")).resolves.toBeUndefined();
    expect(errors).toHaveBeenCalled();
  });
});

describe("a full IndexedDB", () => {
  it("gives up old frames, the largest first, keeps other lists, and retries once", async () => {
    idb.held.set("a:b:provisa.graph.state", "x".repeat(100));
    idb.held.set("c:d:provisa.sparql.state", "x".repeat(10));
    idb.held.set(`${SCOPE}nl-branches`, "n".repeat(20));
    idb.held.set("apollo", "kept");
    idb.capacity = 140;
    await bulk.writeBulk("provisa.sparql.state", "y".repeat(60));
    expect(idb.held.has("a:b:provisa.graph.state")).toBe(false);
    expect(idb.held.has("c:d:provisa.sparql.state")).toBe(true);
    expect(idb.held.has(`${SCOPE}nl-branches`)).toBe(true);
    expect(idb.held.get("apollo")).toBe("kept");
    expect(idb.held.get(`${SCOPE}provisa.sparql.state`)).toBe("y".repeat(60));
  });

  it("logs a list that still does not fit", async () => {
    idb.capacity = 5;
    await bulk.writeBulk("nl-branches", "y".repeat(60));
    expect(idb.held.size).toBe(0);
    expect(errors).toHaveBeenCalled();
  });
});

describe("an IndexedDB that never answers", () => {
  beforeEach(() => vi.useFakeTimers());
  afterEach(() => vi.useRealTimers());

  it("surfaces after 5 seconds, once, and is not used again this session", async () => {
    const { listenForStorageNotice } = await import("../lib/browserStorage");
    const heard: string[] = [];
    listenForStorageNotice((kind) => heard.push(kind));
    await bulk.startBulkMove();
    idb.hang = true;
    const read = bulk.readBulk("nl-branches");
    await vi.advanceTimersByTimeAsync(4999);
    expect(errors).not.toHaveBeenCalled();
    await vi.advanceTimersByTimeAsync(1);
    expect(await read).toBeNull();
    expect(heard).toEqual(["settings"]);
    expect(errors).toHaveBeenCalledTimes(1);

    const calls = idb.calls;
    idb.hang = false; // a late answer changes nothing: it is unavailable for the session
    expect(await bulk.readBulk("nl-branches")).toBeNull();
    await bulk.writeBulk("nl-branches", "dropped");
    await bulk.removeBulk("nl-branches");
    expect(idb.calls).toBe(calls);
    expect(idb.held.size).toBe(0);
    expect(heard).toEqual(["settings"]);
    expect(errors).toHaveBeenCalledTimes(1);
  });

  it("times a write out the same way", async () => {
    await bulk.startBulkMove();
    idb.hang = true;
    const write = bulk.writeBulk("nl-branches", "x");
    await vi.advanceTimersByTimeAsync(5000);
    await expect(write).resolves.toBeUndefined();
    expect(errors).toHaveBeenCalledTimes(1);
  });

  it("does not hold the queue when the move itself hangs, and leaves the copy in place", async () => {
    idb.hang = true;
    localStorage.setItem(`${SCOPE}provisa.graph.state`, "{}");
    localStorage.setItem(`${SCOPE}provisa.sparql.state`, "{}");
    const read = bulk.readBulk("nl-branches");
    await vi.advanceTimersByTimeAsync(5000);
    expect(await read).toBeNull();
    expect(localStorage.getItem(`${SCOPE}provisa.graph.state`)).toBe("{}");
    expect(localStorage.getItem(`${SCOPE}provisa.sparql.state`)).toBe("{}");
    expect(errors).toHaveBeenCalledTimes(1);
  });
});
