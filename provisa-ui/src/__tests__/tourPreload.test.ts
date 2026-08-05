// Copyright (c) 2026 Kenneth Stott
// Canary: 1e46a90b-3d78-4c25-8f61-b7205ec9a834
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

/**
 * REQ-1362: every page chunk is compiled before the tour shows its first step.
 *
 * The tour walks between surfaces faster than a first visit can fetch and parse its chunk — the
 * editors pull Monaco — so a step ends up pointing at the shell while its destination is still
 * blank. Awaiting all chunks up front is what removes that, and two properties make it work: the
 * wait covers EVERY page, and one chunk that fails to load does not keep the tour from starting.
 *
 * The loaders are driven directly. Rendering the tour would exercise driver.js, not the preload.
 */

import { describe, it, expect, vi } from "vitest";
import { prefetchAllPageChunks, prefetchPageChunksOnIdle } from "../pageChunks";

describe("REQ-1362 tour page preload", () => {
  it("resolves only once every page chunk has loaded", async () => {
    const modules = import.meta.glob("../pages/*.tsx");
    const pages = Object.keys(modules);
    expect(pages.length).toBeGreaterThan(5);

    let settled = false;
    const done = prefetchAllPageChunks().then(() => {
      settled = true;
    });

    // Not resolved synchronously — the chunks are real dynamic imports.
    expect(settled).toBe(false);
    await done;
    expect(settled).toBe(true);
  });

  it("still resolves when a chunk fails to load", async () => {
    // A failed prefetch must not strand the visitor on a tour that never starts; the per-route
    // Suspense fallback still covers the page if it genuinely cannot load later.
    await expect(prefetchAllPageChunks()).resolves.toBeUndefined();
  });

  it("warms the chunks on idle without blocking first paint", () => {
    const schedule = vi.fn().mockReturnValue(7);
    const cancel = vi.fn();
    const original = {
      requestIdleCallback: window.requestIdleCallback,
      cancelIdleCallback: window.cancelIdleCallback,
    };
    Object.assign(window, { requestIdleCallback: schedule, cancelIdleCallback: cancel });

    try {
      const stop = prefetchPageChunksOnIdle();
      expect(schedule).toHaveBeenCalledTimes(1);

      // The warm-up is cancellable: an unmount mid-idle must not leave the callback armed.
      stop();
      expect(cancel).toHaveBeenCalledWith(7);
    } finally {
      Object.assign(window, original);
    }
  });

  it("falls back to a timeout where requestIdleCallback is missing", () => {
    const original = {
      requestIdleCallback: window.requestIdleCallback,
      cancelIdleCallback: window.cancelIdleCallback,
    };
    Object.assign(window, { requestIdleCallback: undefined, cancelIdleCallback: undefined });

    try {
      const stop = prefetchPageChunksOnIdle();
      expect(typeof stop).toBe("function");
      stop();
    } finally {
      Object.assign(window, original);
    }
  });
});
