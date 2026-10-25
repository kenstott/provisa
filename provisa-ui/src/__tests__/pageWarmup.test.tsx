// Copyright (c) 2026 Kenneth Stott
// Canary: 43309003-a518-4ecb-b0c8-c7e65dc4919e
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

/**
 * REQ-1362: every page chunk is warmed once the page the visitor opened has arrived and the browser
 * is idle -- never ahead of it. Started at mount, the warm-up queued every chunk (Monaco and
 * GraphiQL included) ahead of the opened page's own imports, which then painted late.
 *
 * Each test loads pageChunks afresh: whether a route's page has arrived is the module's own state.
 */

import { Suspense } from "react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { render, screen, waitFor } from "../test-utils/render";

type Chunks = typeof import("../pageChunks");

const original = {
  requestIdleCallback: window.requestIdleCallback,
  cancelIdleCallback: window.cancelIdleCallback,
};

beforeEach(() => vi.resetModules());
afterEach(() => Object.assign(window, original));

/** A route page through lazyPage, rendered as App renders one; resolves once it is on screen. */
async function openRoute(chunks: Chunks, order: string[]) {
  const Page = chunks.lazyPage(() => {
    order.push("page");
    return Promise.resolve({ default: () => <p>the opened page</p> });
  });
  render(
    <Suspense fallback={<p>loading</p>}>
      <Page />
    </Suspense>,
  );
  await screen.findByText("the opened page");
}

describe("the page chunk warm-up", () => {
  it("starts on idle after the opened route's page has arrived, never before", async () => {
    const order: string[] = [];
    const schedule = vi.fn(() => {
      order.push("warm-up");
      return 7;
    });
    Object.assign(window, { requestIdleCallback: schedule, cancelIdleCallback: vi.fn() });
    const chunks: Chunks = await import("../pageChunks");

    chunks.prefetchPageChunksOnIdle();
    await new Promise((r) => setTimeout(r, 20));
    expect(schedule).not.toHaveBeenCalled();

    await openRoute(chunks, order);
    await waitFor(() => expect(schedule).toHaveBeenCalledTimes(1));
    expect(order).toEqual(["page", "warm-up"]);
  });

  it("is cancellable: stopped before or after it is scheduled, nothing stays armed", async () => {
    const schedule = vi.fn().mockReturnValue(7);
    const cancel = vi.fn();
    Object.assign(window, { requestIdleCallback: schedule, cancelIdleCallback: cancel });
    const chunks: Chunks = await import("../pageChunks");

    // Stopped before any page arrived: the warm-up is never scheduled.
    const early = chunks.prefetchPageChunksOnIdle();
    early();
    await openRoute(chunks, []);
    await new Promise((r) => setTimeout(r, 20));
    expect(schedule).not.toHaveBeenCalled();

    // Scheduled, then stopped (an unmount mid-idle): the idle callback is cancelled.
    const late = chunks.prefetchPageChunksOnIdle();
    await waitFor(() => expect(schedule).toHaveBeenCalledTimes(1));
    late();
    expect(cancel).toHaveBeenCalledWith(7);
  });

  it("stopped while it loads, starts no further chunk and settles", async () => {
    // The idle callback runs at once, so the warm-up is under way when it is stopped.
    Object.assign(window, {
      requestIdleCallback: (cb: () => void) => {
        cb();
        return 1;
      },
      cancelIdleCallback: vi.fn(),
    });
    const loads = vi.spyOn(console, "error");
    const chunks: Chunks = await import("../pageChunks");

    const idle = chunks.warmupSettled();
    const stop = chunks.prefetchPageChunksOnIdle();
    await openRoute(chunks, []);
    // Under way: the warm-up's loading is no longer the idle one.
    await waitFor(() => expect(chunks.warmupSettled()).not.toBe(idle));
    stop();
    // Settles once the chunk in flight arrives: nothing is left loading into a torn-down page.
    await chunks.warmupSettled();
    expect(loads).not.toHaveBeenCalled();
    loads.mockRestore();
  });

  it("falls back to a timeout where requestIdleCallback is missing", async () => {
    Object.assign(window, { requestIdleCallback: undefined, cancelIdleCallback: undefined });
    const timeout = vi.spyOn(window, "setTimeout");
    const chunks: Chunks = await import("../pageChunks");

    const stop = chunks.prefetchPageChunksOnIdle();
    await openRoute(chunks, []);
    await waitFor(() => expect(timeout).toHaveBeenCalledWith(expect.any(Function), 200));
    stop();
    timeout.mockRestore();
  });
});
