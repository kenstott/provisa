// Copyright (c) 2026 Kenneth Stott
// Canary: d3ce46f1-b66b-4061-a1c6-f168c5b308b3
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { lazy } from "react";

/**
 * Route chunks are code-split (lazy() in App.tsx), so the first visit to each
 * surface pays a chunk fetch/parse — worst on the Query/SQL/gRPC editors,
 * which pull Monaco (multi-MB). Vite dedupes modules by resolved id, so this
 * glob hits the SAME chunks as the lazy() imports in App.tsx.
 */
const PAGE_CHUNK_LOADERS = import.meta.glob("./pages/*.tsx");

/** Resolves when the first route's page chunk has arrived (lazyPage). */
let firstPageArrived!: () => void;
const firstPage = new Promise<void>((resolve) => {
  firstPageArrived = resolve;
});

/**
 * A route's page, code-split: lazy(), and the first page to arrive is what lets the warm-up start.
 * Every route page is loaded through here (App.tsx, OnboardGate.tsx), so the page the visitor
 * opened always loads ahead of the others.
 */
export const lazyPage: typeof lazy = (load) =>
  lazy(() =>
    load().then((page) => {
      firstPageArrived();
      return page;
    }),
  );

/**
 * Warm every page chunk once the first route's page has arrived and the browser is next idle, so
 * ordinary navigation is instant. Starting at mount instead queued the whole warm-up -- Monaco and
 * GraphiQL included -- ahead of the imports of the page the visitor opened, which then painted
 * late: on a slow link for a visitor, and on the e2e lane's dev server, hundreds of module fetches
 * late (run 39822399296).
 */
export function prefetchPageChunksOnIdle(): () => void {
  const schedule =
    typeof window.requestIdleCallback === "function"
      ? window.requestIdleCallback
      : (cb: () => void) => window.setTimeout(cb, 200);
  const cancel =
    typeof window.cancelIdleCallback === "function"
      ? window.cancelIdleCallback
      : window.clearTimeout;
  let stopped = false;
  let handle: number | null = null;
  void firstPage.then(() => {
    if (stopped) return;
    handle = schedule(() => {
      for (const load of Object.values(PAGE_CHUNK_LOADERS)) void load();
    }) as number;
  });
  return () => {
    stopped = true;
    if (handle !== null) cancel(handle);
  };
}

/**
 * Eagerly load and await every page chunk. Used before the guided tour starts:
 * the tour navigates between surfaces in quick succession, faster than a
 * step's own destination can pay its first-visit chunk fetch/parse, so a step
 * can end up highlighting an element in a persistent shell (navbar/subnav)
 * while the destination page is still compiling. Awaiting all chunks up
 * front removes that source of the "tour outruns the page" hang. Rejections
 * are swallowed per-chunk (not surfaced) since a single failed prefetch must
 * not block the tour from starting — the normal per-route Suspense fallback
 * still applies if a chunk genuinely fails to load later.
 */
export function prefetchAllPageChunks(): Promise<void> {
  return Promise.all(
    Object.values(PAGE_CHUNK_LOADERS).map((load) => load().catch(() => undefined)),
  ).then(() => undefined);
}
