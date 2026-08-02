// Copyright (c) 2026 Kenneth Stott
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

/**
 * Route chunks are code-split (lazy() in App.tsx), so the first visit to each
 * surface pays a chunk fetch/parse — worst on the Query/SQL/gRPC editors,
 * which pull Monaco (multi-MB). Vite dedupes modules by resolved id, so this
 * glob hits the SAME chunks as the lazy() imports in App.tsx.
 */
const PAGE_CHUNK_LOADERS = import.meta.glob("./pages/*.tsx");

/** Warm every page chunk on idle after first paint, so ordinary navigation is instant. */
export function prefetchPageChunksOnIdle(): () => void {
  const schedule =
    typeof window.requestIdleCallback === "function"
      ? window.requestIdleCallback
      : (cb: () => void) => window.setTimeout(cb, 200);
  const cancel =
    typeof window.cancelIdleCallback === "function" ? window.cancelIdleCallback : window.clearTimeout;
  const handle = schedule(() => {
    for (const load of Object.values(PAGE_CHUNK_LOADERS)) void load();
  });
  return () => cancel(handle as number);
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
  return Promise.all(Object.values(PAGE_CHUNK_LOADERS).map((load) => load().catch(() => undefined))).then(
    () => undefined,
  );
}
