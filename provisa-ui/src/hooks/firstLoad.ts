// Copyright (c) 2026 Kenneth Stott
// Canary: 952c5fd9-7d34-4e6d-981b-c0a433acdd57
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

/**
 * `loading` on the list hooks means "nothing to show yet", not "a request is in flight".
 *
 * They all read `cache-and-network`, which reports `loading: true` on every mount that revalidates
 * a warm cache. Pages gate their whole render on that flag, so a cached page would blank itself on
 * re-entry — which is what made the guided tour (REQ-1362) land on empty screens even after its
 * prefetch had already fetched the data. An undefined `data` is the only signal that there is
 * genuinely nothing to paint; a cached empty list is an answer, not an absence.
 */
export function firstLoad(loading: boolean, data: unknown): boolean {
  return loading && data === undefined;
}
