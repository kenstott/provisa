// Copyright (c) 2026 Kenneth Stott
// Canary: 5e7b2d90-41c3-4a8f-b6d1-0c9f3e8a72b5
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

/**
 * REQ-2201: a reference library document's address as the screens mark it, for a demo's tour to
 * find its row and its scans. Encoded, so the mark is safe in an attribute selector: an address
 * carries `&`, which some selector engines read as part of the selector even inside quotes.
 */
export function documentMark(source: string): string {
  return encodeURIComponent(source);
}
