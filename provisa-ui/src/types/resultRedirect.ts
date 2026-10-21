// Copyright (c) 2026 Kenneth Stott
// Canary: c80ab448-c2b4-48e2-86ee-4f7a84171533
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// A type of its own, not api/admin's: the graph model reads it, and the headless graph renderer
// (graph-render/) compiles the graph model without the app's client or vite's types.

/** Where a result landed in the results store is, as /data/sql's redirect handle names it. */
export interface ResultRedirect {
  url: string;
  rowCount: number;
  /** Seconds until the stored result expires; null when the store keeps it. */
  expiresIn: number | null;
  contentType: string | null;
}
