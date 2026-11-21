// Copyright (c) 2026 Kenneth Stott
// Canary: 56f726b2-b1e5-4707-be66-b809346fdac4
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

export interface Row {
  id: number;
  name: string;
}

/** Rows from the API.
 * @table
 * @requires API_KEY:secret
 * @requires REGION
 */
export function rows(): Row[] {
  return [];
}
