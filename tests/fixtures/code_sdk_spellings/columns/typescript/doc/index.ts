// Copyright (c) 2026 Kenneth Stott
// Canary: 1de80729-eebc-4e98-83e6-e8a27f18fb51
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

export interface Upper {
  upper: string;
}

/** Each name in capitals.
 * @columns
 */
export function shout(names: string[]): Upper[] {
  return names.map((n) => ({ upper: n.toUpperCase() }));
}
