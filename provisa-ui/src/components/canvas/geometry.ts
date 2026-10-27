// Copyright (c) 2026 Kenneth Stott
// Canary: 9f49661b-9937-4db3-be99-8d6b92dc40b1
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// A join canvas card's geometry: where its rows are, for the joins drawn between them.

export const CARD_W = 200;
export const CARD_HEADER_H = 34;
export const COL_ROW_H = 27;

/** Where a card's first column row starts, below its header -- and, when its columns can be
 *  selected, below the row that selects them all. */
export function columnsTop(selectable: boolean): number {
  return CARD_HEADER_H + (selectable ? COL_ROW_H : 0);
}
