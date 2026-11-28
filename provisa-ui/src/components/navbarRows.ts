// Copyright (c) 2026 Kenneth Stott
// Canary: 224b8054-735a-4ea7-9f7a-b2e313d198dc
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2277 (amended 2026-11-27): the bar is one row where that row fits whole, measured live —
// the labels' length differs by locale and the data-address picker shows only on a
// coordinator-served UI, so no fixed width can say where the row stops fitting. Otherwise it is two
// rows: logo, org, pickers and icon actions first, the page links beneath. Back to one row only
// once the row fits with this much to spare, so a window at the edge does not flip on every pixel.
export const NAVBAR_ROWS_HYSTERESIS_PX = 24;

/** Whether the bar is two rows, given the single row's natural width and the width it has. */
export function nextNavbarRows(rows: boolean, required: number, available: number): boolean {
  return rows ? required + NAVBAR_ROWS_HYSTERESIS_PX > available : required > available;
}

/** The single row's natural width (every child at its natural size, the org's name at its least)
 *  and the width the bar has. `navbar-measure` lays the row out at max-content for the one read and
 *  comes off again in the same task, so nothing is painted in between. */
export function measureSingleRow(nav: HTMLElement): { required: number; available: number } {
  const available = nav.getBoundingClientRect().width;
  const rows = nav.classList.contains("navbar-rows");
  nav.classList.remove("navbar-rows");
  nav.classList.add("navbar-measure");
  const required = nav.getBoundingClientRect().width;
  nav.classList.remove("navbar-measure");
  if (rows) nav.classList.add("navbar-rows");
  return { required, available };
}
