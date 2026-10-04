// Copyright (c) 2026 Kenneth Stott
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1922: the admin lists are filtered by region. One selector, the same on every list of objects
// that can name a region (sources, tables, materialized views, replica status, hot tables). This
// module is the pure filter logic behind it — selection semantics, the hidden count, and the
// persisted per-viewer choice — kept free of React/Mantine so it is unit-testable on its own.

/** The two non-region selections. A plain region name is any other selection value. */
export const REGION_ALL = "__all__";
export const REGION_NONE = "__none__";

export type RegionSelection = string; // REGION_ALL | REGION_NONE | <region name>

/** The default selection: the connected region (objects homed there plus objects with no region),
 * or all regions when the deployment declares regions but names no connected one. */
export function defaultSelection(connected: string | null): RegionSelection {
  return connected ?? REGION_ALL;
}

/** Whether ``region`` (an object's home region, null = none) passes ``selection``.
 *
 * - all regions: everything.
 * - no region: only objects that name no region.
 * - the connected region: objects homed there AND objects with no region — both are built/served there.
 * - any other region by name: only objects homed in that region.
 */
export function regionMatches(
  selection: RegionSelection,
  connected: string | null,
  region: string | null,
): boolean {
  if (selection === REGION_ALL) return true;
  if (selection === REGION_NONE) return region == null;
  if (selection === connected) return region === selection || region == null;
  return region === selection;
}

export interface RegionFiltered<T> {
  visible: T[];
  hidden: number;
}

/** Partition ``items`` by ``selection``; ``hidden`` is how many the selection removes, so a list can
 * say nothing is silently missing. ``getRegion`` reads an item's home region (null = none). */
export function filterByRegion<T>(
  items: readonly T[],
  selection: RegionSelection,
  connected: string | null,
  getRegion: (item: T) => string | null,
): RegionFiltered<T> {
  const visible = items.filter((it) => regionMatches(selection, connected, getRegion(it)));
  return { visible, hidden: items.length - visible.length };
}

/** The valid selection values for ``regions``/``connected``, in display order: the connected region
 * first (the default), then all regions, no region, then every other region by name. Used both to
 * build the selector and to validate a persisted choice that may name a region since removed. */
export function selectionValues(regions: readonly string[], connected: string | null): string[] {
  const values: string[] = [];
  if (connected && regions.includes(connected)) values.push(connected);
  values.push(REGION_ALL, REGION_NONE);
  for (const r of regions) {
    if (r !== connected) values.push(r);
  }
  return values;
}

/** A persisted selection, clamped to what is currently valid: a stored region that no longer exists
 * (or a stored value from when no region was connected) falls back to the default. */
export function coerceSelection(
  stored: string | null | undefined,
  regions: readonly string[],
  connected: string | null,
): RegionSelection {
  if (stored && selectionValues(regions, connected).includes(stored)) return stored;
  return defaultSelection(connected);
}
