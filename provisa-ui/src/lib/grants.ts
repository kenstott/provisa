// Copyright (c) 2026 Kenneth Stott
// Canary: 2aa616c0-6114-4b2a-8b02-dbf02a239f1a
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// A grant list entry names one role, or several joined by "+" that a reader must hold ALL of
// (a composite grant). Across a list's entries a reader needs any one.

import type { TFunction } from "i18next";

/** What joins the roles of a composite grant entry. */
export const COMPOSITE = "+";

export function grantParts(entry: string): string[] {
  return entry.split(COMPOSITE);
}

/** The entry for holding every one of ``parts``. */
export function compositeEntry(parts: string[]): string {
  return parts.join(COMPOSITE);
}

/** An entry as a reader reads it: "analyst and region__eu". */
export function grantLabel(entry: string, t: TFunction): string {
  return grantParts(entry).join(` ${t("grantPicker.and")} `);
}

/** A grant list as a reader reads it: its entries, comma-separated. */
export function grantsLabel(entries: string[], t: TFunction): string {
  return entries.map((e) => grantLabel(e, t)).join(", ");
}
