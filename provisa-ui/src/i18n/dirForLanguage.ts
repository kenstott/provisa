// Copyright (c) 2026 Kenneth Stott
// Canary: 6d5c5e9a-3b97-464e-8e40-d91b425fe62b
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// RTL layout support: layout direction is a function of the active locale.
// Only the base subtag matters (he-IL → he). The set covers the RTL scripts
// i18next could resolve to; today only `he` ships a catalog.
//
// Kept apart from DirectionSync so that component file exports components only,
// which is what React Fast Refresh needs to swap it without a full reload.
const RTL_BASE_LNGS = new Set(["he", "ar", "fa", "ur", "yi"]);

export function dirForLanguage(lng: string): "rtl" | "ltr" {
  const base = lng.toLowerCase().split("-")[0];
  return RTL_BASE_LNGS.has(base) ? "rtl" : "ltr";
}
