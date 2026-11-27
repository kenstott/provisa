// Copyright (c) 2026 Kenneth Stott
// Canary: 5a9c1e37-6f2b-4d80-b3e4-c71d8a2f09b6
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import type { TFunction } from "i18next";
import type { ContextPlace } from "../api/contextViews";

/** A place as ContextPlaces names it: the table, or the table and its column. */
export const placeName = (p: ContextPlace) =>
  p.column === null ? p.tableRef : `${p.tableRef}.${p.column}`;

/** Every text ContextPlaces shows for ``places``, which a list's filter searches. */
export function contextPlaceTexts(places: ContextPlace[], hidden: number, t: TFunction): string[] {
  if (places.length === 0 && hidden === 0) return [t("contextViews.nowhere")];
  return [
    ...places.map(placeName),
    hidden > 0 ? t("contextViews.hiddenPlaces", { count: hidden }) : "",
  ];
}
