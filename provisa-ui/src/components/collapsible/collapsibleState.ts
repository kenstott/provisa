// Copyright (c) 2026 Kenneth Stott
// Canary: 350241c1-8b12-4c30-9dc5-c1cbee056965
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { getStoredJson, setStored } from "../../lib/browserStorage";
/**
 * Which collapsible panels a viewer left open or closed, kept in this browser so a panel opens as
 * it was left: one key, ``{panel id: {item: open}}``. It is this viewer's convenience only. Where
 * the browser keeps nothing (a private window, storage blocked), every read finds nothing and
 * every panel takes its default.
 */

const KEY = "provisa.collapsibles";

export type CollapsibleStates = Record<string, Record<string, boolean>>;

export function readCollapsibleStates(): CollapsibleStates {
  const parsed = getStoredJson<unknown>(KEY);
  return parsed && typeof parsed === "object" ? (parsed as CollapsibleStates) : {};
}

export function writeCollapsibleState(id: string, items: Record<string, boolean>): void {
  try {
    const all = readCollapsibleStates();
    all[id] = { ...all[id], ...items };
    setStored(KEY, JSON.stringify(all));
  } catch {
    // Storage unavailable: the state holds for this page view and is not remembered.
  }
}
