// Copyright (c) 2026 Kenneth Stott
// Canary: 8a1d835b-6756-425e-824f-14228fa52eaa
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// A 3D graph's view settings -- its layout, layers, filters and toggles, never the camera or the
// page's search -- remembered across reloads and sessions, per graph: a graph's kind and what it
// is of (a table, a product), so one table's choices never apply to another. Saved where the UI
// saves everything in the browser (lib/browserStorage: per control plane and org, localStorage),
// one JSON object per graph, holding only what the viewer changed.

import { useCallback, useEffect, useRef, useState } from "react";
import { getStoredJson, removeStored, setStored } from "../../../lib/browserStorage";

/** Which graph: its kind, and the table or product it shows (null for one of the whole org). */
export interface GraphKey {
  kind: string;
  subject: string | number | null;
}

/** A setting's value: a toggle, a number, a choice, a selection; null is "every choice". */
export type SettingValue = boolean | number | string | string[] | null;

const keyOf = ({ kind, subject }: GraphKey) => `graph3d.${kind}.${subject ?? "org"}`;

/** Dispatched on window when a graph's settings change, so its other open views follow. */
const CHANGED = "provisa:graph3d-settings";

function readSaved(key: string): Record<string, unknown> {
  const parsed = getStoredJson<unknown>(key);
  return parsed !== null && typeof parsed === "object" && !Array.isArray(parsed)
    ? (parsed as Record<string, unknown>)
    : {};
}

/** The org's one ontology graph, in place and expanded: one set of remembered settings. */
export const ONTOLOGY_SETTINGS: GraphKey = { kind: "ontology", subject: null };

/** ``graph``'s remembered settings as they stand, read where the graph itself is not open. */
export function savedGraphSettings(graph: GraphKey): Record<string, unknown> {
  return readSaved(keyOf(graph));
}

/** Sets ``graph``'s settings from outside it (Polly's navigate): remembered, so a graph opened
 * later has them, and followed by a graph already open. */
export function writeGraphSettings(graph: GraphKey, change: Record<string, unknown>): void {
  const key = keyOf(graph);
  const next = { ...readSaved(key), ...change };
  try {
    setStored(key, JSON.stringify(next));
  } catch {
    // Blocked storage: the setting holds while the graph is open and is not kept.
  }
  window.dispatchEvent(new CustomEvent(CHANGED, { detail: { key, settings: next } }));
}

/** A saved value kept only where it has its default's shape (a selection may also be every
 * choice, null); anything else is the default. */
function fits(value: unknown, fallback: SettingValue): boolean {
  if (Array.isArray(fallback) || fallback === null)
    return value === null || (Array.isArray(value) && value.every((v) => typeof v === "string"));
  return typeof value === typeof fallback;
}

export function restoreSettings<S extends Record<string, SettingValue>>(
  defaults: S,
  saved: Record<string, unknown>,
): S {
  const out: Record<string, SettingValue> = { ...defaults };
  for (const k of Object.keys(defaults))
    if (k in saved && fits(saved[k], defaults[k])) out[k] = saved[k] as SettingValue;
  return out as S;
}

/**
 * ``graph``'s settings, its setter and a reset. A setting is saved when it is set; reset clears
 * the graph's saved settings and puts every one back to its default.
 */
export function useGraphSettings<S extends Record<string, SettingValue>>(
  graph: GraphKey,
  defaults: S,
): [S, <K extends keyof S & string>(name: K, value: S[K]) => void, () => void] {
  const key = keyOf(graph);
  const initial = useRef(defaults);
  const [settings, setSettings] = useState<S>(() => restoreSettings(defaults, readSaved(key)));
  // The same graph open twice (in place and expanded) follows one set of settings.
  useEffect(() => {
    const follow = (e: Event) => {
      const change = (e as CustomEvent<{ key: string; settings: Record<string, unknown> }>).detail;
      if (change.key === key) setSettings(restoreSettings(initial.current, change.settings));
    };
    window.addEventListener(CHANGED, follow);
    return () => window.removeEventListener(CHANGED, follow);
  }, [key]);
  const publish = useCallback(
    (next: Record<string, unknown>) =>
      window.dispatchEvent(new CustomEvent(CHANGED, { detail: { key, settings: next } })),
    [key],
  );
  const set = useCallback(
    <K extends keyof S & string>(name: K, value: S[K]) => {
      const next = { ...readSaved(key), [name]: value };
      try {
        setStored(key, JSON.stringify(next));
      } catch {
        // Blocked storage: the setting holds while the graph is open and is not kept.
      }
      setSettings((prev) => ({ ...prev, [name]: value }));
      publish(next);
    },
    [key, publish],
  );
  const reset = useCallback(() => {
    try {
      removeStored(key);
    } catch {
      // Blocked storage held nothing to clear.
    }
    setSettings(initial.current);
    publish({});
  }, [key, publish]);
  return [settings, set, reset];
}

/** A saved selection as it applies now: every choice for null, otherwise what was chosen that
 * still exists -- a choice since removed (a domain, an edge type) is dropped, never re-added. */
export function chosenNow(saved: string[] | null, every: readonly string[]): string[] {
  return saved === null ? [...every] : saved.filter((v) => every.includes(v));
}

/** A selection as it is saved: null when it is every choice, so a choice added later is in it. */
export function selectionToSave(
  chosen: readonly string[],
  every: readonly string[],
): string[] | null {
  return every.length > 0 && every.every((v) => chosen.includes(v)) ? null : [...chosen];
}
