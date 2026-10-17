// Copyright (c) 2026 Kenneth Stott
// Canary: b7105299-766d-46d3-b954-5dd31d17755c
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2071: the Python tab's scripts, kept as the SQL explorer keeps its queries
// (pages/sql/tabHelpers.ts): the tabs, their order and titles and each one's code in the browser's
// storage for this control plane and organization (lib/browserStorage), each one's last output in
// IndexedDB. A browser that refuses its storage keeps the scripts for the session only.
import { get as idbGet, set as idbSet, del as idbDel } from "idb-keyval";
import { getStored, removeStored, setStored } from "../lib/browserStorage";
import type { FromWorker } from "./protocol";

export type ScriptOutput = Extract<FromWorker, { type: "result" }>;

export interface Script {
  id: string;
  title: string;
  code: string;
}

export interface Scripts {
  tabs: Script[];
  activeId: string;
}

const TABS_KEY = "provisa.python.tabs";
const codeKey = (id: string) => `provisa.python.tab.${id}`;
const outputKey = (id: string) => `provisa.python.tab.${id}.output`;

export function newScriptId(): string {
  return crypto.randomUUID();
}

/** The scripts kept for this organization, or ``fallback`` when none are (or the storage is
 * refused). */
export function loadScripts(fallback: () => Scripts): Scripts {
  try {
    const meta = JSON.parse(getStored(TABS_KEY) ?? "null") as {
      tabs: { id: string; title: string }[];
      activeId: string;
    } | null;
    if (!meta || meta.tabs.length === 0) return fallback();
    const tabs = meta.tabs.map((m) => ({ ...m, code: getStored(codeKey(m.id)) ?? "" }));
    const activeId = tabs.some((s) => s.id === meta.activeId) ? meta.activeId : tabs[0].id;
    return { tabs, activeId };
  } catch {
    // Storage refused (a private window, blocked site data): the scripts live for the session.
    return fallback();
  }
}

/** Keep ``scripts``: their order, titles, the active one and each one's code. */
export function saveScripts(scripts: Scripts): void {
  try {
    setStored(
      TABS_KEY,
      JSON.stringify({
        tabs: scripts.tabs.map(({ id, title }) => ({ id, title })),
        activeId: scripts.activeId,
      }),
    );
    for (const s of scripts.tabs) setStored(codeKey(s.id), s.code);
  } catch {
    // Storage refused or full: the scripts stay in the page for the session.
  }
}

/** Forget a closed script's code and output. */
export function forgetScript(id: string): void {
  try {
    removeStored(codeKey(id));
  } catch {
    // Storage refused: nothing was kept.
  }
  void idbDel(outputKey(id)).catch(() => undefined);
}

export async function loadOutput(id: string): Promise<ScriptOutput | null> {
  try {
    return (await idbGet<ScriptOutput>(outputKey(id))) ?? null;
  } catch {
    return null; // IndexedDB refused: the output is the session's only
  }
}

export function saveOutput(id: string, output: ScriptOutput): void {
  void idbSet(outputKey(id), output).catch(() => undefined);
}

/** The next free "<base> N" title. */
export function nextScriptTitle(tabs: Script[], titleFor: (n: number) => string): string {
  const used = new Set(tabs.map((s) => s.title));
  let n = tabs.length + 1;
  while (used.has(titleFor(n))) n++;
  return titleFor(n);
}

/** ``tabs`` with the tab ``id`` moved to ``index``. */
export function moved(tabs: Script[], id: string, index: number): Script[] {
  const from = tabs.findIndex((s) => s.id === id);
  if (from < 0 || index < 0 || index >= tabs.length || from === index) return tabs;
  const next = tabs.slice();
  const [tab] = next.splice(from, 1);
  next.splice(index, 0, tab);
  return next;
}
