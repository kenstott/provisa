// Copyright (c) 2026 Kenneth Stott
// Canary: e203b774-09b9-4f3a-a172-efc74bdcf20b
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import {
  SQL_QUERY_KEY,
  NL_PROMPT_KEY,
  TABS_KEY,
  tabSqlKey,
  tabNlKey,
} from "./types";
import type { SqlTab } from "./types";

export function newTabId(): string {
  return crypto?.randomUUID?.() ?? `tab-${Date.now()}-${Math.floor(Math.random() * 1e6)}`;
}

export function emptyTab(id: string, title: string, sqlText = "", nlText = ""): SqlTab {
  return { id, title, sqlText, nlText, resultColumns: [], resultRows: [], resultError: "", execMs: null };
}

/** Load tab metadata + per-tab sql/nl from localStorage. Results hydrate later from IndexedDB. */
export function loadTabsMeta(): { tabs: SqlTab[]; activeId: string } {
  let meta: { tabs: { id: string; title: string }[]; activeId: string } | null = null;
  try {
    meta = JSON.parse(localStorage.getItem(TABS_KEY) ?? "null");
  } catch {
    meta = null;
  }
  if (!meta || !meta.tabs?.length) {
    // Migrate legacy single-doc query/prompt into the first tab.
    const id = newTabId();
    const legacySql = localStorage.getItem(SQL_QUERY_KEY) ?? "";
    const legacyNl = localStorage.getItem(NL_PROMPT_KEY) ?? "";
    return { tabs: [emptyTab(id, "Query 1", legacySql, legacyNl)], activeId: id };
  }
  const tabs = meta.tabs.map((m) =>
    emptyTab(
      m.id,
      m.title,
      localStorage.getItem(tabSqlKey(m.id)) ?? "",
      localStorage.getItem(tabNlKey(m.id)) ?? "",
    ),
  );
  const activeId = tabs.some((t) => t.id === meta!.activeId) ? meta.activeId : tabs[0].id;
  return { tabs, activeId };
}

export function persistTabsMeta(tabs: SqlTab[], activeId: string) {
  try {
    localStorage.setItem(
      TABS_KEY,
      JSON.stringify({ tabs: tabs.map((t) => ({ id: t.id, title: t.title })), activeId }),
    );
    for (const t of tabs) {
      localStorage.setItem(tabSqlKey(t.id), t.sqlText);
      localStorage.setItem(tabNlKey(t.id), t.nlText);
    }
  } catch {
    /* quota */
  }
}

export function nextTabTitle(tabs: SqlTab[]): string {
  const used = new Set(tabs.map((t) => t.title));
  let n = tabs.length + 1;
  let title = `Query ${n}`;
  while (used.has(title)) {
    n++;
    title = `Query ${n}`;
  }
  return title;
}
