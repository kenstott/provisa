// Copyright (c) 2026 Kenneth Stott
// Canary: 688d7101-27e1-422e-b038-c2e0768ed837
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2241: every detail view has a deep link. A list page's open panel is its URL: one parameter
// names the object, a second the panel's tab. Opening a row and choosing a tab each push a history
// entry; closing the panel removes both. The page reads what is open from here, so loading the URL,
// following a link, Back and Forward all show the same panel on the same tab.
//
// DETAIL_VIEWS is the one place each page's parameter is named; detailHref builds a link into a
// panel from it, so every link to a detail view is the URL the page itself writes.

import { useCallback, useLayoutEffect, useRef, useState } from "react";
import { useLocation, useSearchParams } from "react-router-dom";

interface DetailView {
  /** The page's route. */
  route: string;
  /** The parameter naming the open object. */
  param: string;
  /** Where the panel has tabs: the parameter naming the one shown, and the tab shown without it. */
  tab?: { param: string; first: string };
  /** Parameters that belong to the open object, dropped with it when another opens or it closes. */
  alsoClears?: readonly string[];
}

// Existing names are kept, so links built before REQ-2241 open the same panels.
const TABLE_VIEW = {
  param: "table",
  tab: { param: "section", first: "identity" },
  // `column` marks a column row, `open` names a table by source.table, `tab` is a Columns tab.
  alsoClears: ["column", "open", "tab"],
} as const;

export const DETAIL_VIEWS = {
  sources: { route: "/sources", param: "expanded", tab: { param: "section", first: "connection" } },
  tables: { route: "/tables", ...TABLE_VIEW },
  views: { route: "/views", ...TABLE_VIEW },
  relationships: { route: "/relationships", param: "relationship" },
  ontology: { route: "/admin/ontology", param: "term" },
  library: { route: "/library", param: "document" },
  metrics: { route: "/metrics", param: "metric" },
  // REQ-2158: a linked contract is the linked product's.
  products: { route: "/data-products", param: "product", alsoClears: ["contract"] },
  roles: { route: "/security/roles", param: "role", tab: { param: "section", first: "details" } },
  rls: { route: "/security/rls", param: "rule" },
  commands: { route: "/commands", param: "command", tab: { param: "section", first: "command" } },
} as const satisfies Record<string, DetailView>;

export type DetailPage = keyof typeof DETAIL_VIEWS;

/** The URL of `page` with `id`'s panel open, on `tab` where given. */
export function detailHref(page: DetailPage, id: string | number, tab?: string): string {
  const view: DetailView = DETAIL_VIEWS[page];
  const params = new URLSearchParams({ [view.param]: String(id) });
  if (tab !== undefined) {
    const tabs = panelTabs(page);
    if (tab !== tabs.first) params.set(tabs.param, tab);
  }
  return `${view.route}?${params.toString()}`;
}

function panelTabs(page: DetailPage): { param: string; first: string } {
  const tabs = (DETAIL_VIEWS[page] as DetailView).tab;
  if (tabs === undefined) throw new Error(`${page}'s detail panel has no tabs`);
  return tabs;
}

export interface DetailParam {
  /** The open object's identity as the URL names it; null while no panel is open. */
  id: string | null;
  /** The open panel's tab: the one the URL names, else the first; null for a panel without tabs. */
  tab: string | null;
  /**
   * Opens `id`'s panel, on `tab` where given: a new history entry, or with `replace` none (a page
   * reopening what it was left showing).
   */
  open: (id: string, how?: { tab?: string; replace?: boolean }) => void;
  /** Shows `tab` in the open panel: a new history entry. */
  setTab: (tab: string) => void;
  /**
   * Closes the panel, removing its parameters: a new history entry, so Back reopens it, or with
   * `replace` none. With `only`, closes it only while it shows that object (one just deleted).
   */
  close: (how?: { only?: string; replace?: boolean }) => void;
}

export function useDetailParam(page: DetailPage): DetailParam {
  const view: DetailView = DETAIL_VIEWS[page];
  const location = useLocation();
  const [, setParams] = useSearchParams();

  // The router applies a new location as a transition, after the page's own state changes made in
  // the same handler (a form closing as its object opens). What was just written is shown at once,
  // with them, until the location arrives; a location of any other making (Back) replaces it.
  const [written, setWritten] = useState<{ from: string; search: string } | null>(null);
  // Once the location has moved on, what was written is the location's to say, also on coming
  // Back to the entry it was written from.
  if (written !== null && written.from !== location.key) setWritten(null);
  const shown =
    written !== null && written.from === location.key ? written.search : location.search;
  const params = new URLSearchParams(shown);
  const id = params.get(view.param);
  const tab = view.tab === undefined ? null : (params.get(view.tab.param) ?? view.tab.first);

  // Two writes in one handler (open, then a tab) compose: the second builds on the first's search
  // and replaces its entry, so one action is one entry. A render of a new location ends the batch.
  const latest = useRef({ from: location.key, search: location.search, wrote: false });
  useLayoutEffect(() => {
    latest.current = { from: location.key, search: location.search, wrote: false };
  }, [location.key, location.search]);

  const write = useCallback(
    (change: (next: URLSearchParams) => void, replacing = false) => {
      const held = latest.current;
      const next = new URLSearchParams(held.search);
      change(next);
      const search = next.toString();
      if (search === new URLSearchParams(held.search).toString()) return;
      const replace = replacing || held.wrote;
      latest.current = { ...held, search: search ? `?${search}` : "", wrote: true };
      setWritten({ from: held.from, search });
      setParams(next, { replace });
    },
    [setParams],
  );

  // The first tab is the panel without a tab named: one URL for it, and choosing it again is no change.
  const showTab = useCallback(
    (p: URLSearchParams, next: string) => {
      const tabs = panelTabs(page);
      if (next === tabs.first) p.delete(tabs.param);
      else p.set(tabs.param, next);
    },
    [page],
  );
  const open = useCallback(
    (next: string, how?: { tab?: string; replace?: boolean }) =>
      write((p) => {
        // Another object keeps the tab in view, as a row clicked beside an open panel always has;
        // what belonged to the one before (a marked column, a linked contract) goes with it.
        if (p.get(view.param) !== next) {
          for (const linked of view.alsoClears ?? []) p.delete(linked);
          p.set(view.param, next);
        }
        if (how?.tab !== undefined) showTab(p, how.tab);
      }, how?.replace),
    [write, view, showTab],
  );
  const setTab = useCallback((next: string) => write((p) => showTab(p, next)), [write, showTab]);
  const close = useCallback(
    (how?: { only?: string; replace?: boolean }) =>
      write((p) => {
        if (how?.only !== undefined && p.get(view.param) !== how.only) return;
        p.delete(view.param);
        if (view.tab !== undefined) p.delete(view.tab.param);
        for (const linked of view.alsoClears ?? []) p.delete(linked);
      }, how?.replace),
    [write, view],
  );
  return { id, tab, open, setTab, close };
}
