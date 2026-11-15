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

import { useCallback, useContext, useLayoutEffect, useRef, useSyncExternalStore } from "react";
import { UNSAFE_NavigationContext, useLocation, useSearchParams } from "react-router-dom";

interface DetailView {
  /** The page's route. */
  route: string;
  /** The parameter naming the open object. */
  param: string;
  /** Where the panel has tabs: the parameter naming the one shown, and the tab shown without it. */
  tab?: { param: string; first: string };
  /** Parameters that belong to the open object, dropped with it when another opens or it closes. */
  alsoClears?: readonly string[];
  /** Another view's parameters, of an object sharing this panel: dropped when this one opens. */
  replaces?: readonly string[];
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
  // A role and a derived role share the page's list and its one panel: opening one closes the other.
  roles: {
    route: "/security/roles",
    param: "role",
    tab: { param: "section", first: "details" },
    replaces: ["derived"],
  },
  derived: { route: "/security/roles", param: "derived", replaces: ["role", "section"] },
  rls: { route: "/security/rls", param: "rule" },
  commands: { route: "/commands", param: "command", tab: { param: "section", first: "command" } },
  // REQ-2240: a domain's row and a tag's are marked on their lists, where they are changed.
  domains: { route: "/admin/domains", param: "domain" },
  tags: { route: "/admin/tags", param: "tag" },
} as const satisfies Record<string, DetailView>;

export type DetailPage = keyof typeof DETAIL_VIEWS;

// A function and a webhook may hold the same name (each table keeps its own names unique; the
// admin API checks only its own), so a webhook's id names its kind. A function's is its bare name,
// as links built before REQ-2241 name it.
export type CommandKind = "function" | "webhook";
const WEBHOOK = "webhook:";
/** The `commands` id of a command of `kind` named `name`. */
export function commandId(kind: CommandKind, name: string): string {
  return kind === "webhook" ? `${WEBHOOK}${name}` : name;
}
/** The command a `commands` id names. */
export function readCommandId(id: string): { kind: CommandKind; name: string } {
  return id.startsWith(WEBHOOK)
    ? { kind: "webhook", name: id.slice(WEBHOOK.length) }
    : { kind: "function", name: id };
}

/**
 * The `derived` id of a derived role: its attribute, and the value picked under it where one is
 * (`<attribute>:<value>`). The value is what follows the first colon, so it may hold colons.
 */
export function derivedId(attribute: string, value: string | null): string {
  return value === null ? attribute : `${attribute}:${value}`;
}
/** The derived role a `derived` id names. */
export function readDerivedId(id: string): { attribute: string; value: string | null } {
  const at = id.indexOf(":");
  return at < 0
    ? { attribute: id, value: null }
    : { attribute: id.slice(0, at), value: id.slice(at + 1) };
}

/**
 * The URL of `page` with `id`'s panel open, on `tab` where given, with `marks`: parameters that
 * belong to the open object (`alsoClears`), as a column row marked on a table's Columns tab.
 */
export function detailHref(
  page: DetailPage,
  id: string | number,
  tab?: string,
  marks?: Record<string, string>,
): string {
  const view: DetailView = DETAIL_VIEWS[page];
  const params = new URLSearchParams({ [view.param]: String(id) });
  if (tab !== undefined) {
    const tabs = panelTabs(page);
    if (tab !== tabs.first) params.set(tabs.param, tab);
  }
  for (const [name, value] of Object.entries(marks ?? {})) {
    if (!view.alsoClears?.includes(name)) throw new Error(`${page}'s panel has no mark ${name}`);
    params.set(name, value);
  }
  return `${view.route}?${params.toString()}`;
}

/**
 * The URL of a table's (or a view's) panel naming it by `<source>.<table>` instead of its id:
 * what a link that knows only the table's name (a lineage box) opens.
 */
export function tableByNameHref(page: "tables" | "views", sourceTable: string): string {
  return `${DETAIL_VIEWS[page].route}?${new URLSearchParams({ open: sourceTable }).toString()}`;
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

// The router applies a new location as a transition, after the page's own state changes made in
// the same handler (a form closing as its object opens). What was just written is shown at once,
// with them, until the location arrives. Every useDetailParam under one router shares it: a page
// with two (a role and a derived role) composes their writes, and each shows what the other wrote.
interface Pending {
  /** The location the write was made from; another location (the write's own, Back) ends it. */
  from: string;
  search: string;
}
class PendingStore {
  private pending: Pending | null = null;
  private readonly listeners = new Set<() => void>();
  readonly subscribe = (listener: () => void) => {
    this.listeners.add(listener);
    return () => {
      this.listeners.delete(listener);
    };
  };
  readonly current = () => this.pending;
  /** The search written from the location `key`, while no other has arrived; else none. */
  writtenFrom(key: string): string | null {
    return this.pending !== null && this.pending.from === key ? this.pending.search : null;
  }
  write(from: string, search: string): void {
    this.pending = { from, search };
    for (const listener of this.listeners) listener();
  }
  /** The location `key` has arrived: a write from any other is over. */
  arrived(key: string): void {
    if (this.pending !== null && this.pending.from !== key) this.pending = null;
  }
}
const stores = new WeakMap<object, PendingStore>();
function storeOf(navigator: object): PendingStore {
  let store = stores.get(navigator);
  if (store === undefined) {
    store = new PendingStore();
    stores.set(navigator, store);
  }
  return store;
}

export function useDetailParam(page: DetailPage): DetailParam {
  const view: DetailView = DETAIL_VIEWS[page];
  const location = useLocation();
  const [, setParams] = useSearchParams();
  const { navigator } = useContext(UNSAFE_NavigationContext);
  const store = storeOf(navigator);
  const pending = useSyncExternalStore(store.subscribe, store.current);
  const shown =
    pending !== null && pending.from === location.key ? pending.search : location.search;
  const params = new URLSearchParams(shown);
  const id = params.get(view.param);
  const tab = view.tab === undefined ? null : (params.get(view.tab.param) ?? view.tab.first);

  // Two writes in one handler (open, then a tab; a role closed as a derived role opens) compose:
  // the second builds on the first's search and replaces its entry, so one action is one entry.
  // Once the location has moved on, the write is the location's to say, also on coming Back to
  // the entry it was written from.
  const here = useRef({ key: location.key, search: location.search });
  useLayoutEffect(() => {
    here.current = { key: location.key, search: location.search };
    store.arrived(location.key);
  }, [location.key, location.search, store]);

  const write = useCallback(
    (change: (next: URLSearchParams) => void, replacing = false) => {
      const at = here.current;
      const written = store.writtenFrom(at.key);
      const batched = written !== null;
      const base = new URLSearchParams(written ?? at.search);
      const next = new URLSearchParams(base);
      change(next);
      const search = next.toString();
      if (search === base.toString()) return;
      store.write(at.key, search ? `?${search}` : "");
      setParams(next, { replace: replacing || batched });
    },
    [setParams, store],
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
          for (const other of view.replaces ?? []) p.delete(other);
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
