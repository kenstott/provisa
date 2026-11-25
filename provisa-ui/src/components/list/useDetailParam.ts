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

import {
  useCallback,
  useContext,
  useEffect,
  useLayoutEffect,
  useRef,
  useState,
  useSyncExternalStore,
} from "react";
import { UNSAFE_NavigationContext, useLocation, useSearchParams } from "react-router-dom";
import { getStored, removeStored, setStored, storedKeys, viewer } from "../../lib/browserStorage";

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
  /** Parameters that name the object another way: a link carrying one names an item (REQ-2260). */
  alsoNames?: readonly string[];
}

// Existing names are kept, so links built before REQ-2241 open the same panels.
const TABLE_VIEW = {
  param: "table",
  tab: { param: "section", first: "identity" },
  // `column` marks a column row, `open` names a table by source.table, `tab` is a Columns tab;
  // REQ-2277: `profile` and `profileView` are the table's profile runs dialog and its result.
  alsoClears: ["column", "open", "tab", "profile", "profileView"],
  alsoNames: ["open"],
} as const;

export const DETAIL_VIEWS = {
  sources: { route: "/sources", param: "expanded", tab: { param: "section", first: "connection" } },
  tables: { route: "/tables", ...TABLE_VIEW },
  views: { route: "/views", ...TABLE_VIEW },
  relationships: { route: "/relationships", param: "relationship" },
  ontology: { route: "/context/ontology", param: "term" },
  library: { route: "/context/documents", param: "document" },
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

/**
 * The item `search` names on `page`'s panel: its parameter, or one naming it another way
 * (`alsoNames`); null when it names none.
 */
export function namedIn(page: DetailPage, search: string): string | null {
  const view: DetailView = DETAIL_VIEWS[page];
  const params = new URLSearchParams(search);
  for (const name of [view.param, ...(view.alsoNames ?? [])]) {
    const value = params.get(name);
    if (value !== null) return value;
  }
  return null;
}

// REQ-2260: each list page remembers the item whose detail was open, per viewer in this browser
// (lib/browserStorage keeps it per control plane and org; the key names the viewer). Coming back to
// the page by any route, with no item named in the address, reopens it: the address is given its
// parameter (replacing the entry, so Back is unchanged) and the item's row is scrolled into view.
// A link naming an item opens that one, and it is what is remembered from then. Closing the detail
// on the page forgets it. A remembered item the page's loaded list does not hold (deleted, or no
// longer readable by this viewer) is not opened, and is forgotten.

const MEMORY = "detail.";

function memoryKey(page: DetailPage): string {
  return `${MEMORY}${viewer()}.${page}`;
}

/** The item `page` remembers having open for the viewer, if any. */
export function rememberedDetail(page: DetailPage): string | null {
  return getStored(memoryKey(page));
}

/** Forget what `page` remembers, for every viewer in this browser's scope (a tour's fresh start). */
export function forgetDetailForEveryone(page: DetailPage): void {
  const suffix = `.${page}`;
  for (const key of storedKeys()) {
    if (key.startsWith(MEMORY) && key.endsWith(suffix)) removeStored(key);
  }
}

/** The open item's row (ListRow marks it `row-selected`), brought into view once it is drawn. */
function scrollToOpenRow(): boolean {
  const row = document.querySelector("tr.list-row.row-selected");
  if (row === null) return false;
  row.scrollIntoView({ block: "center" });
  return true;
}

export interface RememberedDetailParam extends DetailParam {
  /**
   * Closes the panel; with `keep`, the item is only set aside (a page showing something else in its
   * place, to open it again) and stays remembered.
   */
  close: (how?: { only?: string; replace?: boolean; keep?: true }) => void;
}

/**
 * `useDetailParam` for a list page, remembering the item open (REQ-2260). `listed` is the keys, as
 * the address names them, of every item the page's loaded list holds; `null` while it loads (or
 * while the page shows something else in the detail's place, so nothing is reopened meanwhile).
 */
export function useRememberedDetail(
  page: DetailPage,
  listed: readonly string[] | null,
): RememberedDetailParam {
  const detail = useDetailParam(page);
  const { search } = useLocation();
  const named = detail.id !== null || namedIn(page, search) !== null;
  const key = memoryKey(page);

  // The page arriving (mounted, or turned into another page or viewer's) with no item named looks
  // up the item it last had open, to reopen once the list is loaded.
  const [restoring, setRestoring] = useState<{ key: string; id: string } | null>(null);
  const [scrolling, setScrolling] = useState(false);
  const arrived = useRef<string | null>(null);
  const was = useRef<{ key: string; id: string | null } | null>(null);
  const keeping = useRef(false);
  const { id, open, close: closeDetail } = detail;
  const close = useCallback(
    (how?: { only?: string; replace?: boolean; keep?: true }) => {
      keeping.current = how?.keep === true && id !== null;
      closeDetail({ only: how?.only, replace: how?.replace });
    },
    [closeDetail, id],
  );

  useLayoutEffect(() => {
    if (key === arrived.current) return;
    arrived.current = key;
    if (named) return;
    const remembered = getStored(key);
    // eslint-disable-next-line react-hooks/set-state-in-effect -- read as the page arrives, once
    if (remembered !== null) setRestoring({ key, id: remembered });
  }, [key, named]);

  // A named item is remembered; the open item closed on the page is forgotten. Leaving the page is
  // not closing it, and nothing is forgotten while the remembered item waits for the list.
  useEffect(() => {
    const before = was.current;
    was.current = { key, id };
    const kept = keeping.current;
    keeping.current = false;
    if (id !== null) {
      setStored(key, id);
      return;
    }
    if (before !== null && before.key === key && before.id !== null && !kept) removeStored(key);
  }, [key, id]);

  useEffect(() => {
    if (restoring === null) return;
    if (restoring.key !== key || named) {
      // The page moved on (another route, or an item named) before the list arrived.
      setRestoring(null); // eslint-disable-line react-hooks/set-state-in-effect -- see above
      return;
    }
    if (listed === null) return;
    setRestoring(null);
    if (listed.includes(restoring.id)) {
      open(restoring.id, { replace: true });
      setScrolling(true);
    } else {
      removeStored(key);
    }
  }, [restoring, key, named, listed, open]);

  // The row is drawn after the detail opens: looked for as the detail and the list change until it
  // is there; the detail closed in the meantime ends the search.
  useEffect(() => {
    if (!scrolling) return;
    // eslint-disable-next-line react-hooks/set-state-in-effect -- stops once the row is in view
    if (id === null || scrollToOpenRow()) setScrolling(false);
  }, [scrolling, id, listed]);

  return { ...detail, close };
}
