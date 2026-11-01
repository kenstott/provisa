// Copyright (c) 2026 Kenneth Stott
// Canary: 898e654f-bd66-46b5-8afd-fd7411e0e12b
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

/**
 * REQ-2008: everything the UI saves in the browser is saved per control plane and organization,
 * under `<control plane id>:<org id>:<key>`, so each demo, instance and org keeps its own state:
 * switching to another carries nothing across, and coming back finds what was left there. The
 * control plane's id comes from `/auth/me`; AuthContext's scope gate binds it with the acting org
 * before any page mounts, and remounts the pages when either changes.
 *
 * A key saved for the browser as a whole says so with `{ global: true }`: the session's identity
 * (token, org, role, environment), which rides on the requests that establish the scope, and the
 * tour's offer, seen and declined marks.
 *
 * Every read and write of `localStorage` and `sessionStorage` goes through here
 * (src/__tests__/browserStorage.guard.test.ts).
 *
 * The maintainer's rule: the browser's storage can never crash the app or stop it starting. Quota,
 * corrupt JSON and blocked or throwing storage all land here. This is the documented exception to
 * no silent handling, and it is not silent: the app goes on with defaults, the failure is logged
 * with console.error, and a small notice is shown once (StorageNotice). The session's identity is
 * the one thing that does not go on: a failed write or removal of it is a SessionStorageError.
 *
 * Only small settings stay here; a large list is lib/bulkStorage's, in IndexedDB.
 */

export interface StorageScope {
  controlPlaneId: string;
  /** `null` when acting in no org (a platform administrator before any org exists): the state
   * is then the control plane's own, saved under `<control plane id>::<key>`. */
  orgId: string | null;
}

export interface StoredKeyOptions {
  /** Saved for the browser as a whole, not per control plane and org. */
  global?: true;
  /** `sessionStorage` (this tab, until it closes) rather than `localStorage`. */
  session?: true;
}

/** What a notice says: settings were not saved, or the session itself could not be kept. */
export type StorageNoticeKind = "settings" | "session";

/** The browser would not keep the session's identity: this is a session failure, never a success. */
export class SessionStorageError extends Error {
  constructor(key: string, cause: unknown) {
    super(`The browser refused to keep the session's "${key}"`, { cause });
    this.name = "SessionStorageError";
  }
}

const noticed = new Set<StorageNoticeKind>();
const undelivered = new Set<StorageNoticeKind>();
let noticeListener: ((kind: StorageNoticeKind) => void) | null = null;

function deliverNotices(): void {
  if (noticeListener === null) return;
  for (const kind of undelivered) noticeListener(kind);
  undelivered.clear();
}

/** Hear of each kind of notice once; one raised before anything listened arrives now. */
export function listenForStorageNotice(listener: (kind: StorageNoticeKind) => void): () => void {
  noticeListener = listener;
  deliverNotices();
  return () => {
    noticeListener = null;
  };
}

/** Log a storage failure, and raise its notice the first time. Every failure is logged. */
export function reportStorageFailure(
  action: string,
  key: string,
  error: unknown,
  kind: StorageNoticeKind = "settings",
): void {
  console.error(`Browser storage: could not ${action} "${key}":`, error);
  if (noticed.has(kind)) return;
  noticed.add(kind);
  undelivered.add(kind);
  deliverNotices();
}

let bound: StorageScope | null = null;

/** When each control plane was last bound in this browser: what "oldest" means to `evict`. */
const CONTROL_PLANES_USED = "provisa_control_planes_used";
type ControlPlanesUsed = Record<string, number>;

function controlPlanesUsed(): ControlPlanesUsed {
  return getStoredJson<ControlPlanesUsed>(CONTROL_PLANES_USED, { global: true }) ?? {};
}

/** The scope the pages now save under; `null` unbinds it (no page is mounted then). */
export function bindStorageScope(scope: StorageScope | null): void {
  bound = scope;
  if (scope === null) return;
  const used = { ...controlPlanesUsed(), [scope.controlPlaneId]: Date.now() };
  setStored(CONTROL_PLANES_USED, JSON.stringify(used), { global: true });
}

export function storageScope(): StorageScope | null {
  return bound;
}

/** The key as the browser holds it. */
export function storageKey(key: string, options?: StoredKeyOptions): string {
  if (options?.global) return key;
  if (bound === null) {
    // The scope gate binds the scope before any page that saves state mounts; a read or write
    // before it is a defect, never a read of "nothing saved".
    throw new Error(
      `"${key}" is saved per control plane and organization, and neither is known yet`,
    );
  }
  return `${bound.controlPlaneId}:${bound.orgId ?? ""}:${key}`;
}

function area(options?: StoredKeyOptions): Storage {
  return options?.session ? window.sessionStorage : window.localStorage;
}

/**
 * REQ-2006: a live demo runs as its own anonymous visitor (the token opening its sandbox gave),
 * never as a Provisa login the browser also holds. While a tab is a visitor's, the session's
 * identity -- its bearer, org and role -- is the visitor's own: read and written in that tab's
 * session storage, under its own names, so the signed-in session's keys are neither read nor
 * changed. Closing the tab, or signing out of the demo, ends the visit and the browser's own
 * session is as it was. The environment picked is the visit's own the same way: one a tour left
 * picked is not the next visit's, which opens as its demo does.
 */
const VISITOR_FLAG = "provisa_visitor";
const IDENTITY_KEYS: ReadonlySet<string> = new Set([
  "provisa_token",
  "provisa_su_token",
  "provisa_org",
  "provisa_role",
]);

/** What a visit holds of its own: its identity and the environment picked. */
const VISIT_KEYS: ReadonlySet<string> = new Set([...IDENTITY_KEYS, "provisa_env"]);

function readVisitorFlag(): boolean {
  return window.sessionStorage.getItem(VISITOR_FLAG) === "true";
}

/** Whether this tab is a live demo's visitor. Storage that cannot say starts it as no visitor. */
export function visitorActive(): boolean {
  try {
    return readVisitorFlag();
  } catch (e) {
    reportStorageFailure("read", VISITOR_FLAG, e);
    return false;
  }
}

/** Run a change to the visit; it is the session's identity, so a failure is the session's. */
function changeVisit(change: (store: Storage) => void): void {
  try {
    change(window.sessionStorage);
  } catch (e) {
    reportStorageFailure("save", VISITOR_FLAG, e, "session");
    throw new SessionStorageError(VISITOR_FLAG, e);
  }
}

/** Make this tab a live demo's visitor, with no identity of its own yet. */
export function startVisit(): void {
  changeVisit((store) => {
    for (const key of VISIT_KEYS) store.removeItem(`visitor:${key}`);
    store.setItem(VISITOR_FLAG, "true");
  });
}

/** End this tab's visit: the visitor's identity goes, and the browser's own session is back. */
export function endVisit(): void {
  changeVisit((store) => {
    for (const key of VISIT_KEYS) store.removeItem(`visitor:${key}`);
    store.removeItem(VISITOR_FLAG);
  });
}

/** Whether `key` is the session's identity, wherever this tab holds it. */
function isIdentity(key: string, options?: StoredKeyOptions): boolean {
  return Boolean(options?.global && !options.session && IDENTITY_KEYS.has(key));
}

/** The storage and key a value is held under: the visitor's own, for its identity. */
function where(key: string, options?: StoredKeyOptions): [Storage, string] {
  if (options?.global && !options.session && VISIT_KEYS.has(key) && readVisitorFlag()) {
    return [window.sessionStorage, `visitor:${key}`];
  }
  return [area(options), storageKey(key, options)];
}

/** The large lists lib/bulkStorage keeps in IndexedDB: a copy in `localStorage` is an old one. */
export const BULK_KEYS = [
  "provisa.graph.state",
  "provisa.sparql.state",
  "nl-branches",
  "nl-branches-backup",
  "provisa_stable_node_ids",
] as const;

/**
 * What is thrown away to make room, in this order, largest first within a tier: a scoped key's own
 * name (or, ending `.`, the start of it), in any scope. Listed, because everything else stays: the
 * session's identity is global and never matches, and a scoped key not named here is something the
 * user wrote or set.
 */
const EVICTION_TIERS: readonly (readonly string[])[] = [
  ["provisa.graph.state", "provisa.sparql.state"], // old result frames
  BULK_KEYS,
  ["provisa.lineage.graph", "mcp.chat."], // results in sessionStorage, re-run on demand
  ["provisa.graph.counts."], // read again from the server
  ["sql_modeling_history", "mcp.hist."],
];

/** The key a scoped entry was saved under: `null` for an entry not shaped `<cp>:<org>:<key>`. */
function scopedName(entry: string): string | null {
  const parts = entry.split(":");
  return parts.length < 3 ? null : parts.slice(2).join(":");
}

/** Whether `entry` is shaped as a scoped key by a library's own key, never one of ours. */
function isForeign(entry: string): boolean {
  return entry.startsWith("firebase:");
}

/** The tier a stored entry is evicted in, or `null` when it is never evicted. */
export function evictionTier(entry: string): number | null {
  const name = scopedName(entry);
  if (name === null || isForeign(entry)) return null;
  const tier = EVICTION_TIERS.findIndex((names) =>
    names.some((n) => (n.endsWith(".") ? name.startsWith(n) : name === n)),
  );
  return tier < 0 ? null : tier;
}

export function isQuotaError(e: unknown): boolean {
  const name = (e as { name?: string } | null)?.name;
  return name === "QuotaExceededError" || name === "NS_ERROR_DOM_QUOTA_REACHED";
}

/**
 * Save a value `store` refused, throwing away one entry at a time until it fits: the disposable
 * ones, then what other control planes left behind (an old demo's settings), the one used longest
 * ago first. The bound control plane's settings stay, and with none bound any control plane may
 * be the current one, so all of theirs do. Throws the last refusal when nothing more can go.
 */
function saveByEviction(store: Storage, keep: string, value: string): void {
  const disposable: { at: string; tier: number; size: number }[] = [];
  const left: { at: string; controlPlane: string; size: number }[] = [];
  for (let i = 0; i < store.length; i++) {
    const at = store.key(i);
    if (at === null || at === keep) continue;
    const tier = evictionTier(at);
    const size = store.getItem(at)?.length ?? 0;
    if (tier !== null) disposable.push({ at, tier, size });
    else if (bound !== null && scopedName(at) !== null && !isForeign(at)) {
      const controlPlane = at.slice(0, at.indexOf(":"));
      if (controlPlane !== bound.controlPlaneId) left.push({ at, controlPlane, size });
    }
  }
  disposable.sort((a, b) => a.tier - b.tier || b.size - a.size);
  const used = controlPlanesUsed();
  // A control plane with no mark was last used before the marks began: it is the oldest.
  const usedAt = (controlPlane: string) => used[controlPlane] ?? 0;
  left.sort(
    (a, b) =>
      usedAt(a.controlPlane) - usedAt(b.controlPlane) ||
      a.controlPlane.localeCompare(b.controlPlane) ||
      b.size - a.size,
  );
  for (const { at, size } of [...disposable, ...left]) {
    store.removeItem(at);
    console.error(`Browser storage: evicted "${at}" (${size} characters) to make room`);
    try {
      store.setItem(keep, value);
      return;
    } catch (e) {
      if (!isQuotaError(e)) throw e;
    }
  }
  store.setItem(keep, value);
}

/** The value as saved, or `null` when there is none or it is not JSON: the caller's defaults. */
export function getStoredJson<T>(key: string, options?: StoredKeyOptions): T | null {
  const raw = getStored(key, options);
  if (raw === null) return null;
  try {
    return JSON.parse(raw) as T;
  } catch (e) {
    reportStorageFailure("parse", key, e);
    return null;
  }
}

export function getStored(key: string, options?: StoredKeyOptions): string | null {
  storageKey(key, options); // a read before the scope is bound is a defect, not a storage failure
  try {
    const [store, at] = where(key, options);
    return store.getItem(at);
  } catch (e) {
    reportStorageFailure("read", key, e);
    return null;
  }
}

/** Save `value`. A failure is logged and noticed, and only an identity key's is thrown. */
export function setStored(key: string, value: string, options?: StoredKeyOptions): void {
  storageKey(key, options); // as in getStored
  try {
    const [store, at] = where(key, options);
    try {
      store.setItem(at, value);
    } catch (e) {
      if (!isQuotaError(e)) throw e;
      saveByEviction(store, at, value);
    }
  } catch (e) {
    if (isIdentity(key, options)) {
      reportStorageFailure("save", key, e, "session");
      throw new SessionStorageError(key, e);
    }
    reportStorageFailure("save", key, e);
  }
}

/** Remove `key`. As with `setStored`, only an identity key's failure is thrown. */
export function removeStored(key: string, options?: StoredKeyOptions): void {
  storageKey(key, options); // as in getStored
  try {
    const [store, at] = where(key, options);
    store.removeItem(at);
  } catch (e) {
    if (isIdentity(key, options)) {
      reportStorageFailure("remove", key, e, "session");
      throw new SessionStorageError(key, e);
    }
    reportStorageFailure("remove", key, e);
  }
}

/** The keys saved in the bound scope (as passed to `getStored`), in the area named. */
export function storedKeys(options?: { session?: true }): string[] {
  const prefix = storageKey("", options);
  const keys: string[] = [];
  try {
    const store = area(options);
    for (let i = 0; i < store.length; i++) {
      const k = store.key(i);
      if (k !== null && k.startsWith(prefix)) keys.push(k.slice(prefix.length));
    }
  } catch (e) {
    reportStorageFailure("list", prefix, e);
  }
  return keys;
}

/** Remove `key` from every control plane and org it was saved under in this browser: state that
 * must not outlive the session that saved it (REQ-1297, REQ-1349), whichever scope it was in. */
export function removeStoredEverywhere(key: string): void {
  const suffix = `:${key}`;
  try {
    const store = window.localStorage;
    const doomed: string[] = [];
    for (let i = 0; i < store.length; i++) {
      const k = store.key(i);
      // A scoped key is `<control plane>:<org>:<key>`: two separators before the key itself.
      if (k !== null && k.endsWith(suffix) && k.slice(0, -suffix.length).split(":").length === 2) {
        doomed.push(k);
      }
    }
    for (const k of doomed) store.removeItem(k);
  } catch (e) {
    reportStorageFailure("remove", key, e);
  }
}

/** Every saved copy of a bulk key still in `localStorage`, any scope: `[entry, value]`. */
export function strayBulkEntries(): [string, string][] {
  const found: [string, string][] = [];
  try {
    const store = window.localStorage;
    for (let i = 0; i < store.length; i++) {
      const at = store.key(i);
      const name = at === null ? null : scopedName(at);
      if (at === null || name === null || !(BULK_KEYS as readonly string[]).includes(name)) {
        continue;
      }
      const value = store.getItem(at);
      if (value !== null) found.push([at, value]);
    }
  } catch (e) {
    reportStorageFailure("list", "bulk keys", e);
  }
  return found;
}

/** Drop a copy `strayBulkEntries` found, now held in IndexedDB. */
export function removeStrayEntry(entry: string): void {
  try {
    window.localStorage.removeItem(entry);
  } catch (e) {
    reportStorageFailure("remove", entry, e);
  }
}

/** A `Storage`-shaped view of the bound scope's `localStorage`, for a library that saves its own
 * state through a storage it is handed (GraphiQL's tabs, query history and headers): it saves in
 * the bound scope like everything else. `clear` and `length` cover the keys starting `prefix`,
 * the library's own namespace. */
export function scopedStorage(prefix: string): {
  getItem(key: string): string | null;
  setItem(key: string, value: string): void;
  removeItem(key: string): void;
  clear(): void;
  readonly length: number;
} {
  const own = () => storedKeys().filter((k) => k.startsWith(prefix));
  return {
    getItem: (key) => getStored(key),
    setItem: (key, value) => setStored(key, value),
    removeItem: (key) => removeStored(key),
    clear: () => {
      for (const k of own()) removeStored(k);
    },
    get length() {
      return own().length;
    },
  };
}
