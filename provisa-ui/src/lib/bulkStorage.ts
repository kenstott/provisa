// Copyright (c) 2026 Kenneth Stott
// Canary: 3f6f4b0e-6f0c-4a55-9d3a-71c1a04e6a2d
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { del as idbDel, get as idbGet, getMany, keys as idbKeys, set as idbSet } from "idb-keyval";
import {
  evictionTier,
  isQuotaError,
  removeStrayEntry,
  reportStorageFailure,
  storageKey,
  strayBulkEntries,
} from "./browserStorage";

/**
 * A large list (result frames, answers, node ids) kept in IndexedDB, under the same per-scope name
 * lib/browserStorage gives a setting (REQ-2008): localStorage's ~5 MB is one budget for the origin
 * and every setting in it, and a list that fills it fails the next small write.
 *
 * Reads and writes run strictly in the order asked, after any copies left in localStorage have
 * moved across. A failure is logged and noticed (see lib/browserStorage) and the caller goes on
 * with what it holds in memory; none of these reject. An IndexedDB that never answers would fail
 * no other way, so each operation has 5 seconds: past that it is logged and noticed the same way,
 * and bulk storage is not used for the rest of the session.
 */
const OPERATION_TIMEOUT_MS = 5000;

let unavailable = false;
let tail: Promise<unknown> | null = null;

/** `operation`'s result, or `otherwise` if IndexedDB is unavailable or stops answering it. */
async function timed<T>(
  action: string,
  key: string,
  operation: () => Promise<T>,
  otherwise: T,
): Promise<T> {
  if (unavailable) return otherwise;
  const hung = Symbol("hung");
  let timer: ReturnType<typeof setTimeout> | undefined;
  const limit = new Promise<typeof hung>((resolve) => {
    timer = setTimeout(() => resolve(hung), OPERATION_TIMEOUT_MS);
  });
  try {
    const result = await Promise.race([operation(), limit]);
    if (result !== hung) return result as T;
  } finally {
    clearTimeout(timer);
  }
  unavailable = true;
  const silence = new Error(
    `IndexedDB gave no answer in ${OPERATION_TIMEOUT_MS} ms; not used for the rest of this session`,
  );
  reportStorageFailure(action, key, silence);
  return otherwise;
}

/** Move what an earlier version left in localStorage across, every scope, freeing its space. */
async function moveStrayEntries(): Promise<void> {
  for (const [entry, raw] of strayBulkEntries()) {
    const moveEntry = async () => {
      try {
        // A copy already in IndexedDB is newer than the one left behind.
        if ((await idbGet(entry)) === undefined) await idbSet(entry, JSON.parse(raw));
        removeStrayEntry(entry);
      } catch (e) {
        if (e instanceof SyntaxError) removeStrayEntry(entry); // corrupt: nothing to keep
        reportStorageFailure("move to IndexedDB", entry, e);
      }
    };
    await timed("move to IndexedDB", entry, moveEntry, undefined);
  }
}

function inOrder<T>(step: () => Promise<T>): Promise<T> {
  const run = (tail ??= moveStrayEntries()).then(step);
  tail = run.catch(() => undefined);
  return run;
}

/** Start moving copies across, ahead of the first read. */
export function startBulkMove(): Promise<void> {
  return inOrder(() => Promise.resolve());
}

/** Throw away other disposable lists, largest first, until `needed` characters are free. */
async function evictBulk(needed: number, keep: string): Promise<void> {
  const entries = (await idbKeys()).filter(
    (k): k is string => typeof k === "string" && k !== keep && evictionTier(k) !== null,
  );
  const sizes = (await getMany(entries)).map((v) => JSON.stringify(v)?.length ?? 0);
  const ranked = entries
    .map((entry, i) => ({ entry, size: sizes[i], tier: evictionTier(entry)! }))
    .sort((a, b) => a.tier - b.tier || b.size - a.size);
  let freed = 0;
  for (const { entry, size } of ranked) {
    if (freed >= needed) return;
    await idbDel(entry);
    freed += size;
    console.error(`Browser storage: evicted "${entry}" (${size} characters) to make room`);
  }
}

async function put(entry: string, value: unknown): Promise<void> {
  try {
    await idbSet(entry, value);
  } catch (e) {
    if (!isQuotaError(e)) throw e;
    await evictBulk(JSON.stringify(value)?.length ?? 0, entry);
    await idbSet(entry, value); // the one retry
  }
}

/** The list saved in the bound scope, or `null` when there is none or it cannot be read. */
export function readBulk<T>(key: string): Promise<T | null> {
  const entry = storageKey(key);
  const read = async (): Promise<T | null> => {
    try {
      return (await idbGet<T>(entry)) ?? null;
    } catch (e) {
      reportStorageFailure("read", key, e);
      return null;
    }
  };
  return inOrder(() => timed("read", key, read, null));
}

const latest = new Map<string, number>();
let writes = 0;

/** Save a list in the bound scope; of writes queued behind a slow one, only the last is made. */
export function writeBulk(key: string, value: unknown): Promise<void> {
  const entry = storageKey(key);
  const write = ++writes;
  latest.set(entry, write);
  const save = async (): Promise<void> => {
    if (latest.get(entry) !== write) return;
    try {
      await put(entry, value);
    } catch (e) {
      reportStorageFailure("save", key, e);
    }
  };
  return inOrder(() => timed("save", key, save, undefined));
}

/** Remove a list from the bound scope. */
export function removeBulk(key: string): Promise<void> {
  const entry = storageKey(key);
  latest.set(entry, ++writes); // a write queued before this is superseded
  const remove = async (): Promise<void> => {
    try {
      await idbDel(entry);
    } catch (e) {
      reportStorageFailure("remove", key, e);
    }
  };
  return inOrder(() => timed("remove", key, remove, undefined));
}
