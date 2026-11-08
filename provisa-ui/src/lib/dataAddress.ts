// Copyright (c) 2026 Kenneth Stott
// Canary: 9d3f6a20-7c18-4e5b-b2a4-e81c0f5d7a93
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1916 (amended 2026-11-02): where the UI's data calls go. A coordinator serves the UI but no
// data requests, so a coordinator-served UI sends every data call (every request under /data) to
// the data node the user picks here: one of the addresses the coordinator offers for its region,
// or one they type. An every node serves its own UI's data calls: it offers no choice, and the
// calls stay on the UI's own origin. The choice is kept per browser, in the org's scope.
//
// The offer is asked for once the session's scope is known (StorageScopeGate), before any page
// mounts. Until then nothing has asked, and a request stays on this origin.

import { getStored, setStored } from "./browserStorage";

/** Where the chosen address is kept: per browser, in the control plane's and org's scope. */
export const DATA_ADDRESS_KEY = "provisa_data_address";

/** What the coordinator offers (GET /admin/data-addresses). */
export interface DataAddressOffer {
  /** Whether this UI picks a data node at all: true when a coordinator serves it. */
  choose: boolean;
  /** The addresses offered for the coordinator's region, in order. */
  addresses: string[];
}

let asked = false;
let pending: Promise<DataAddressOffer> | null = null;

async function fetchOffer(): Promise<DataAddressOffer> {
  const res = await fetch("/admin/data-addresses");
  if (!res.ok) throw new Error(`GET /admin/data-addresses ${res.status} ${res.statusText}`);
  return (await res.json()) as DataAddressOffer;
}

/** Ask the coordinator what it offers. Called when the session's scope is bound. */
export function requestDataAddresses(): void {
  asked = true;
  pending = fetchOffer();
  // A failed ask is reported by the request that waits on it (dataAddressOffer); asking again is
  // the next wait's job.
  pending.catch(() => undefined);
}

/** The offer, or null while nothing has asked for it. A failed ask throws, and the next call
 * asks again. */
export async function dataAddressOffer(): Promise<DataAddressOffer | null> {
  if (!asked) return null;
  if (pending === null) pending = fetchOffer();
  try {
    return await pending;
  } catch (err) {
    pending = null;
    throw err;
  }
}

/** Forget the offer (tests; a later ask starts over). */
export function resetDataAddresses(): void {
  asked = false;
  pending = null;
}

/** The address the browser chose, or null for none chosen. */
export function storedDataAddress(): string | null {
  return getStored(DATA_ADDRESS_KEY);
}

/** Keep `address` as the browser's choice. */
export function chooseDataAddress(address: string): void {
  setStored(DATA_ADDRESS_KEY, address);
}

/** The address data calls go to under `offer`: the browser's choice, else the first offered; null
 * for this origin (an every node's UI, or a coordinator offering none). */
export function dataAddressFor(offer: DataAddressOffer | null): string | null {
  if (offer === null || !offer.choose) return null;
  return storedDataAddress() ?? offer.addresses[0] ?? null;
}

/** Whether `address` is one a data call can go to: an http(s) URL with a host and no query. */
export function isDataAddress(address: string): boolean {
  let url: URL;
  try {
    url = new URL(address);
  } catch {
    return false;
  }
  return (url.protocol === "http:" || url.protocol === "https:") && !!url.host && !url.search;
}

/** Whether `pathname` is a data call's: the server's /data surface. */
export function isDataPath(pathname: string): boolean {
  return pathname === "/data" || pathname.startsWith("/data/");
}

/** `url` (relative, or on this origin) sent to `address` when it is a data call; otherwise
 * unchanged. A URL on another origin is never moved. */
export function atDataAddress(url: string, address: string | null): string {
  if (address === null) return url;
  const parsed = new URL(url, window.location.origin);
  if (parsed.origin !== window.location.origin || !isDataPath(parsed.pathname)) return url;
  return `${address.replace(/\/+$/, "")}${parsed.pathname}${parsed.search}`;
}

/** Where a data call to `url` goes now: on a coordinator-served UI, the chosen data address. */
export async function dataCallUrl(url: string): Promise<string> {
  const parsed = new URL(url, window.location.origin);
  if (parsed.origin !== window.location.origin || !isDataPath(parsed.pathname)) return url;
  return atDataAddress(url, dataAddressFor(await dataAddressOffer()));
}

/** A data surface's own page (the OpenAPI docs, loaded into a srcDoc frame, whose requests do not
 * pass through this window's fetch), its root-relative URLs resolved at `address`. */
export function pageAtDataAddress(html: string, address: string | null): string {
  if (address === null) return html;
  const base = `<base href="${address.replace(/\/+$/, "").replace(/"/g, "&quot;")}/">`;
  const head = /<head[^>]*>/i.exec(html);
  if (head === null) return base + html;
  const at = head.index + head[0].length;
  return html.slice(0, at) + base + html.slice(at);
}

/** The data address in force now, asking for the offer if it is in flight. */
export async function currentDataAddress(): Promise<string | null> {
  return dataAddressFor(await dataAddressOffer());
}
