// Copyright (c) 2026 Kenneth Stott
// Canary: 8a3f5d27-0b6e-4c91-a4d8-3e2c7b9f1a65
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1922: regions, edited in the UI. The platform's regions (an id and the address its nodes
// answer at) are a platform administrator's and take effect on restart; an org's stores and the
// platform regions it selects are its administrator's, saved as one set and refused naming the
// field. A store URL comes back as entered -- a reference stays one -- without its password.

import { requestFailed, serverMessage } from "../i18n/serverMessage";

const API_BASE = import.meta.env.VITE_API_BASE || "";

export interface PlatformRegion {
  id: string;
  /** Where the region's nodes answer, as entered (a reference stays one). */
  address: string;
}

export interface Store {
  id: string;
  /** The connection URL as entered, its password left out. */
  url: string;
  /** The engine kind, for a store a region names as its engine. */
  kind: string | null;
}

/** The store each kind of data is kept in, in a region. */
export const STORE_ROLES = ["engine", "replicas", "views", "cache", "state", "record"] as const;
export type StoreRole = (typeof STORE_ROLES)[number];

export type OrgRegion = { id: string } & Record<StoreRole, string>;

export interface OrgRegions {
  platform_regions: PlatformRegion[];
  stores: Store[];
  regions: OrgRegion[];
  engine_kinds: string[];
}

/** A save the server refused: its reason, and the field it is about (`regions.eu.engine`). */
export class RegionsRefused extends Error {
  readonly field: string | null;

  constructor(message: string, field: string | null) {
    super(message);
    this.field = field;
  }
}

async function ok<T>(res: Response, op: string): Promise<T> {
  if (!res.ok) {
    const data = await res.json().catch(() => ({ detail: res.statusText }));
    const field = (data?.params?.field as string | null | undefined) ?? null;
    throw new RegionsRefused(serverMessage(data, requestFailed(op, res.status)), field);
  }
  return res.json() as Promise<T>;
}

const put = (path: string, body: unknown) =>
  fetch(`${API_BASE}${path}`, {
    method: "PUT",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });

export async function fetchPlatformRegions(): Promise<PlatformRegion[]> {
  const answered = await ok<{ regions: PlatformRegion[] }>(
    await fetch(`${API_BASE}/admin/platform/regions`),
    "fetchPlatformRegions",
  );
  return answered.regions;
}

export async function savePlatformRegions(regions: PlatformRegion[]): Promise<PlatformRegion[]> {
  const answered = await ok<{ regions: PlatformRegion[] }>(
    await put("/admin/platform/regions", { regions }),
    "savePlatformRegions",
  );
  return answered.regions;
}

export async function fetchOrgRegions(): Promise<OrgRegions> {
  return ok<OrgRegions>(await fetch(`${API_BASE}/admin/regions`), "fetchOrgRegions");
}

export async function saveOrgRegions(
  stores: Store[],
  regions: OrgRegion[],
): Promise<Pick<OrgRegions, "stores" | "regions">> {
  return ok(await put("/admin/regions", { stores, regions }), "saveOrgRegions");
}
