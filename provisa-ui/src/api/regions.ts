// Copyright (c) 2026 Kenneth Stott
// Canary: 8a3f5d27-0b6e-4c91-a4d8-3e2c7b9f1a65
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1922: regions, edited in the UI. The platform's regions (an id, a description, the purposes
// it serves and its data addresses) and the grant every new org receives are a platform
// administrator's and take effect on restart; so is each org's grant -- the regions it is granted
// and its stores in each -- saved as one set and refused naming the field. An org administrator
// reads its grant. A demo sandbox reads the platform's regions. A store URL comes back as entered
// -- a reference stays one -- without its password.

import { requestFailed, serverMessage } from "../i18n/serverMessage";

const API_BASE = import.meta.env.VITE_API_BASE || "";

/** Why a region exists. Explanatory only: a purpose changes no routing, policy or validation. */
export const REGION_PURPOSES = [
  "residency",
  "workload",
  "latency",
  "resilience",
  "dedicated",
] as const;
export type RegionPurpose = (typeof REGION_PURPOSES)[number];

/** A platform region as it is listed: its id, what it is for and why. */
export interface RegionInfo {
  id: string;
  description: string;
  purposes: RegionPurpose[];
}

/** A platform region as the platform administrator edits it. */
export interface PlatformRegion extends RegionInfo {
  /** Where the UI sends this region's data calls; none, the region's registered nodes. */
  data_addresses: string[];
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

/** Regions granted, with the stores they name. */
export interface Grant {
  stores: Store[];
  regions: OrgRegion[];
}

/** A grant as a page shows it: with the platform's regions and the engine kinds a store names. */
export interface GrantView extends Grant {
  platform_regions: RegionInfo[];
  engine_kinds: string[];
}

export interface PlatformRegions {
  regions: PlatformRegion[];
  default_grant: Grant;
  engine_kinds: string[];
  /** A demo sandbox: shown to explain regions, never changed there. */
  read_only: boolean;
}

/** A save the server refused: its reason, the field it is about (`regions.eu.engine`), and --
 * when it removes a region orgs hold -- the orgs it affects. */
export class RegionsRefused extends Error {
  readonly field: string | null;
  readonly affectedOrgs: string[] | null;

  constructor(message: string, field: string | null, affectedOrgs: string[] | null = null) {
    super(message);
    this.field = field;
    this.affectedOrgs = affectedOrgs;
  }
}

async function ok<T>(res: Response, op: string): Promise<T> {
  if (!res.ok) {
    const data = await res.json().catch(() => ({ detail: res.statusText }));
    const field = (data?.params?.field as string | null | undefined) ?? null;
    const orgs = (data?.params?.orgs as string[] | undefined) ?? null;
    throw new RegionsRefused(serverMessage(data, requestFailed(op, res.status)), field, orgs);
  }
  return res.json() as Promise<T>;
}

const put = (path: string, body: unknown) =>
  fetch(`${API_BASE}${path}`, {
    method: "PUT",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });

export async function fetchPlatformRegions(): Promise<PlatformRegions> {
  return ok<PlatformRegions>(
    await fetch(`${API_BASE}/admin/platform/regions`),
    "fetchPlatformRegions",
  );
}

export async function savePlatformRegions(
  regions: PlatformRegion[],
  defaultGrant: Grant,
  confirmRemoval: boolean,
): Promise<PlatformRegions> {
  return ok<PlatformRegions>(
    await put("/admin/platform/regions", {
      regions,
      default_grant: defaultGrant,
      confirm_removal: confirmRemoval,
    }),
    "savePlatformRegions",
  );
}

export interface OrgGrantSummary {
  id: string;
  regions: string[];
}

export async function fetchOrgGrants(): Promise<OrgGrantSummary[]> {
  const answered = await ok<{ orgs: OrgGrantSummary[] }>(
    await fetch(`${API_BASE}/admin/platform/orgs`),
    "fetchOrgGrants",
  );
  return answered.orgs;
}

const orgPath = (orgId: string) => `/admin/platform/orgs/${encodeURIComponent(orgId)}/regions`;

export async function fetchOrgGrant(orgId: string): Promise<GrantView> {
  return ok<GrantView>(await fetch(`${API_BASE}${orgPath(orgId)}`), "fetchOrgGrant");
}

export async function saveOrgGrant(
  orgId: string,
  grant: Grant,
  confirmRemoval: boolean,
): Promise<GrantView> {
  return ok<GrantView>(
    await put(orgPath(orgId), { ...grant, confirm_removal: confirmRemoval }),
    "saveOrgGrant",
  );
}

/** The acting org's grant, for its administrator to read. */
export async function fetchOrgRegions(): Promise<GrantView> {
  return ok<GrantView>(await fetch(`${API_BASE}/admin/regions`), "fetchOrgRegions");
}

/** The refusal of a save, and the field it names (`regions.eu.engine`). */
export interface Problem {
  text: string;
  field: string | null;
}

export const problemOf = (e: unknown): Problem =>
  e instanceof RegionsRefused
    ? { text: e.message, field: e.field }
    : { text: e instanceof Error ? e.message : String(e), field: null };
