// Copyright (c) 2026 Kenneth Stott
// Canary: efe058d7-81b2-4c62-b865-6bc28ef4bdc4
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2219 (ONE API, THREE WAYS TO USE IT): the UI is a client of the one write API for a
// principal's entitlement lists (provisa/api/admin/entitlements_router.py). Nothing here decides
// precedence or validity: the server answers with each list's origin and refuses a write the
// identity provider supersedes, by code.

import { requestFailed, serverMessage } from "../i18n/serverMessage";

const API_BASE = import.meta.env.VITE_API_BASE || "";

/** Where a list came from: the identity provider, Provisa, or the role's defaults. */
export type ListOrigin = "idp" | "provisa" | "role";

/** One kind's effective list for a person's role grant (provisa/entitlements/effective.py). */
export interface EntitlementListView {
  kind: string;
  identifiers: string[];
  origin: ListOrigin;
  superseded: string[] | null;
  ended: string[];
  set_by: string | null;
  reason: string | null;
}

export interface ExpiryRuleView {
  rule_key: string | null;
  ends_at: string;
  ends: Record<string, string[]>;
  list_kind: string | null;
  reason: string | null;
  passed: boolean;
}

export interface EntitlementFinding {
  code: string;
  role_id: string;
  kind: string;
  identifier: string;
  message: string;
}

export interface AccessView {
  principal: string;
  role_id: string;
  lists: EntitlementListView[];
  expiries: ExpiryRuleView[];
  grant_ended: boolean;
  findings: EntitlementFinding[];
}

export interface HeldList {
  role_id: string;
  kind: string;
  identifiers: string[];
  capability: string | null;
  set_by: string | null;
  reason: string | null;
}

export interface HeldLists {
  principal: string;
  lists: HeldList[];
}

/** A person the picker found: the principal a sign-in token carries, name and email for display. */
export interface PersonMatch {
  principal: string;
  name: string | null;
  email: string | null;
}

/** A grant waiting on a typed email until a sign-in token's email matches it. */
export interface PendingGrant {
  pending: boolean;
  email: string;
  role_id: string;
  kind: string;
  identifiers: string[];
  expires_at: string | null;
  set_by: string | null;
  reason: string | null;
}

/** Where a person's role came from: Provisa's assignments, the deployment's default
 *  assignments, or the identity provider (provisa/entitlements/roles.py). */
export type RoleOrigin = "provisa" | "default_assignment" | "idp";

/** A role a person holds, with each place it came from and the domains its assignments name. */
export interface HeldRole {
  role_id: string;
  origins: RoleOrigin[];
  domains: string[];
}

export interface PersonRoles {
  principal: string;
  roles: HeldRole[];
}

/** One node of the names tree: the identifier it is listed as, and its last segment. */
export interface NamesNode {
  id: string;
  label: string;
  children: NamesNode[];
}

/** A kind of list the write accepts: whether an expiry can end it, and how deep the names tree
 *  goes when it is picked from there (null: typed or picked otherwise). */
export interface KindInfo {
  kind: string;
  semantic: boolean;
  tree_depth: 1 | 2 | 3 | null;
}

/** The kinds the write accepts, in the order they are listed, and the model's names tree (null
 *  before a model is built). */
export interface EntitlementKinds {
  kinds: KindInfo[];
  names: NamesNode[] | null;
}

/** Whom a write names: a principal, or an email for a person who may not have been seen yet. */
export type Grantee = { principal: string } | { email: string };

/** A refusal from the entitlements API, carrying the server's stable code. */
export class EntitlementError extends Error {
  readonly status: number;
  readonly code: string | null;
  constructor(status: number, code: string | null, message: string) {
    super(message);
    this.name = "EntitlementError";
    this.status = status;
    this.code = code;
  }
}

async function call<T>(op: string, path: string, init?: RequestInit): Promise<T> {
  const res = await fetch(`${API_BASE}/admin/entitlements${path}`, init);
  if (!res.ok) {
    const body = await res.json().catch(() => null);
    throw new EntitlementError(
      res.status,
      body?.code ?? null,
      serverMessage(body, requestFailed(op, res.status)),
    );
  }
  return res.json();
}

const json = (method: string, body: unknown): RequestInit => ({
  method,
  headers: { "Content-Type": "application/json" },
  body: JSON.stringify(body),
});

export function fetchEffective(principal: string, roleId: string): Promise<AccessView> {
  const q = new URLSearchParams({ principal, role_id: roleId });
  return call("fetchEffective", `/effective?${q}`);
}

export function fetchHeldLists(principal: string): Promise<HeldLists> {
  return call("fetchHeldLists", `/lists?${new URLSearchParams({ principal })}`);
}

export function searchPeople(q: string): Promise<PersonMatch[]> {
  return call("searchPeople", `/people?${new URLSearchParams({ q })}`);
}

/** The roles `principal` holds, within the roles the caller administers. */
export function fetchPersonRoles(principal: string): Promise<PersonRoles> {
  return call("fetchPersonRoles", `/roles?${new URLSearchParams({ principal })}`);
}

/** The kinds the write accepts and the names tree a list is picked from. */
export function fetchKinds(): Promise<EntitlementKinds> {
  return call("fetchKinds", "/kinds");
}

export function fetchPending(): Promise<PendingGrant[]> {
  return call("fetchPending", "/pending");
}

export interface SetListInput {
  grantee: Grantee;
  roleId: string;
  kind: string;
  identifiers: string[];
  reason: string;
  /** ISO timestamp with a zone; null keeps the list without an end. */
  expiresAt: string | null;
}

/** Set a list, replacing what was held. Answers with the person's effective access through the
 *  role, or the pending grant when the email matched no one. */
export function setList(input: SetListInput): Promise<AccessView | PendingGrant> {
  return call(
    "setList",
    "/lists",
    json("PUT", {
      ...input.grantee,
      role_id: input.roleId,
      kind: input.kind,
      identifiers: input.identifiers,
      reason: input.reason,
      expires_at: input.expiresAt,
    }),
  );
}

export interface EndListInput {
  grantee: Grantee;
  roleId: string;
  kind: string;
  reason: string;
}

export function endList(input: EndListInput): Promise<AccessView | PendingGrant> {
  return call(
    "endList",
    "/lists/end",
    json("POST", {
      ...input.grantee,
      role_id: input.roleId,
      kind: input.kind,
      reason: input.reason,
    }),
  );
}
