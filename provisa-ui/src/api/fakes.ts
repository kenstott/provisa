// Copyright (c) 2026 Kenneth Stott
// Canary: 1b451441-1fb0-405e-9bea-b7f9a4ac5367
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1494: the table editor's test-data mode (provisa/api/admin/fakes_router.py).

import { serverMessage, requestFailed } from "../i18n/serverMessage";

const API_BASE = import.meta.env.VITE_API_BASE || "";

export interface KindArg {
  name: string;
  // number | date | interval | text | list | column | expression | point
  kind: string;
  required: boolean;
}

// What the server's checks answer for a kind or method: the column families (provisa.fakes.checks
// .family) whose columns it can fill, and whether a stable one is accepted -- with `stableNeeds`
// the arguments a stable one must declare. A stable method takes no arguments.
export interface FakeFit {
  families: string[];
  stable: boolean;
  stableNeeds: string[];
}

export interface FakeKind extends FakeFit {
  category: string;
  name: string;
  positional: boolean;
  ruleOnly: boolean;
  args: KindArg[];
}

export interface FakeMethodParam {
  name: string;
  required: boolean;
  default: string | null;
}

export interface FakeMethod extends FakeFit {
  name: string;
  category: string;
  params: FakeMethodParam[];
}

export interface FakeCatalog {
  kinds: FakeKind[];
  methods: FakeMethod[];
  // The family of each column type the catalog was asked for, as the server's checks name it.
  typeFamilies: Record<string, string>;
}

export interface ColumnFakeCheck {
  tableId: number | null;
  tableName: string;
  column: string;
  dataType: string;
  fake: string | null;
  syntheticRule: string | null;
  stable: boolean;
}

async function call<T>(op: string, path: string, init: RequestInit = {}): Promise<T> {
  const headers = new Headers(init.headers);
  if (init.body) headers.set("Content-Type", "application/json");
  const resp = await fetch(`${API_BASE}${path}`, { ...init, headers });
  if (!resp.ok) {
    const body = await resp.json().catch(() => ({ detail: resp.statusText }));
    throw new Error(serverMessage(body, requestFailed(op, resp.status)));
  }
  return resp.json() as Promise<T>;
}

// The catalog, with the family of each of the column types given.
export const fetchFakeCatalog = (dataTypes: string[]) =>
  call<FakeCatalog>(
    "fetchFakeCatalog",
    `/admin/fakes/catalog?${new URLSearchParams(dataTypes.map((t) => ["dataType", t]))}`,
  );

// Resolves when the column's fake, rule and stable flag would be saved; rejects with the save's own
// refusal otherwise.
export const checkColumnFake = (body: ColumnFakeCheck) =>
  call<{ ok: boolean }>("checkColumnFake", "/admin/fakes/check", {
    method: "POST",
    body: JSON.stringify(body),
  });

export interface FakeProposals {
  runId: string;
  // By column: a fake for an identifying column, else a synthetic rule.
  columns: Record<string, { fake?: string; syntheticRule?: string }>;
  // Columns tagged pii with no confident match: left for the operator.
  unmatchedPii: string[];
}

// Fill from profile: proposals from the table's latest profile run; nothing is saved.
export const proposeFakes = (tableId: number) =>
  call<FakeProposals>("proposeFakes", "/admin/fakes/propose", {
    method: "POST",
    body: JSON.stringify({ tableId }),
  });
