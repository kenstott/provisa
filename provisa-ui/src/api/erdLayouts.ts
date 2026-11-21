// Copyright (c) 2026 Kenneth Stott
// Canary: 0e6b3c18-4d92-4f7a-a5c1-8b27e9d4f610
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2189: the saved ERD layouts, kept in the model (provisa/api/admin/erd_layouts_router.py).

import { serverMessage, requestFailed } from "../i18n/serverMessage";
import type { ErdOptions } from "../components/erd/useErdOptions";

const API_BASE = import.meta.env.VITE_API_BASE || "";

export interface SavedLayoutName {
  name: string;
  updatedBy: string | null;
  updatedAt: string;
}

/** A domain the layout shows: named by its focus, collapsed, and its box (centre and size). */
export interface DomainPlace {
  domainId: string;
  selected: boolean;
  collapsed: boolean;
  x: number | null;
  y: number | null;
  w: number | null;
  h: number | null;
}

/** A table the layout holds, by id; no position when its domain was collapsed at the save. */
export interface TablePlace {
  tableId: number;
  selected: boolean;
  x: number | null;
  y: number | null;
}

export interface LayoutArrangement {
  options: ErdOptions;
  domains: DomainPlace[];
  tables: TablePlace[];
}

export interface SavedLayout extends LayoutArrangement {
  name: string;
  updatedBy: string | null;
  updatedAt: string;
}

async function call<T>(
  op: string,
  path: string,
  method: "GET" | "PUT" | "DELETE" = "GET",
  body?: unknown,
): Promise<T> {
  const resp = await fetch(`${API_BASE}${path}`, {
    method,
    headers: body === undefined ? {} : { "Content-Type": "application/json" },
    body: body === undefined ? undefined : JSON.stringify(body),
  });
  if (!resp.ok) {
    const body = await resp.json().catch(() => ({ detail: resp.statusText }));
    throw new Error(serverMessage(body, requestFailed(op, resp.status)));
  }
  return resp.json() as Promise<T>;
}

const layoutPath = (name: string) => `/admin/erd-layouts/${encodeURIComponent(name)}`;

export const fetchErdLayouts = () =>
  call<{ layouts: SavedLayoutName[] }>("fetchErdLayouts", "/admin/erd-layouts");

export const fetchErdLayout = (name: string) =>
  call<SavedLayout>("fetchErdLayout", layoutPath(name));

export const saveErdLayout = (name: string, arrangement: LayoutArrangement) =>
  call<SavedLayout>("saveErdLayout", layoutPath(name), "PUT", arrangement);

export const deleteErdLayout = (name: string) =>
  call<{ deleted: string }>("deleteErdLayout", layoutPath(name), "DELETE");
