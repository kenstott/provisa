// Copyright (c) 2026 Kenneth Stott
// Canary: e856229d-dd05-470e-b147-7860047308e5
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2259: the reads behind the Context group's Tags and Classification pages. Only the tables
// and columns the caller reads are named; the rest come back as a count.

import { requestFailed, serverMessage } from "../i18n/serverMessage";

const API_BASE = import.meta.env.VITE_API_BASE || "";

/** A table (``column`` null) or a column the caller reads. */
export interface ContextPlace {
  tableId: number;
  tableRef: string;
  column: string | null;
}

/** Where a tag is applied, and what depends on it. */
export interface ContextTagUse {
  id: string;
  usages: ContextPlace[];
  hiddenUsages: number;
  rowFilters: number;
  classificationKinds: string[];
}

/** A classification kind, and the columns it was detected on. */
export interface ContextClassificationKind {
  id: string;
  sensitive: boolean;
  threshold: number;
  maskPreset: string | null;
  enabled: boolean;
  detections: ContextPlace[];
  hiddenDetections: number;
}

async function get<T>(op: string, path: string): Promise<T> {
  const resp = await fetch(`${API_BASE}${path}`);
  if (!resp.ok) {
    const body = await resp.json().catch(() => ({ detail: resp.statusText }));
    throw new Error(serverMessage(body, requestFailed(op, resp.status)));
  }
  return resp.json() as Promise<T>;
}

export const fetchContextTags = () =>
  get<{ tags: ContextTagUse[] }>("fetchContextTags", "/admin/context/tags");

export const fetchContextClassification = () =>
  get<{ kinds: ContextClassificationKind[] }>(
    "fetchContextClassification",
    "/admin/context/classification",
  );

/** REQ-2259: a table the caller reads, with its detections awaiting a decision. */
export interface AwaitingTable {
  tableId: number;
  tableRef: string;
  awaiting: number;
}

/** REQ-2259 (Context Health): each table's latest profile run's detections no one has decided,
 * per table the caller reads; those on columns it does not read are counted in ``hidden``. */
export const fetchClassificationAwaiting = () =>
  get<{ tables: AwaitingTable[]; hidden: number }>(
    "fetchClassificationAwaiting",
    "/admin/context/classification/awaiting",
  );
