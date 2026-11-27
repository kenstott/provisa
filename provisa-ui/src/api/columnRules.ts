// Copyright (c) 2026 Kenneth Stott
// Canary: 9d2c4f71-3a85-4e6b-b1f0-7c8e2a5d9b14
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2278: column rules keyed on a tag or a classification. A rule names a role and hides, or
// masks, every column (a table's or a command's output column) carrying its key.

import { serverMessage, requestFailed } from "../i18n/serverMessage";
import type { MaskSpec } from "../lib/masks";

const API_BASE = import.meta.env.VITE_API_BASE || "";

export type ColumnRuleAction = "hide" | "mask";

export interface ColumnRule {
  id: number;
  tagId: string | null;
  classification: string | null;
  roleId: string;
  action: ColumnRuleAction;
  /** A mask rule's mask; null on a mask rule: completely masked. Always null on a hide rule. */
  mask: MaskSpec | null;
}

/** The key a rule is on: a tag or a classification kind, exactly one. */
export type ColumnRuleKey = { tagId: string } | { classification: string };

async function call<T>(op: string, path: string, method = "GET", body?: unknown): Promise<T> {
  const resp = await fetch(`${API_BASE}${path}`, {
    method,
    headers: body === undefined ? {} : { "Content-Type": "application/json" },
    body: body === undefined ? undefined : JSON.stringify(body),
  });
  if (!resp.ok) {
    const said = await resp.json().catch(() => ({ detail: resp.statusText }));
    throw new Error(serverMessage(said, requestFailed(op, resp.status)));
  }
  return resp.json() as Promise<T>;
}

function query(key: ColumnRuleKey): string {
  return "tagId" in key
    ? `tag=${encodeURIComponent(key.tagId)}`
    : `classification=${encodeURIComponent(key.classification)}`;
}

export const fetchColumnRules = (key: ColumnRuleKey) =>
  call<{ rules: ColumnRule[] }>("fetchColumnRules", `/admin/column-rules?${query(key)}`);

export const saveColumnRule = (
  key: ColumnRuleKey,
  roleId: string,
  action: ColumnRuleAction,
  mask: MaskSpec | null,
) =>
  call<{ rule: ColumnRule }>("saveColumnRule", "/admin/column-rules", "PUT", {
    tag_id: "tagId" in key ? key.tagId : null,
    classification: "classification" in key ? key.classification : null,
    role_id: roleId,
    action,
    mask: action === "hide" ? null : mask,
  });

export const deleteColumnRule = (id: number) =>
  call<{ deleted: number }>("deleteColumnRule", `/admin/column-rules/${id}`, "DELETE");
