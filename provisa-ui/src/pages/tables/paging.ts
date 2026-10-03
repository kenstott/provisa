// Copyright (c) 2026 Kenneth Stott
// Canary: 0af70d5b-2b07-4135-af77-1ae5f07db484
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-318: a table's paging, authored on the table (provisa/core/paging.py). A paged REST
// endpoint declares its type, the parameters it pages by, its page size and max pages; a
// connection table sets max rows only, which may lower the operator's graphql_remote.max_rows but
// never raise it. The server refuses the same cases by name; these checks keep Save honest.

import type { Paging, PagingKind } from "../../types/admin";

export const NO_PAGING: Paging = {
  type: null,
  cursorField: null,
  cursorParam: null,
  pageParam: null,
  pageSizeParam: null,
  pageSize: null,
  maxPages: null,
  maxRows: null,
};

const FIELDS = Object.keys(NO_PAGING) as (keyof Paging)[];

// The declared fields only: no nulls (unset), no Apollo __typename (the input type rejects it).
export function pagingInput(paging: Paging): Partial<Record<keyof Paging, string | number>> {
  const out: Partial<Record<keyof Paging, string | number>> = {};
  for (const field of FIELDS) {
    const value = paging[field];
    if (value !== null && value !== undefined && value !== "") out[field] = value;
  }
  return out;
}

// Nothing declared is no paging: saved as null, which clears it.
export function declaredPaging(paging: Paging | null): Paging | null {
  if (paging === null) return null;
  return Object.keys(pagingInput(paging)).length === 0 ? null : paging;
}

export function pagingChanged(saved: Paging | null, staged: Paging | null): boolean {
  const a = declaredPaging(saved);
  const b = declaredPaging(staged);
  return JSON.stringify(a && pagingInput(a)) !== JSON.stringify(b && pagingInput(b));
}

export interface PagingProblem {
  key: string;
  params?: Record<string, number>;
}

const positive = (v: number | null) => v === null || (Number.isInteger(v) && v >= 1);

export function pagingProblem(
  kind: PagingKind,
  paging: Paging | null,
  ceilingRows: number | null,
): PagingProblem | null {
  const declared = declaredPaging(paging);
  if (declared === null) return null;
  if (kind === "connection") {
    if (!positive(declared.maxRows)) return { key: "tableEditForm.pagingPositive" };
    if (ceilingRows !== null && declared.maxRows !== null && declared.maxRows > ceilingRows) {
      return { key: "tableEditForm.pagingAboveCeiling", params: { ceiling: ceilingRows } };
    }
    return null;
  }
  if (declared.type === null) return { key: "tableEditForm.pagingTypeRequired" };
  if (!positive(declared.pageSize) || !positive(declared.maxPages)) {
    return { key: "tableEditForm.pagingPositive" };
  }
  return null;
}
