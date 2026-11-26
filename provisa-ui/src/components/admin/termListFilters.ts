// Copyright (c) 2026 Kenneth Stott
// Canary: 6c2e9a41-7f3b-4d85-b0e6-2a9d14c8f573
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2259, REQ-2277: the Ontology page's term list filters as a URL names them, so a count on
// Context Health opens the list showing exactly the terms it counts. One predicate decides both
// the count and the list.

import type { GlossaryTermSummary, TermGrounding, TermStatus } from "../../api/glossary";
import { RELATED_TO_PARAM } from "../../lib/reportCellHref";
import { toReview } from "./termFindings";

export interface TermListFilters {
  status: TermStatus[];
  grounding: TermGrounding | null;
  agentVisible: boolean;
  toReview: boolean;
  relatedTo: boolean;
}

export const NO_TERM_FILTERS: TermListFilters = {
  status: [],
  grounding: null,
  agentVisible: false,
  toReview: false,
  relatedTo: false,
};

const STATUSES: readonly TermStatus[] = ["draft", "approved", "deprecated", "retired"];
const GROUNDINGS: readonly TermGrounding[] = ["grounded", "grounded_pending", "not_grounded"];

/** The URL parameter each filter is named by. */
export const TERM_FILTER_PARAMS = {
  status: "status",
  grounding: "grounding",
  agentVisible: "agentVisible",
  toReview: "toReview",
  relatedTo: RELATED_TO_PARAM,
} as const;

/** The filters a URL names, or null when it names none (the page then opens as it was left). */
export function linkedTermFilters(params: URLSearchParams): TermListFilters | null {
  if (!Object.values(TERM_FILTER_PARAMS).some((p) => params.has(p))) return null;
  const status = (params.get(TERM_FILTER_PARAMS.status) ?? "")
    .split(",")
    .filter((s): s is TermStatus => (STATUSES as readonly string[]).includes(s));
  const grounding = params.get(TERM_FILTER_PARAMS.grounding);
  return {
    status,
    grounding: (GROUNDINGS as readonly string[]).includes(grounding ?? "")
      ? (grounding as TermGrounding)
      : null,
    agentVisible: params.get(TERM_FILTER_PARAMS.agentVisible) === "1",
    toReview: params.get(TERM_FILTER_PARAMS.toReview) === "1",
    relatedTo: params.get(TERM_FILTER_PARAMS.relatedTo) === "1",
  };
}

/** `params` with the filters written in, every filter parameter not set removed. */
export function withTermFilters(params: URLSearchParams, f: TermListFilters): URLSearchParams {
  const n = new URLSearchParams(params);
  const set = (name: string, value: string | null) => (value ? n.set(name, value) : n.delete(name));
  set(TERM_FILTER_PARAMS.status, f.status.join(","));
  set(TERM_FILTER_PARAMS.grounding, f.grounding);
  set(TERM_FILTER_PARAMS.agentVisible, f.agentVisible ? "1" : null);
  set(TERM_FILTER_PARAMS.toReview, f.toReview ? "1" : null);
  set(TERM_FILTER_PARAMS.relatedTo, f.relatedTo ? "1" : null);
  return n;
}

/** The Ontology page's term list (never its graph), filtered by `f`. */
export function termListHref(f: Partial<TermListFilters>): string {
  const params = withTermFilters(new URLSearchParams({ panel: "terms" }), {
    ...NO_TERM_FILTERS,
    ...f,
  });
  return `/context/ontology?${params.toString()}`;
}

/** Whether `term` is one the filters show. */
export function matchesTermFilters(term: GlossaryTermSummary, f: TermListFilters): boolean {
  return (
    (f.status.length === 0 || f.status.includes(term.status)) &&
    (f.grounding === null || term.grounding === f.grounding) &&
    (!f.agentVisible || term.agent_visible) &&
    (!f.toReview || toReview(term)) &&
    (!f.relatedTo || term.related_to > 0)
  );
}
