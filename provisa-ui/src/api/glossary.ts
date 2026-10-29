// Copyright (c) 2026 Kenneth Stott
// Canary: 1b9a4102-8560-44b2-989d-abad05c5affa
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1387: business-glossary curation client. Terms are lifecycle-managed by
// the semantic layer; this surface carries the human curation — rename,
// definitions, ref moves, experts, abstract terms and their typed edges.

import { serverMessage, requestFailed } from "../i18n/serverMessage";

const API_BASE = import.meta.env.VITE_API_BASE || "";

// Closed enums — the server rejects anything else.
export const GLOSSARY_REL_TYPES = [
  "KIND_OF",
  "RELATED_TO",
  "PART_OF",
  "SYNONYM_OF",
  "VALID_VALUE_OF",
  "DERIVED_FROM",
  "REPLACES",
  "PREFERRED_TERM_FOR",
  "TRANSLATION_OF",
  "ANTONYM_OF",
] as const;
export type GlossaryRelType = (typeof GLOSSARY_REL_TYPES)[number];

// REQ-1592: a term's scope as the whole org rather than a named set of domains. Exclusive — it
// never appears alongside a domain — and declaring or lifting it takes `org_glossary_rw`.
export const ENTERPRISE_DOMAIN = "*";

export const GLOSSARY_EXPERT_KINDS = ["expert", "author"] as const;
export type GlossaryExpertKind = (typeof GLOSSARY_EXPERT_KINDS)[number];

/** A term's status (REQ-1387, REQ-2144), set by people: draft (the default), approved (a
 * steward's choice, which needs a definition), deprecated or retired. */
export type TermStatus = "draft" | "approved" | "deprecated" | "retired";

/** A term's grounding (REQ-1387), computed and never set: it looks down the term's paths to data.
 * grounded_pending: every path down has a term not yet approved between it and the data. */
export type TermGrounding = "grounded" | "grounded_pending" | "not_grounded";

export interface GlossaryTermSummary {
  id: number;
  name: string;
  definition: string | null;
  is_abstract: boolean;
  deprecated: boolean;
  ref_count: number;
  // REQ-1387 (RELATED_TO HOLDS THE PLACE): the RELATED_TO edges the term is on, either end.
  related_to: number;
  // REQ-2130: the tables the term grounds (as a class term).
  table_count: number;
  // REQ-2130/2141: grounded in several tables that same-entity relationships do not all join.
  identity_gap: boolean;
  export_excluded: boolean;
  // Curator soft delete: the term keeps its refs but no consuming surface binds it —
  // agent term search skips it and metadata export withholds it.
  retired: boolean;
  // REQ-1387: server-computed from the term graph, never derivable here.
  grounding: TermGrounding;
  // REQ-1387: an agent sees the term by name (approved on a path to data, or a defined draft on
  // a path holding an approved term).
  agent_visible: boolean;
  // REQ-1591: a rooted term's domains are DERIVED from its refs' tables, an abstract term's are
  // DECLARED. Empty means unscoped — reachable by any glossary-right holder.
  domains: string[];
  // REQ-2146: how many findings the term has (GET /admin/glossary/findings lists them).
  findings: number;
  status: TermStatus;
  // REQ-2144: why it was deprecated or retired, who said so, and a deprecation's planned
  // retirement; none on a draft or an approved term.
  status_reason: string | null;
  status_by: string | null;
  retire_on: string | null;
}

export interface GlossaryRef {
  table_id: number;
  column_name: string;
  source_id: string;
  schema_name: string;
  table_name: string;
  alias: string | null;
  domain_id: string;
}

export interface GlossaryEdge {
  term_id: number;
  rel_type: GlossaryRelType;
  name: string;
}

export interface GlossaryExpert {
  user_id: string;
  kind: GlossaryExpertKind;
}

export interface GlossaryTermDetail extends GlossaryTermSummary {
  refs: GlossaryRef[];
  edges_out: GlossaryEdge[];
  edges_in: GlossaryEdge[];
  experts: GlossaryExpert[];
  // REQ-2145: the tags on the term (pii), which every column it holds carries.
  tags: { tag_id: string; reason: string | null }[];
  // REQ-1387: for a grounded_pending term, the drafts whose approval alone grounds it.
  pending_on: { term_id: number; name: string }[];
}

// REQ-1634: a data product's Related Terms panel — terms directly tied to any member-table
// column, plus the full transitive closure of abstract terms reachable from those via edges.
export interface RelatedGlossaryTerm {
  id: number;
  name: string;
  definition: string | null;
  is_abstract: boolean;
  status: TermStatus;
  grounding: TermGrounding;
  domains: string[];
}

async function mutationError(res: Response, op: string): Promise<Error> {
  const data = await res.json().catch(() => ({ detail: res.statusText }));
  return new Error(serverMessage(data, requestFailed(op, res.status)));
}

// REQ-1591: ``domains`` is the navbar VIEW filter — null means "no narrowing asked for", which is
// a different request from an empty selection. It only ever intersects with the caller's role
// authority on the server; it can never widen it. Repeated ``domains=`` parameters rather than one
// comma-joined string, because the no-domain bucket's id is the empty string and a join loses it.
export async function listGlossaryTerms(
  q: string,
  includeDeprecated: boolean,
  domains: string[] | null = null,
): Promise<GlossaryTermSummary[]> {
  const params = new URLSearchParams({ include_deprecated: String(includeDeprecated) });
  if (q) params.set("q", q);
  if (domains) for (const d of domains) params.append("domains", d);
  const res = await fetch(`${API_BASE}/admin/glossary/terms?${params.toString()}`);
  if (!res.ok) throw new Error(requestFailed("listGlossaryTerms", res.status));
  return res.json();
}

export async function fetchRelatedGlossaryTerms(
  tableIds: (number | string)[],
): Promise<RelatedGlossaryTerm[]> {
  if (tableIds.length === 0) return [];
  const params = new URLSearchParams();
  for (const id of tableIds) params.append("table_id", String(id));
  const res = await fetch(`${API_BASE}/admin/glossary/related-to-tables?${params.toString()}`);
  if (!res.ok) throw new Error(requestFailed("fetchRelatedGlossaryTerms", res.status));
  return res.json();
}

export async function fetchGlossaryTerm(termId: number): Promise<GlossaryTermDetail> {
  const res = await fetch(`${API_BASE}/admin/glossary/terms/${termId}`);
  if (!res.ok) throw new Error(requestFailed("fetchGlossaryTerm", res.status));
  return res.json();
}

// REQ-1387: column-name hover card lookup. A 404 ({code:"glossary.ref_not_found"})
// is the server's designed "this column has no glossary term" signal — the null
// mapping is that contract, not a fallback. Any other failure is a real error.
export async function fetchGlossaryTermByRef(
  tableId: number,
  columnName: string,
): Promise<GlossaryTermDetail | null> {
  const params = new URLSearchParams({
    table_id: String(tableId),
    column_name: columnName,
  });
  const res = await fetch(`${API_BASE}/admin/glossary/ref?${params.toString()}`);
  if (res.status === 404) return null;
  if (!res.ok) throw await mutationError(res, "fetchGlossaryTermByRef");
  return res.json();
}

// REQ-1591: an abstract term holds no refs, so its domains are DECLARED here — the server requires
// at least one in multi-domain mode, and each must be within the caller's own authority.
export async function createGlossaryTerm(body: {
  name: string;
  definition?: string;
  domains?: string[];
}): Promise<{ id: number }> {
  const res = await fetch(`${API_BASE}/admin/glossary/terms`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  if (!res.ok) throw await mutationError(res, "createGlossaryTerm");
  return res.json();
}

export async function updateGlossaryTerm(
  termId: number,
  body: {
    name?: string;
    definition?: string | null;
    export_excluded?: boolean;
    // REQ-1591: an abstract term's declared domains; a rooted term's come from its refs.
    domains?: string[];
  },
): Promise<void> {
  const res = await fetch(`${API_BASE}/admin/glossary/terms/${termId}`, {
    method: "PATCH",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  if (!res.ok) throw await mutationError(res, "updateGlossaryTerm");
}

export async function generateAllGlossaryDefinitions(): Promise<{ generated: number }> {
  const res = await fetch(`${API_BASE}/admin/glossary/definitions/generate`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
  });
  if (!res.ok) throw await mutationError(res, "generateAllGlossaryDefinitions");
  return res.json();
}

// REQ-1387 (THE SHAPES THAT RESOLVE A RELATED PAIR): the one-term shapes, tightest first, that would
// turn a RELATED_TO pair into grounding edges; the assistant names candidates, the curator chooses.
export const RESOLVE_SHAPES = [
  "component_type",
  "specialisation",
  "common_whole",
  "common_genus",
  "common_enumeration",
] as const;
export type ResolveShape = (typeof RESOLVE_SHAPES)[number];

/** A RELATED_TO the assistant added, with the shapes that may resolve it. */
export interface RelatedToReview {
  from_term_id: number;
  from: string;
  to_term_id: number;
  to: string;
  resolve: { shape: ResolveShape; candidates: string[] }[];
}

// Where the guidance on building a glossary that grounds lives: the type tests, RELATED_TO as the
// placeholder, and the shapes that resolve a pair. Served by the app's own docs site, so an
// air-gapped install reaches it too.
export const GROUNDING_GUIDE_URL = "/docs-site/ontology/#building-a-glossary-that-grounds";

/** The grounding types' order in the picker: these connect a term to data, so they come first. */
export const GROUNDING_REL_TYPES: readonly GlossaryRelType[] = [
  "KIND_OF",
  "PART_OF",
  "VALID_VALUE_OF",
];

/** Picker order: the grounding types first, RELATED_TO last, the rest between as declared. */
export function orderRelTypes(types: readonly GlossaryRelType[]): GlossaryRelType[] {
  const rank = (rt: GlossaryRelType) =>
    GROUNDING_REL_TYPES.includes(rt) ? 0 : rt === "RELATED_TO" ? 2 : 1;
  return [...types].sort((a, b) => rank(a) - rank(b));
}

/** Only RELATED_TO links, and not grounded: the term may want an intermediate term (REQ-1387). */
export function onlyRelatedTo(t: GlossaryTermDetail): boolean {
  const edges = [...t.edges_out, ...t.edges_in];
  return (
    t.grounding === "not_grounded" &&
    edges.length > 0 &&
    edges.every((e) => e.rel_type === "RELATED_TO")
  );
}

export async function generateGlossaryRelationships(): Promise<{
  added: number;
  review: RelatedToReview[];
}> {
  const res = await fetch(`${API_BASE}/admin/glossary/relationships/generate`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
  });
  if (!res.ok) throw await mutationError(res, "generateGlossaryRelationships");
  return res.json();
}

export async function generateGlossaryDefinition(termId: number): Promise<string> {
  const res = await fetch(`${API_BASE}/admin/glossary/terms/${termId}/definition/generate`, {
    method: "POST",
  });
  if (!res.ok) throw await mutationError(res, "generateGlossaryDefinition");
  const data = (await res.json()) as { definition: string };
  return data.definition;
}

export async function deleteGlossaryTerm(termId: number): Promise<void> {
  const res = await fetch(`${API_BASE}/admin/glossary/terms/${termId}`, { method: "DELETE" });
  if (!res.ok) throw await mutationError(res, "deleteGlossaryTerm");
}

/** How a move that takes tags off columns goes ahead (REQ-2145): each lost tag confirmed, or the
 * tags carried to the term the columns move to. */
export interface TagLossResolution {
  confirm_tag_loss?: string[];
  carry_tags?: boolean;
}

export async function moveGlossaryRef(
  body: {
    table_id: number;
    column_name: string;
    to_term_id: number;
  } & TagLossResolution,
): Promise<{ source_term_removed: boolean }> {
  const res = await fetch(`${API_BASE}/admin/glossary/refs/move`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  if (!res.ok) throw await moveRefusal(res, "moveGlossaryRef");
  return (await res.json()) as { source_term_removed: boolean };
}

export async function addGlossaryEdge(
  termId: number,
  toTermId: number,
  relType: GlossaryRelType,
): Promise<void> {
  const res = await fetch(`${API_BASE}/admin/glossary/terms/${termId}/edges`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ to_term_id: toTermId, rel_type: relType }),
  });
  if (!res.ok) throw await mutationError(res, "addGlossaryEdge");
}

// The relationship type is part of an edge's identity, so retyping is its own endpoint —
// the server does the delete and the insert together rather than the UI doing both.
export async function retypeGlossaryEdge(
  termId: number,
  toTermId: number,
  relType: GlossaryRelType,
  newRelType: GlossaryRelType,
): Promise<void> {
  const res = await fetch(`${API_BASE}/admin/glossary/terms/${termId}/edges`, {
    method: "PATCH",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ to_term_id: toTermId, rel_type: relType, new_rel_type: newRelType }),
  });
  if (!res.ok) throw await mutationError(res, "retypeGlossaryEdge");
}

export async function removeGlossaryEdge(
  termId: number,
  toTermId: number,
  relType: GlossaryRelType,
): Promise<void> {
  const params = new URLSearchParams({ to_term_id: String(toTermId), rel_type: relType });
  const res = await fetch(`${API_BASE}/admin/glossary/terms/${termId}/edges?${params.toString()}`, {
    method: "DELETE",
  });
  if (!res.ok) throw await mutationError(res, "removeGlossaryEdge");
}

export async function addGlossaryExpert(
  termId: number,
  userId: string,
  kind: GlossaryExpertKind,
): Promise<void> {
  const res = await fetch(`${API_BASE}/admin/glossary/terms/${termId}/experts`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ user_id: userId, kind }),
  });
  if (!res.ok) throw await mutationError(res, "addGlossaryExpert");
}

export async function removeGlossaryExpert(termId: number, userId: string): Promise<void> {
  const res = await fetch(
    `${API_BASE}/admin/glossary/terms/${termId}/experts/${encodeURIComponent(userId)}`,
    { method: "DELETE" },
  );
  if (!res.ok) throw await mutationError(res, "removeGlossaryExpert");
}

// REQ-2130 (class grounding): the tables and views a term names the rows of. A table has at most
// one class term, and only a table with a primary key can have one; the server says so by code.
export interface GlossaryTermTable {
  table_id: number;
  source_id: string;
  schema_name: string;
  table_name: string;
  domain_id: string;
}

/** A finding on a term (REQ-2146): its kind, the catalog code its message is worded by
 * (serverErrors), the server's own words, and the values the message names. */
export type FindingKind =
  | "unlinked_tables"
  | "class_and_attribute"
  | "tag_wrong_kind"
  | "retirement_blocked"
  | "no_data"
  | "approved_not_grounded"
  | "path_pending";

export interface GlossaryFinding {
  kind: FindingKind;
  code: string;
  message: string;
  params: Record<string, string>;
}

export interface TermFindings {
  term_id: number;
  term: string;
  findings: GlossaryFinding[];
}

export async function fetchGlossaryFindings(): Promise<TermFindings[]> {
  const res = await fetch(`${API_BASE}/admin/glossary/findings`);
  if (!res.ok) throw await mutationError(res, "fetchGlossaryFindings");
  return res.json();
}

/** What a move does to one column's tags (REQ-2145): those it would lose, those it would gain. */
export interface ColumnTagChange {
  column: string;
  table: string;
  loses: string[];
  gains: string[];
}

/** A move refused because columns would lose tags unconfirmed (409 tag.move_loses): its message,
 * and each moved column's change, for the reader to confirm or carry. */
export class TagLossRefused extends Error {
  readonly columns: ColumnTagChange[];
  constructor(message: string, columns: ColumnTagChange[]) {
    super(message);
    this.columns = columns;
  }
}

/** A move's refusal: TagLossRefused for the tags it would take off columns, else its error. */
async function moveRefusal(res: Response, op: string): Promise<Error> {
  if (res.status !== 409) return mutationError(res, op);
  const data = await res.json();
  const message = serverMessage(data, requestFailed(op, res.status));
  return data.code === "tag.move_loses"
    ? new TagLossRefused(message, data.params.columns)
    : new Error(message);
}

/** Retire a term (REQ-2144): its columns and tables move to ``successor``, which a term holding
 * any must name. A move that takes tags off columns is refused (TagLossRefused) until each lost
 * tag is confirmed or the tags are carried to the successor. */
export async function retireGlossaryTerm(
  termId: number,
  body: { reason: string; successor: number | null } & TagLossResolution,
): Promise<{ status: string; tag_changes: ColumnTagChange[] }> {
  const res = await fetch(`${API_BASE}/admin/glossary/terms/${termId}/retire`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  if (!res.ok) throw await moveRefusal(res, "retireGlossaryTerm");
  return res.json();
}

/** Deprecate an approved term (REQ-2144): it is retired on ``retire_on``, if given. */
export async function deprecateGlossaryTerm(
  termId: number,
  body: { reason: string; retire_on: string | null },
): Promise<void> {
  const res = await fetch(`${API_BASE}/admin/glossary/terms/${termId}/deprecate`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  if (!res.ok) throw await mutationError(res, "deprecateGlossaryTerm");
}

/** What a retired term applied to (REQ-2144): its columns (table path and column) and tables at
 * the model commit before its retirement. None when the model's history holds no retirement of it
 * (404 glossary.no_retirement_history). */
export interface AppliedHistory {
  retired_in: string;
  as_of: string | null;
  columns: { table: string; column: string }[];
  tables: string[];
}

export async function fetchAppliedHistory(termId: number): Promise<AppliedHistory | null> {
  const res = await fetch(`${API_BASE}/admin/glossary/terms/${termId}/applied-history`);
  if (res.status === 404) {
    const data = await res.json();
    if (data.code === "glossary.no_retirement_history") return null;
    throw new Error(serverMessage(data, requestFailed("fetchAppliedHistory", res.status)));
  }
  if (!res.ok) throw await mutationError(res, "fetchAppliedHistory");
  return res.json();
}

/** Clear a term's retirement or a curator's deprecation. */
export async function restoreGlossaryTerm(termId: number): Promise<void> {
  const res = await fetch(`${API_BASE}/admin/glossary/terms/${termId}/restore`, {
    method: "POST",
  });
  if (!res.ok) throw await mutationError(res, "restoreGlossaryTerm");
}

export async function listTermTables(termId: number): Promise<GlossaryTermTable[]> {
  const res = await fetch(`${API_BASE}/admin/glossary/terms/${termId}/tables`);
  if (!res.ok) throw await mutationError(res, "listTermTables");
  return res.json();
}

export async function groundTermTable(termId: number, tableId: number): Promise<void> {
  const res = await fetch(`${API_BASE}/admin/glossary/terms/${termId}/tables`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ table_id: tableId }),
  });
  if (!res.ok) throw await mutationError(res, "groundTermTable");
}

export async function ungroundTermTable(termId: number, tableId: number): Promise<void> {
  const res = await fetch(`${API_BASE}/admin/glossary/terms/${termId}/tables/${tableId}`, {
    method: "DELETE",
  });
  if (!res.ok) throw await mutationError(res, "ungroundTermTable");
}

/** What a term is in the ontology, by what it is grounded in: a Class names a table's rows, a
 * Property a column's values, a Concept neither. A term grounded in both is a Class. */
export type TermRole = "class" | "property" | "concept";

export function termRole(t: { table_count: number; ref_count: number }): TermRole {
  if (t.table_count > 0) return "class";
  return t.ref_count > 0 ? "property" : "concept";
}

/** Approve a draft term (REQ-1387): it needs a definition. */
export async function approveGlossaryTerm(termId: number): Promise<void> {
  const res = await fetch(`${API_BASE}/admin/glossary/terms/${termId}/approve`, {
    method: "POST",
  });
  if (!res.ok) throw await mutationError(res, "approveGlossaryTerm");
}

export interface TermRef {
  term_id: number;
  name: string;
}

/** The defined, grounded drafts the caller may approve together (REQ-2144). */
export async function listApprovableTerms(): Promise<TermRef[]> {
  const res = await fetch(`${API_BASE}/admin/glossary/approvable`);
  if (!res.ok) throw await mutationError(res, "listApprovableTerms");
  return res.json();
}

export async function bulkApproveTerms(termIds: number[]): Promise<{ approved: number[] }> {
  const res = await fetch(`${API_BASE}/admin/glossary/approve`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ term_ids: termIds }),
  });
  if (!res.ok) throw await mutationError(res, "bulkApproveTerms");
  return res.json();
}

/** The steward actions whose impact is previewed (REQ-2144). */
export type ImpactAction = "approve" | "retire" | "bind";

/** What an action would change, changing nothing (REQ-2144). */
export interface TermImpact {
  action: ImpactAction;
  term_id: number;
  term: string;
  becomes_grounded: TermRef[];
  becomes_pending: TermRef[];
  becomes_not_grounded: TermRef[];
  tables_gained: number;
  tables_lost: number;
}

export async function previewTermImpact(
  termId: number,
  action: ImpactAction,
  extra: { successor?: number | null; table_id?: number; column_name?: string } = {},
): Promise<TermImpact> {
  const params = new URLSearchParams({ action });
  if (extra.successor != null) params.set("successor", String(extra.successor));
  if (extra.table_id != null) params.set("table_id", String(extra.table_id));
  if (extra.column_name != null) params.set("column_name", extra.column_name);
  const res = await fetch(`${API_BASE}/admin/glossary/terms/${termId}/impact?${params}`);
  if (!res.ok) throw await mutationError(res, "previewTermImpact");
  return res.json();
}

/** The stewardship summary (REQ-2144). */
export interface StewardshipSummary {
  agent_visible: (TermRef & { status: TermStatus })[];
  waiting_on_one_approval: (TermRef & { approve: TermRef[] })[];
  approved_not_grounded: TermRef[];
}

export async function fetchStewardshipSummary(): Promise<StewardshipSummary> {
  const res = await fetch(`${API_BASE}/admin/glossary/stewardship`);
  if (!res.ok) throw await mutationError(res, "fetchStewardshipSummary");
  return res.json();
}
