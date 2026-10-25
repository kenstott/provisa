// Copyright (c) 2026 Kenneth Stott
// Canary: 58323c9c-1cfb-441b-8c94-cee9cf226d27
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// A term's edit form, as a draft: every field the curator can change, held until Save. Save is the
// list of changes between the term as read and the draft, made through the glossary's own calls
// in a fixed order; Cancel drops the draft. A change the server refuses stops the rest, so what
// was made before it is kept and the draft still holds what was not.

import {
  addGlossaryEdge,
  addGlossaryExpert,
  groundTermTable,
  moveGlossaryRef,
  removeGlossaryEdge,
  removeGlossaryExpert,
  retypeGlossaryEdge,
  ungroundTermTable,
  updateGlossaryTerm,
  type GlossaryEdge,
  type TagLossResolution,
  type GlossaryExpert,
  type GlossaryTermDetail,
} from "../../api/glossary";
import { columnKey, type ColumnTerm } from "./claimColumns";

/** A column claimed for the term: its ref moves here on Save. */
export interface Claim {
  table_id: number;
  column_name: string;
  /** The term the column is now, if any. */
  from: ColumnTerm | null;
}

export interface TermDraft {
  name: string;
  definition: string;
  exportExcluded: boolean;
  /** REQ-2145: pii on the term, which every column it holds carries (a Property's only). */
  pii: boolean;
  /** Declared domains: a concept's only; a rooted term's come from its columns. */
  domains: string[];
  /** A column of the term moved to another term: its key, and the term it moves to. */
  moves: Record<string, number>;
  claims: Claim[];
  /** The tables the term is grounded in. */
  tables: number[];
  edgesOut: GlossaryEdge[];
  edgesIn: GlossaryEdge[];
  experts: GlossaryExpert[];
}

export function draftOf(term: GlossaryTermDetail, tables: number[]): TermDraft {
  return {
    name: term.name,
    definition: term.definition ?? "",
    exportExcluded: term.export_excluded,
    pii: term.tags.some((g) => g.tag_id === "pii"),
    domains: [...term.domains],
    moves: {},
    claims: [],
    tables: [...tables],
    edgesOut: [...term.edges_out],
    edgesIn: [...term.edges_in],
    experts: [...term.experts],
  };
}

/** One change Save makes, named by the field it changes so a refusal is said beside it. A column's
 * move names the term it moves to, and goes ahead over the tags it takes off (REQ-2145) as the
 * reader resolves them. */
export interface Change {
  field: "fields" | "columns" | "tables" | "relationships" | "experts";
  run: (resolve?: TagLossResolution) => Promise<{ termRemoved?: boolean } | void>;
  moveTo?: number;
}

const edgeKey = (from: number, to: number, rel: string) => `${from}>${to}:${rel}`;

/** Marking and unmarking a tag on the term (REQ-2145): the tag picker's own mutations. */
export interface TermTags {
  mark: (tagId: string) => Promise<void>;
  unmark: (tagId: string) => Promise<void>;
}

/** What Save does to make ``term`` (grounded in ``tables``) the draft, in order. */
export function changes(
  term: GlossaryTermDetail,
  tables: number[],
  draft: TermDraft,
  termTags: TermTags,
): Change[] {
  const out: Change[] = [];
  const patch: Parameters<typeof updateGlossaryTerm>[1] = {};
  if (draft.name.trim() !== term.name) patch.name = draft.name.trim();
  if (draft.definition !== (term.definition ?? "")) patch.definition = draft.definition;
  if (draft.exportExcluded !== term.export_excluded) patch.export_excluded = draft.exportExcluded;
  const sameDomains =
    draft.domains.length === term.domains.length &&
    draft.domains.every((d) => term.domains.includes(d));
  if (term.is_abstract && !sameDomains) patch.domains = draft.domains;
  if (Object.keys(patch).length > 0)
    out.push({ field: "fields", run: () => updateGlossaryTerm(term.id, patch) });
  // After the definition: a term is marked only once it is defined.
  const pii = term.tags.some((g) => g.tag_id === "pii");
  if (draft.pii !== pii)
    out.push({
      field: "fields",
      run: () => (draft.pii ? termTags.mark("pii") : termTags.unmark("pii")),
    });

  for (const c of draft.claims)
    out.push({
      field: "columns",
      moveTo: term.id,
      run: async (resolve) => {
        await moveGlossaryRef({
          table_id: c.table_id,
          column_name: c.column_name,
          to_term_id: term.id,
          ...resolve,
        });
      },
    });
  for (const ref of term.refs) {
    const to = draft.moves[columnKey(ref.table_id, ref.column_name)];
    if (to === undefined) continue;
    out.push({
      field: "columns",
      moveTo: to,
      run: async (resolve) => {
        const moved = await moveGlossaryRef({
          table_id: ref.table_id,
          column_name: ref.column_name,
          to_term_id: to,
          ...resolve,
        });
        return { termRemoved: moved.source_term_removed };
      },
    });
  }

  for (const id of draft.tables)
    if (!tables.includes(id))
      out.push({ field: "tables", run: () => groundTermTable(term.id, id) });
  for (const id of tables)
    if (!draft.tables.includes(id))
      out.push({ field: "tables", run: () => ungroundTermTable(term.id, id) });

  const edges = (outgoing: GlossaryEdge[], incoming: GlossaryEdge[]) =>
    new Map([
      ...outgoing.map(
        (e) => [edgeKey(term.id, e.term_id, e.rel_type), [term.id, e.term_id, e.rel_type]] as const,
      ),
      ...incoming.map(
        (e) => [edgeKey(e.term_id, term.id, e.rel_type), [e.term_id, term.id, e.rel_type]] as const,
      ),
    ]);
  const was = edges(term.edges_out, term.edges_in);
  const now = edges(draft.edgesOut, draft.edgesIn);
  const gone = [...was].filter(([k]) => !now.has(k)).map(([, e]) => e);
  const added = [...now].filter(([k]) => !was.has(k)).map(([, e]) => e);
  // An edge between the same two terms whose type changed is retyped, not removed and re-added.
  for (const [from, to, rel] of gone) {
    const i = added.findIndex(([f, t]) => f === from && t === to);
    if (i >= 0) {
      const next = added.splice(i, 1)[0][2];
      out.push({ field: "relationships", run: () => retypeGlossaryEdge(from, to, rel, next) });
    } else out.push({ field: "relationships", run: () => removeGlossaryEdge(from, to, rel) });
  }
  for (const [from, to, rel] of added)
    out.push({ field: "relationships", run: () => addGlossaryEdge(from, to, rel) });

  for (const e of term.experts)
    if (!draft.experts.some((d) => d.user_id === e.user_id && d.kind === e.kind))
      out.push({ field: "experts", run: () => removeGlossaryExpert(term.id, e.user_id) });
  for (const e of draft.experts)
    if (!term.experts.some((d) => d.user_id === e.user_id && d.kind === e.kind))
      out.push({ field: "experts", run: () => addGlossaryExpert(term.id, e.user_id, e.kind) });
  return out;
}
