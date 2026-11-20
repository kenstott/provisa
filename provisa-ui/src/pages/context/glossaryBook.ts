// Copyright (c) 2026 Kenneth Stott
// Canary: 6e1d9b42-8c3a-4f57-a2e0-3b7c5d9f1a86
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2259: the Glossary page's book -- entries grouped by name, each with its senses, filtered,
// searched and sorted. Kept apart from the page so the rules are plain functions.

import { ENTERPRISE_DOMAIN, type GlossaryEntries } from "../../api/glossary";

export interface TermRef {
  id: number;
  name: string;
}

/** One meaning of an entry: one approved term, labelled by its domains. */
export interface GlossarySense {
  id: number;
  definition: string | null;
  domains: string[];
  changedAt: string;
  alsoCalled: TermRef[];
  kindOf: TermRef[];
  kinds: TermRef[];
  seeAlso: TermRef[];
}

/** One headword: the terms of one name (told apart by domain, REQ-1591) as numbered senses. */
export interface GlossaryEntry {
  key: string;
  name: string;
  letter: string;
  senses: GlossarySense[];
}

export type GlossarySort = "name" | "recent";

/** The letters of the letter bar; a name starting with anything else files under "#". */
export const LETTERS = [..."ABCDEFGHIJKLMNOPQRSTUVWXYZ", "#"];

export function letterOf(name: string): string {
  const first = name.trim().charAt(0).toUpperCase(); // naming-ok: the letter a name files under, not a name
  return first >= "A" && first <= "Z" ? first : "#";
}

const byName = (a: TermRef, b: TermRef) => a.name.localeCompare(b.name);

/** The entries of ``data``, A to Z: terms of one name (case aside) are one entry. */
export function buildEntries(data: GlossaryEntries): GlossaryEntry[] {
  const names = new Map(data.terms.map((t) => [t.id, t.name]));
  const ref = (id: number): TermRef => ({ id, name: names.get(id) as string });
  const related = new Map<
    number,
    Omit<GlossarySense, "id" | "definition" | "domains" | "changedAt">
  >();
  for (const t of data.terms)
    related.set(t.id, { alsoCalled: [], kindOf: [], kinds: [], seeAlso: [] });
  for (const e of data.edges) {
    const from = related.get(e.from_term_id);
    const to = related.get(e.to_term_id);
    // The server sends only edges between two terms it sends (glossary_entries).
    if (from === undefined || to === undefined)
      throw new Error(`edge to a term not sent: ${JSON.stringify(e)}`);
    if (e.rel_type === "KIND_OF") {
      from.kindOf.push(ref(e.to_term_id));
      to.kinds.push(ref(e.from_term_id));
    } else if (e.rel_type === "RELATED_TO") {
      from.seeAlso.push(ref(e.to_term_id));
      to.seeAlso.push(ref(e.from_term_id));
    } else {
      from.alsoCalled.push(ref(e.to_term_id));
      to.alsoCalled.push(ref(e.from_term_id));
    }
  }
  const entries = new Map<string, GlossaryEntry>();
  for (const t of data.terms) {
    const key = t.name.trim().toLowerCase();
    let entry = entries.get(key);
    if (entry === undefined) {
      entry = { key, name: t.name, letter: letterOf(t.name), senses: [] };
      entries.set(key, entry);
    }
    const rel = related.get(t.id)!;
    entry.senses.push({
      id: t.id,
      definition: t.definition,
      domains: t.domains,
      changedAt: t.changed_at,
      alsoCalled: rel.alsoCalled.sort(byName),
      kindOf: rel.kindOf.sort(byName),
      kinds: rel.kinds.sort(byName),
      seeAlso: rel.seeAlso.sort(byName),
    });
  }
  for (const entry of entries.values()) {
    entry.senses.sort((a, b) => a.domains.join(",").localeCompare(b.domains.join(",")));
  }
  return [...entries.values()].sort((a, b) => a.name.localeCompare(b.name));
}

/** The domains the entries' senses name, for the domain filter; the enterprise scope is in every
 * domain, so it is no choice of its own. */
export function domainsOf(entries: GlossaryEntry[]): string[] {
  const all = new Set<string>();
  for (const e of entries) for (const s of e.senses) for (const d of s.domains) all.add(d);
  all.delete(ENTERPRISE_DOMAIN);
  return [...all].sort();
}

function senseInDomains(sense: GlossarySense, chosen: Set<string>): boolean {
  return sense.domains.some((d) => d === ENTERPRISE_DOMAIN || chosen.has(d));
}

function matches(entry: GlossaryEntry, sense: GlossarySense, needle: string): boolean {
  if (entry.name.toLowerCase().includes(needle)) return true;
  if ((sense.definition ?? "").toLowerCase().includes(needle)) return true;
  return sense.alsoCalled.some((r) => r.name.toLowerCase().includes(needle));
}

/** ``entries`` narrowed to the senses in ``domains`` (none chosen: every sense) whose name, other
 * names or definition hold ``search``; an entry left with no sense is dropped. */
export function filterEntries(
  entries: GlossaryEntry[],
  search: string,
  domains: string[],
): GlossaryEntry[] {
  const needle = search.trim().toLowerCase();
  const chosen = new Set(domains);
  const out: GlossaryEntry[] = [];
  for (const entry of entries) {
    const senses = entry.senses.filter(
      (s) =>
        (chosen.size === 0 || senseInDomains(s, chosen)) &&
        (needle === "" || matches(entry, s, needle)),
    );
    if (senses.length > 0) out.push({ ...entry, senses });
  }
  return out;
}

/** When an entry last changed (ms since the epoch): its most recently changed sense. The server
 * sends every term's time (glossary_entries); a sense without one is an error, not sorted by name. */
export function changedAt(entry: GlossaryEntry): number {
  return Math.max(
    ...entry.senses.map((s) => {
      const at = Date.parse(s.changedAt);
      if (Number.isNaN(at)) throw new Error(`term ${s.id} has no changed_at: ${s.changedAt}`);
      return at;
    }),
  );
}

/** A to Z, or newest change first with the name ordering only entries changed at the same time. */
export function sortEntries(entries: GlossaryEntry[], sort: GlossarySort): GlossaryEntry[] {
  if (sort === "name") return [...entries].sort((a, b) => a.name.localeCompare(b.name));
  return [...entries].sort(
    (a, b) => changedAt(b) - changedAt(a) || a.name.localeCompare(b.name),
  );
}

/** The anchor id of the entry holding term ``id``. */
export function senseAnchor(id: number): string {
  return `glossary-term-${id}`;
}

export function letterAnchor(letter: string): string {
  return `glossary-letter-${letter === "#" ? "other" : letter}`;
}
