// Copyright (c) 2026 Kenneth Stott
// Canary: 0d7c5e2b-9a41-4f36-b8e3-71c4a2f9d508
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1591: a term's name is unique only within a domain, so where two terms of one name are
// listed together each is told apart by its domains.

type Named = { id: number; name: string; domains: string[] };

/** A term as a picker offers it: its label, and its plain name and domains for what is built
 * from the choice. */
export type TermOption = { value: string; label: string; name: string; domains: string[] };

/** ``shown`` as picker options, each labelled as it reads among ``shown``. */
export function termOptions(shown: Named[]): TermOption[] {
  return shown.map((term) => ({
    value: String(term.id),
    label: termLabel(term, shown),
    name: term.name,
    domains: term.domains,
  }));
}

/** A term's domains as the ontology schema states them, ``|a|b|``. */
export function pipedDomains(piped: string | null | undefined): string[] {
  return (piped ?? "").split("|").filter((d) => d !== "");
}

/** ``term``'s name, followed by its domains when another of ``terms`` shares the name. */
export function termLabel(term: Named, terms: Named[]): string {
  const shared = terms.some((o) => o.id !== term.id && o.name === term.name);
  if (!shared) return term.name;
  return `${term.name} (${term.domains.length > 0 ? term.domains.join(", ") : "—"})`;
}
