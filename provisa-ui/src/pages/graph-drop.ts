// Copyright (c) 2026 Kenneth Stott
// Canary: 54aa4032-3f65-4159-9eb5-4e639403b22a
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

/**
 * REQ-1586: the query a pill dropped onto a graph frame expands into.
 *
 * Dropping a table asks for its instances as they connect to what is already on the surface, so the
 * expansion has to name every relationship type that reaches it — not one of them. A junction table
 * backs several types at once (its discriminator column names each edge), which is exactly the case
 * where answering with the first type found would draw one kind of edge and hide the rest.
 *
 * Kept out of the page component so the pattern it writes can be read back in a test.
 */
import type { SchemaRel } from "../components/graph/graph-schema-types";

export interface DropExpansion {
  /** The rewritten query, before any native-filter WHERE clause is spliced in. */
  query: string;
  /** Every variable bound to the dropped label, one per hop. */
  targetVars: string[];
}

/**
 * ``query`` with ``matchBlock`` (its OPTIONAL MATCH lines) before its last RETURN and
 * ``extraReturn`` (", var, ...") added to that RETURN's projection. The result's own ORDER BY,
 * SKIP and LIMIT stay where Cypher puts them, after the projection: the expansion widens the
 * pattern, never the paging the query asked for. A query with no RETURN returns ``projection``.
 */
export function withExpansion(
  query: string,
  matchBlock: string,
  extraReturn: string,
  projection: string,
): string {
  const q = query.trim();
  const last = [...q.matchAll(/\bRETURN\b/gi)].pop();
  if (!last || last.index === undefined) return `${q}\n${matchBlock}\nRETURN ${projection}`;
  const before = q.slice(0, last.index).trimEnd();
  const clause = q.slice(last.index + "RETURN".length).trim();
  // The projection ends where the paging begins: ORDER BY, SKIP or LIMIT as a clause, preceded by
  // whitespace (a property spelled n.limit is not one).
  const paging = clause.search(/\s(ORDER\s+BY|SKIP|LIMIT)\s/i);
  const own = paging < 0 ? clause : clause.slice(0, paging).trimEnd();
  const tail = paging < 0 ? "" : `\n${clause.slice(paging).trim()}`;
  return `${before}\n${matchBlock}\nRETURN ${own}${extraReturn}${tail}`;
}

export function buildDropExpansion(
  query: string,
  compoundLabel: string,
  schemaRels: SchemaRel[],
): DropExpansion {
  // The table part of the dropped label (Shelter:Assignments -> Assignments), for variable names.
  const droppedTableName = compoundLabel.split(":").pop() ?? compoundLabel;

  // Map each label the query declares, as written (a compound label whole), to its variable, by
  // parsing the MATCH clauses. Use the QUERY (not result nodes): an OPTIONAL MATCH branch that
  // returned no rows still declares its label and must remain matchable.
  const varByLabel: Record<string, string> = {};
  for (const m of query.matchAll(/\(\s*(\w+)\s*:([\w:]+)\s*\)/g)) {
    const [, varName, labels] = m;
    varByLabel[labels] = varName;
  }

  // The relationship types are the server's, from the acting role's label map
  // (/data/graph-schema, the sidebar's Relationship Types): only a type it registers between the
  // dropped label and one the query declares is written, so the translator never meets a type it
  // does not know (REQ-1586). Every type that reaches the dropped label from that node is
  // collected, not just the first: one junction table backs several types between the same two
  // labels, and stopping at the first would hide the rest.
  let sourceVar: string | undefined;
  const relAliases: string[] = [];
  for (const r of schemaRels) {
    // dropped node is the relationship target; existing query node is the source
    const asTarget = r.target === compoundLabel && varByLabel[r.source];
    // dropped node is the relationship source; existing query node is the target
    const asSource = r.source === compoundLabel && varByLabel[r.target];
    if (!asTarget && !asSource) continue;
    const anchor = asTarget ? varByLabel[r.source] : varByLabel[r.target];
    // Every hop hangs off one anchor variable — the first one found. A type reaching the dropped
    // label from a DIFFERENT existing node is a different question.
    if (sourceVar === undefined) sourceVar = anchor;
    else if (anchor !== sourceVar) continue;
    if (!relAliases.includes(r.type)) relAliases.push(r.type);
  }
  if (!sourceVar) {
    // No known relationship — fall back to first MATCH variable
    const nodeVarMatch = query.match(/\bMATCH\s*\(\s*(\w+)/i);
    sourceVar = nodeVarMatch?.[1] ?? "n";
  }

  const suffix = droppedTableName.replace(/[^a-zA-Z0-9]/g, "").slice(0, 12);
  const trimmed = query.trim();
  const taken = (v: string) =>
    trimmed.includes(`[${v}`) || trimmed.includes(` ${v}`) || trimmed.includes(`(${v}`);
  // One free (rel, target) variable pair per type. Each type gets its own OPTIONAL MATCH: a
  // single-hop pattern resolves one relationship mapping, so alternation is not a spelling the
  // translator accepts.
  const pairs: { relVar: string; targetVar: string }[] = [];
  let counter = 1;
  for (let k = 0; k < Math.max(relAliases.length, 1); k++) {
    let relVar = `r${suffix}`;
    let targetVar = `m${suffix}`;
    while (
      taken(relVar) ||
      taken(targetVar) ||
      pairs.some((p) => p.relVar === relVar || p.targetVar === targetVar)
    ) {
      counter++;
      relVar = `r${suffix}${counter}`;
      targetVar = `m${suffix}${counter}`;
    }
    pairs.push({ relVar, targetVar });
  }
  const optMatchPatterns =
    relAliases.length > 0
      ? relAliases.map(
          (a, k) =>
            `(${sourceVar})-[${pairs[k].relVar}:${a}]-(${pairs[k].targetVar}:${compoundLabel})`,
        )
      : [`(${pairs[0].targetVar}:${compoundLabel})`];
  const extraReturn =
    relAliases.length > 0
      ? relAliases.map((_, k) => `, ${pairs[k].relVar}, ${pairs[k].targetVar}`).join("")
      : `, ${pairs[0].targetVar}`;
  const optMatchBlock = optMatchPatterns.map((p) => `OPTIONAL MATCH ${p}`).join("\n");
  const newQueryBase = withExpansion(
    trimmed,
    optMatchBlock,
    extraReturn,
    `${sourceVar}${extraReturn}`,
  );
  return { query: newQueryBase, targetVars: pairs.map((p) => p.targetVar) };
}
