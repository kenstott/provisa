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
import { tableLabel as dbTableLabel, cypherRelType } from "../naming";
import type { Relationship } from "../types/admin";

export interface DropExpansion {
  /** The rewritten query, before any native-filter WHERE clause is spliced in. */
  query: string;
  /** Every variable bound to the dropped label, one per hop. */
  targetVars: string[];
}

export function buildDropExpansion(
  query: string,
  compoundLabel: string,
  adminRels: Relationship[],
  labelToTableLabel: Record<string, string>,
): DropExpansion {
  const droppedTableName = labelToTableLabel[compoundLabel] ?? compoundLabel;

  // Map each declared Cypher label → its query variable by parsing the MATCH
  // clauses. Use the QUERY (not result nodes): an OPTIONAL MATCH branch that
  // returned no rows still declares its label and must remain matchable.
  const varByLabel: Record<string, string> = {};
  for (const m of query.matchAll(/\(\s*(\w+)\s*:([\w:]+)\s*\)/g)) {
    const [, varName, labels] = m;
    labels.split(":").forEach((l) => {
      varByLabel[l] = varName;
    });
  }

  // Find a relationship whose one endpoint is the dropped table and whose other
  // endpoint is a label already declared in the query. Comparison is exact: the
  // dropped label and dbTableLabel(table_name) are both produced by the same
  // label-derivation function on registered_tables.table_name.
  // REQ-1586: collect EVERY relationship type that reaches the dropped table from a label the
  // query already declares, not just the first. One junction table backs several types at once
  // (a discriminator column names each edge), so stopping at the first match would draw KIND_OF
  // and quietly hide RELATED_TO between the very same two tables.
  let sourceVar: string | undefined;
  const relAliases: string[] = [];
  for (const r of adminRels) {
    if (r.disableCypher) continue;
    const srcLabel = dbTableLabel(r.sourceTableName);
    const tgtLabel = r.targetTableName ? dbTableLabel(r.targetTableName) : null;
    // dropped node is the relationship target; existing query node is the source
    const asTarget = tgtLabel === droppedTableName && varByLabel[srcLabel];
    // dropped node is the relationship source; existing query node is the target
    const asSource = srcLabel === droppedTableName && tgtLabel && varByLabel[tgtLabel];
    if (!asTarget && !asSource) continue;
    const anchor = asTarget ? varByLabel[srcLabel] : varByLabel[tgtLabel as string];
    // Every hop hangs off one anchor variable — the first one found. A second relationship
    // reaching the dropped table from a DIFFERENT existing node is a different question.
    if (sourceVar === undefined) sourceVar = anchor;
    else if (anchor !== sourceVar) continue;
    const relType = cypherRelType(r);
    if (relType && !relAliases.includes(relType)) relAliases.push(relType);
  }
  if (!sourceVar) {
    // No known relationship — fall back to first MATCH variable
    const nodeVarMatch = query.match(/\bMATCH\s*\(\s*(\w+)/i);
    sourceVar = nodeVarMatch?.[1] ?? "n";
  }

  const suffix = droppedTableName.replace(/[^a-zA-Z0-9]/g, "").slice(0, 12);
  const trimmed = query.replace(/\s+LIMIT\s+\d+\s*$/i, "").trim();
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
  const returnMatches = [...trimmed.matchAll(/\bRETURN\b/gi)];
  const lastReturn = returnMatches.pop();
  let newQueryBase: string;
  if (!lastReturn || lastReturn.index === undefined) {
    newQueryBase = `${trimmed}\n${optMatchBlock}\nRETURN ${sourceVar}${extraReturn}`;
  } else {
    const beforeReturn = trimmed.slice(0, lastReturn.index).trimEnd();
    const returnClause = trimmed.slice(lastReturn.index + 6).trim();
    newQueryBase = `${beforeReturn}\n${optMatchBlock}\nRETURN ${returnClause}${extraReturn}`;
  }
  return { query: newQueryBase, targetVars: pairs.map((p) => p.targetVar) };
}
