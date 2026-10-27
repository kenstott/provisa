// Copyright (c) 2026 Kenneth Stott
// Canary: d4e7a1c9-58b2-4f30-a6e1-9c3b7d2f5a18
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2125, REQ-2127, REQ-2128: meta-domain queries run through the governed pipeline as the
// signed-in user. The role's GraphQL schema holds only what the role may read, so each query is
// built from it: a view, column or relationship the role cannot read is not selected, and what it
// would have filled is absent. Each field is found in that schema by its snake_case name and
// aliased to it, so the answer's keys are the view's own column names whatever the schema spells.

export interface GraphqlType {
  kind: string;
  name: string | null;
  ofType?: GraphqlType | null;
}

interface IntrospectedType {
  name: string;
  fields: { name: string; type: GraphqlType }[] | null;
}

export interface Introspection {
  __schema: { queryType: { name: string }; types: IntrospectedType[] };
}

export const INTROSPECTION_QUERY = `query RoleSchema {
  __schema {
    queryType { name }
    types { name fields { name type { kind name ofType { kind name ofType { kind name ofType { kind name } } } } } }
  }
}`;

/** A one-to-many field as answered: undefined when the role cannot select it, else its rows. The
 * schema types the list as nullable and the server answers one with no rows with null (json_agg
 * over nothing), so null here means none. */
export function rowsOf<T>(v: T[] | null | undefined): T[] | undefined {
  return v === undefined ? undefined : (v ?? []);
}

/** POST a GraphQL query to /data/graphql as ``role``. A refusal or a GraphQL error is thrown. */
export async function dataGraphql<T>(
  query: string,
  variables: Record<string, unknown>,
  role: string,
): Promise<T> {
  const resp = await fetch("/data/graphql", {
    method: "POST",
    headers: { "Content-Type": "application/json", "X-Provisa-Role": role },
    body: JSON.stringify({ query, variables }),
  });
  if (!resp.ok) throw new Error(`HTTP ${resp.status}: ${await resp.text()}`);
  const body = (await resp.json()) as { data?: T; errors?: { message: string }[] };
  if (body.errors && body.errors.length > 0) {
    throw new Error(body.errors.map((e) => e.message).join("; "));
  }
  return body.data as T;
}

const norm = (name: string) => name.replace(/_/g, "").toLowerCase();

function named(t: GraphqlType): string | null {
  let cur: GraphqlType | null | undefined = t;
  while (cur && cur.name === null) cur = cur.ofType;
  return cur?.name ?? null;
}

/** A role's schema: which fields each type has, and the type each field reaches. */
export class RoleSchema {
  private types = new Map<string, { name: string; type: string | null }[]>();
  readonly query: string;

  constructor(raw: Introspection) {
    this.query = raw.__schema.queryType.name;
    for (const t of raw.__schema.types) {
      if (!t.fields) continue;
      this.types.set(
        t.name,
        t.fields.map((f) => ({ name: f.name, type: named(f.type) })),
      );
    }
  }

  /** A column of ``type`` by its view's column name (``dq_contract``), as the schema spells it;
   * null when the role cannot read it. */
  column(type: string, column: string): string | null {
    const want = norm(column);
    return this.types.get(type)?.find((f) => norm(f.name) === want)?.name ?? null;
  }

  /** A relationship of ``type`` by its snake_case name (``edges_out``), as the schema spells it,
   * and the type it reaches; null when ``type`` has no such relationship the role can read. */
  relationship(type: string, name: string): { name: string; type: string } | null {
    const want = norm(name);
    const f = this.types.get(type)?.find((x) => norm(x.name) === want);
    return f?.type && this.types.has(f.type) ? { name: f.name, type: f.type } : null;
  }

  /** The field ``name`` of ``type`` exactly (a root field, or a relationship's declared alias),
   * and the type it reaches; null when the role's schema has no such field. */
  exact(type: string, name: string): { name: string; type: string | null } | null {
    return this.types.get(type)?.find((f) => f.name === name) ?? null;
  }

  /** The type a root query field answers with; null when the role has no such field. */
  rootType(field: string): string | null {
    return this.exact(this.query, field)?.type ?? null;
  }

  /** The scalar fields of ``type`` the role reads (its columns), without the system fields. */
  scalars(type: string): string[] {
    return (this.types.get(type) ?? [])
      .filter((f) => f.type !== null && !this.types.has(f.type) && !f.name.startsWith("_"))
      .map((f) => f.name);
  }

  /** The one field of ``type`` reaching ``target``: a relationship declared without an alias. */
  reaching(type: string, target: string): { name: string; type: string | null } | null {
    const found = this.types.get(type)?.filter((f) => f.type === target) ?? [];
    return found.length === 1 ? found[0] : null;
  }
}

/** Where each meta view is queried from: its root field in the role's GraphQL schema, read from
 * /data/graph-schema (the compiler's own names; never derived here). */
export type RootFields = Map<string, string>;

export const rootKey = (domain: string, table: string) => `${domain}.${table}`;

/** The system schema a view is the table of: the glossary's definition views (``_terms``,
 * ``_term_refs``, ...) are the ontology schema's, named with a leading underscore so they never
 * meet a generated Class table; every other view is meta's. */
export const viewDomain = (view: string) => (view.startsWith("_") ? "ontology" : "meta");

export async function rootFields(role: string): Promise<RootFields> {
  const resp = await fetch("/data/graph-schema", { headers: { "X-Provisa-Role": role } });
  if (!resp.ok) throw new Error(`HTTP ${resp.status}: ${await resp.text()}`);
  const body = (await resp.json()) as {
    // Every table the role has a root field for, junctions included (they are no node label).
    root_fields: { domain_id: string; table_name: string; graphql_field: string }[];
  };
  return new Map(
    body.root_fields.map((r) => [rootKey(r.domain_id, r.table_name), r.graphql_field]),
  );
}

/** A relationship to follow: by its declared GraphQL alias, or (declared without one) by the
 * meta view it reaches. */
export type Hop = { alias: string } | { to: string };

/** One column's condition, as the where input spells it (provisa/compiler/type_map.py). */
export type Cond =
  | { eq: unknown }
  | { neq: unknown }
  | { in: unknown[] }
  | { like: string }
  | { ilike: string }
  | { is_null: boolean };

/** A where: every column condition holds, each of ``_and`` and any of ``_or``, and for each
 * relationship named, a related row matching its own where. */
export type Where = { _and?: Where[]; _or?: Where[] } & {
  [columnOrRelationship: string]: Cond | Where | Where[];
};

/** What to read of a type: its columns, and the relationships to follow, each answered under
 * ``key``; a one-to-many relationship can be narrowed by its own ``where``. */
export interface Selection {
  fields: string[];
  rels?: Record<string, Hop & { sel: Selection; where?: Where }>;
}

/** The meta domain as one role reads it: its schema and where each view is queried from. */
export interface MetaReader {
  schema: RoleSchema;
  roots: RootFields;
}

function viewType(m: MetaReader, view: string): string | null {
  const field = m.roots.get(rootKey(viewDomain(view), view));
  if (!field) return null;
  return m.schema.exact(m.schema.query, field)?.type ?? null;
}

/** The selection set of ``sel`` on ``type``, with only what the role can read: one selection a
 * line, nested ``depth`` deep, so the query reads as it is shown. */
export function selectionSet(m: MetaReader, type: string, sel: Selection, depth = 1): string {
  const pad = "  ".repeat(depth);
  const parts: string[] = [];
  for (const f of sel.fields) {
    const name = m.schema.column(type, f);
    if (name) parts.push(`${f}: ${name}`);
  }
  for (const [key, hop] of Object.entries(sel.rels ?? {})) {
    const target = "to" in hop ? viewType(m, hop.to) : null;
    const found =
      "alias" in hop
        ? m.schema.exact(type, hop.alias)
        : target
          ? m.schema.reaching(type, target)
          : null;
    if (!found?.type) continue;
    // A narrowing the role cannot state (a column it cannot read) leaves the relationship out
    // rather than read wider than asked.
    const args = hop.where ? whereArg(m, found.type, hop.where) : "";
    if (args === null) continue;
    const body = selectionSet(m, found.type, hop.sel, depth + 1);
    if (body) parts.push(`${key}: ${found.name}${args} ${body}`);
  }
  if (parts.length === 0) return "";
  return `{\n${parts.map((p) => pad + p).join("\n")}\n${"  ".repeat(depth - 1)}}`;
}

/** ``where`` on ``type`` as GraphQL input text, each column as the role's schema spells it; null
 * when a column it names is not the role's to read. */
export function whereText(m: MetaReader, type: string, where: Where): string | null {
  const conds: string[] = [];
  for (const [key, cond] of Object.entries(where)) {
    if (key === "_and" || key === "_or") {
      const subs = (cond as Where[]).map((w) => whereText(m, type, w));
      if (subs.some((x) => x === null)) return null;
      conds.push(`${key}: [${subs.join(", ")}]`);
      continue;
    }
    // A relationship's condition: a related row matching it (REQ-2136), on the type it reaches.
    const rel = m.schema.relationship(type, key);
    if (rel) {
      const sub = whereText(m, rel.type, cond as Where);
      if (sub === null) return null;
      conds.push(`${rel.name}: ${sub}`);
      continue;
    }
    const name = m.schema.column(type, key);
    if (!name) return null;
    const [op, value] = Object.entries(cond as Cond)[0];
    conds.push(`${name}: { ${op}: ${JSON.stringify(value)} }`);
  }
  return `{ ${conds.join(", ")} }`;
}

function whereArg(m: MetaReader, type: string, where: Where): string | null {
  const text = whereText(m, type, where);
  return text === null ? null : `(where: ${text})`;
}

/** One root field over the meta or ontology view ``view``, answered under ``view``, with an
 * optional ``where``; null when the role cannot read the view or a column the where names. */
export function rootField(
  m: MetaReader,
  view: string,
  sel: Selection,
  where?: Where,
): string | null {
  const field = m.roots.get(rootKey(viewDomain(view), view));
  const type = viewType(m, view);
  if (!field || !type) return null;
  const body = selectionSet(m, type, sel, 2);
  if (!body) return null;
  const args = where ? whereArg(m, type, where) : "";
  if (args === null) return null;
  return `${view}: ${field}${args} ${body}`;
}

/** One query operation reading each of ``fields`` (rootField's), so a view is one query. */
export const operation = (name: string, fields: string[]) =>
  `query ${name} {\n${fields.map((f) => `  ${f}`).join("\n")}\n}`;

/** A root query over the meta view ``view`` (``data_products``), answered under ``view``, with
 * an optional ``where``; null when the role cannot read the view or a column the where names. */
export function rootQuery(
  m: MetaReader,
  name: string,
  view: string,
  sel: Selection,
  where?: Where,
): string | null {
  const field = rootField(m, view, sel, where);
  return field === null ? null : operation(name, [field]);
}
