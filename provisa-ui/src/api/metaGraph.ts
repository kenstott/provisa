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

export async function rootFields(role: string): Promise<RootFields> {
  const resp = await fetch("/data/graph-schema", { headers: { "X-Provisa-Role": role } });
  if (!resp.ok) throw new Error(`HTTP ${resp.status}: ${await resp.text()}`);
  const body = (await resp.json()) as {
    node_labels: {
      domain_id: string | null;
      table_name: string | null;
      graphql_field: string | null;
    }[];
  };
  const out: RootFields = new Map();
  for (const n of body.node_labels) {
    if (n.domain_id && n.table_name && n.graphql_field)
      out.set(rootKey(n.domain_id, n.table_name), n.graphql_field);
  }
  return out;
}

/** A relationship to follow: by its declared GraphQL alias, or (declared without one) by the
 * meta view it reaches. */
export type Hop = { alias: string } | { to: string };

/** What to read of a type: its columns, and the relationships to follow, each answered under
 * ``key``. */
export interface Selection {
  fields: string[];
  rels?: Record<string, Hop & { sel: Selection }>;
}

/** The meta domain as one role reads it: its schema and where each view is queried from. */
export interface MetaReader {
  schema: RoleSchema;
  roots: RootFields;
}

function viewType(m: MetaReader, view: string): string | null {
  const field = m.roots.get(rootKey("meta", view));
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
    const body = selectionSet(m, found.type, hop.sel, depth + 1);
    if (body) parts.push(`${key}: ${found.name} ${body}`);
  }
  if (parts.length === 0) return "";
  return `{\n${parts.map((p) => pad + p).join("\n")}\n${"  ".repeat(depth - 1)}}`;
}

/** A root query over the meta view ``view`` (``data_products``), answered under ``view``, with
 * an optional ``where``; null when the role cannot read the view. */
export function rootQuery(
  m: MetaReader,
  name: string,
  view: string,
  sel: Selection,
  where?: Record<string, { eq: unknown } | { in: unknown[] }>,
): string | null {
  const field = m.roots.get(rootKey("meta", view));
  const type = viewType(m, view);
  if (!field || !type) return null;
  const body = selectionSet(m, type, sel, 2);
  if (!body) return null;
  let args = "";
  if (where) {
    const conds: string[] = [];
    for (const [col, cond] of Object.entries(where)) {
      const name = m.schema.column(type, col);
      if (!name) return null;
      const [op, value] = Object.entries(cond)[0];
      conds.push(`${name}: { ${op}: ${JSON.stringify(value)} }`);
    }
    args = `(where: { ${conds.join(", ")} })`;
  }
  return `query ${name} {\n  ${view}: ${field}${args} ${body}\n}`;
}
