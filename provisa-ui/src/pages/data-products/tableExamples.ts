// Copyright (c) 2026 Kenneth Stott
// Canary: 3b7e1c94-52d8-4f06-a9c3-6e0d84f1b725
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1634, REQ-2158: one example per table on each Explore page, as a navigation: what a product's
// Access Patterns and a contract's How to use open. Every example is built from the names the acting
// role's schema gives the table, so a surface the role has no name on has no example.

import { rootKey, type MetaReader } from "../../api/metaGraph";
import { tableSqlRef } from "../../naming";

export interface TableRef {
  domainId: string;
  tableName: string;
  alias: string | null;
}

/** Where an example opens: the Explore page and the hand-off it reads. */
export interface ExampleLink {
  to: string;
  state: Record<string, unknown>;
}

/** A table's root query field in the acting role's schema (from /data/graph-schema). */
export function rootOf(reader: MetaReader | null, tb: TableRef): string | null {
  return reader?.roots.get(rootKey(tb.domainId, tb.tableName)) ?? null;
}

export const sqlExample = (tb: TableRef): ExampleLink => ({
  to: "/sql",
  state: { sql: `select * from ${tableSqlRef(tb)}`, autoRun: true },
});

export const jsonApiExample = (tb: TableRef): ExampleLink => ({
  to: "/jsonapi",
  state: { jsonapiUrl: `/data/jsonapi/${tb.domainId}/${tb.tableName}`, autoRun: true },
});

// By the root field and the columns the role's own schema gives it -- the compiler's names, never
// re-derived here.
export function graphqlExample(reader: MetaReader | null, tb: TableRef): ExampleLink | null {
  const field = rootOf(reader, tb);
  if (!field || !reader) return null;
  const type = reader.schema.rootType(field);
  const columns = type ? reader.schema.scalars(type) : [];
  if (columns.length === 0) return null;
  return {
    to: "/query",
    state: { query: `{ ${field} { ${columns.join(" ")} } }`, autoRun: true },
  };
}

export function restExample(reader: MetaReader | null, tb: TableRef): ExampleLink | null {
  const field = rootOf(reader, tb);
  return field
    ? { to: "/openapi", state: { openApiUrl: `GET /data/rest/${field}`, autoRun: true } }
    : null;
}

// Mirrors provisa.compiler.naming.to_type_name + provisa.grpc.proto_gen._to_proto_type_name — both
// pure functions of the field name alone. sa__userByName -> Sa__UserByName -> SaUserByName.
function grpcMethod(fieldName: string): string {
  let typeName: string;
  if (fieldName.includes("__")) {
    const [prefix, rest] = fieldName.split("__", 2);
    const prefixPascal = prefix ? prefix[0].toUpperCase() + prefix.slice(1).toLowerCase() : "";
    const restPascal = rest ? rest[0].toUpperCase() + rest.slice(1) : "";
    typeName = prefixPascal + restPascal;
  } else {
    typeName = fieldName[0].toUpperCase() + fieldName.slice(1);
  }
  return `Query${typeName}()`;
}

export function grpcExample(reader: MetaReader | null, tb: TableRef): ExampleLink | null {
  const field = rootOf(reader, tb);
  return field ? { to: "/grpc", state: { grpcMethod: grpcMethod(field), autoRun: true } } : null;
}
