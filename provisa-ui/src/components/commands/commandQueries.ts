// Copyright (c) 2026 Kenneth Stott
// Canary: 8d3f1a67-2c94-4e5b-b071-9a6e4c2d8f13
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1159 (amended 2026-11-27): a command's call on each surface it is served on, as an Explore
// hand-off with its arguments filled in. SQL calls it in FROM by its SQL name, GraphQL by the field
// the role's schema gives it, Cypher with a CALL of its Cypher name, REST at its command path (a
// read a GET with each argument a query parameter, a write a POST with them as the body), and gRPC
// with its call's name and arguments. A relation argument is the one value the argument takes on
// every surface: a table reference, the text of a query, or rows (their JSON text where a surface
// takes a string, the array itself where it takes JSON). A read runs on opening; a write only when
// Run is pressed. SPARQL has no call of a command, and the JSON:API Explore page reads tables only,
// so neither is offered.

import type { ActionArg } from "../../api/actions";
import type { MetaReader } from "../../api/metaGraph";
import type { QueryLink, RelationshipSurface } from "../relationships/relationshipQueries";

/** A command the role's GraphQL schema offers, as /data/graph-schema lists it. */
export interface ServedCommand {
  command: string;
  domain_id: string;
  /** The root type its field is on: the schema's query type for a read, mutation for a write. */
  graphql_root: string;
  graphql_field: string;
  /** Its name in each language a call names it by; null where it is excluded (REQ-2168). */
  names: { sql: string | null; cypher: string | null; rest: string | null };
}

/** The surfaces a command is called on, in the order a table's links are drawn. */
export const COMMAND_SURFACES = ["cypher", "sql", "graphql", "rest", "grpc"] as const;
export type CommandSurface = (typeof COMMAND_SURFACES)[number] & RelationshipSurface;

/** A command as its call is written: its registered name, its kind and its arguments. */
export interface CommandCall {
  name: string;
  kind: string;
  arguments: ActionArg[];
}

/** One argument's value as the argument form gives it: a scalar typed as the argument is, or a
 *  relation as its one value (``text``) and, given as rows, the rows themselves. */
export type ArgValue =
  | { kind: "scalar"; value: unknown }
  | { kind: "relation"; text: string; rows: Record<string, unknown>[] | null };

/** A relation argument: one that takes a table (REQ-885 arg kinds table_ref and result_set). */
export const isRelationArg = (a: ActionArg): boolean =>
  a.argKind === "table_ref" || a.argKind === "result_set";

/** ``name``'s entry among the commands the role is served; null when it is not served one. */
export function servedCommand(
  served: readonly ServedCommand[] | null,
  name: string,
): ServedCommand | null {
  return served?.find((c) => c.command === name) ?? null;
}

/** The surfaces ``served`` is called on; none when the role is not served the command. GraphQL
 *  needs the role's schema to select what the command answers. */
export function commandSurfaces(
  served: ServedCommand | null,
  reader: MetaReader | null,
): CommandSurface[] {
  if (served === null) return [];
  const offered: Record<CommandSurface, boolean> = {
    cypher: served.names.cypher !== null,
    sql: served.names.sql !== null,
    graphql: reader !== null,
    rest: served.names.rest !== null,
    grpc: true,
  };
  return COMMAND_SURFACES.filter((s) => offered[s]);
}

/** Each argument the call passes, in declared order: those given (a left-out one with a default
 *  takes it on the server). */
function given(call: CommandCall, values: Record<string, ArgValue>): [string, ArgValue][] {
  return call.arguments.filter((a) => a.name in values).map((a) => [a.name, values[a.name]]);
}

/** A value as a surface that takes JSON passes it: rows as the array, a scalar as itself. */
const jsonValue = (v: ArgValue): unknown =>
  v.kind === "scalar" ? v.value : v.rows !== null ? v.rows : v.text;

/** A value as a surface that takes it as a literal passes it: a relation as its text. */
const plainValue = (v: ArgValue): unknown => (v.kind === "scalar" ? v.value : v.text);

const sqlString = (s: string) => `'${s.replace(/'/g, "''")}'`;

function sqlLiteral(v: unknown): string {
  if (v === null) return "null";
  if (typeof v === "number" || typeof v === "boolean") return String(v);
  if (typeof v === "string") return sqlString(v);
  return sqlString(JSON.stringify(v));
}

function graphqlLiteral(v: unknown): string {
  if (v === null) return "null";
  if (Array.isArray(v)) return `[${v.map(graphqlLiteral).join(", ")}]`;
  if (typeof v === "object") {
    const fields = Object.entries(v as Record<string, unknown>).map(
      ([k, x]) => `${k}: ${graphqlLiteral(x)}`,
    );
    return `{${fields.join(", ")}}`;
  }
  return JSON.stringify(v);
}

const cypherString = (s: string) => `'${s.replace(/\\/g, "\\\\").replace(/'/g, "\\'")}'`;

function cypherLiteral(v: unknown): string {
  if (v === null) return "null";
  if (typeof v === "number" || typeof v === "boolean") return String(v);
  if (typeof v === "string") return cypherString(v);
  if (Array.isArray(v)) return `[${v.map(cypherLiteral).join(", ")}]`;
  const fields = Object.entries(v as Record<string, unknown>).map(
    ([k, x]) => `\`${k.replace(/`/g, "``")}\`: ${cypherLiteral(x)}`,
  );
  return `{${fields.join(", ")}}`;
}

/** A REST read's query parameter: text as written, any other value as its JSON literal
 *  (provisa/api/command_fields.py argument_values). */
const restParam = (v: unknown): string => (typeof v === "string" ? v : JSON.stringify(v));

/** The name ``served`` is called by in ``language``; it is offered there (commandSurfaces). */
function nameIn(served: ServedCommand, language: keyof ServedCommand["names"]): string {
  const name = served.names[language];
  if (name === null) throw new Error(`${served.command} is not offered in ${language}`);
  return name;
}

/** The GraphQL call: the served field with its arguments, and the columns the role reads of
 *  what it answers (none for a scalar answer). */
function graphqlCall(
  served: ServedCommand,
  reader: MetaReader,
  args: [string, ArgValue][],
  write: boolean,
): string {
  const answers = reader.schema.exact(served.graphql_root, served.graphql_field)?.type ?? null;
  const columns = answers ? reader.schema.scalars(answers) : [];
  const argText =
    args.length > 0
      ? `(${args.map(([n, v]) => `${n}: ${graphqlLiteral(plainValue(v))}`).join(", ")})`
      : "";
  const selection = columns.length > 0 ? ` { ${columns.join(" ")} }` : "";
  return `${write ? "mutation" : "query"} { ${served.graphql_field}${argText}${selection} }`;
}

/** ``call`` on ``surface``, its arguments ``values``, as an Explore hand-off. */
export function commandQueryLink(
  surface: CommandSurface,
  call: CommandCall,
  served: ServedCommand,
  reader: MetaReader | null,
  values: Record<string, ArgValue>,
): QueryLink {
  const write = call.kind !== "query";
  const autoRun = !write;
  const args = given(call, values);
  switch (surface) {
    case "sql": {
      const list = args.map(([, v]) => sqlLiteral(plainValue(v))).join(", ");
      return {
        to: "/sql",
        state: { sql: `select * from ${nameIn(served, "sql")}(${list})`, autoRun },
      };
    }
    case "cypher": {
      const list = args.map(([, v]) => cypherLiteral(plainValue(v))).join(", ");
      return {
        to: "/graph",
        state: { query: `CALL ${nameIn(served, "cypher")}(${list})`, autoRun },
      };
    }
    case "graphql": {
      if (reader === null) throw new Error("GraphQL needs the role's schema");
      return { to: "/query", state: { query: graphqlCall(served, reader, args, write), autoRun } };
    }
    case "rest": {
      const path = `/data/rest/${served.domain_id}/commands/${nameIn(served, "rest")}`;
      if (write) {
        const body = Object.fromEntries(args.map(([n, v]) => [n, jsonValue(v)]));
        return {
          to: "/openapi",
          state: { openApiUrl: `POST ${path}`, openApiBody: body, autoRun },
        };
      }
      const params = new URLSearchParams(args.map(([n, v]) => [n, restParam(plainValue(v))]));
      const query = params.toString();
      return {
        to: "/openapi",
        state: { openApiUrl: `GET ${path}${query ? `?${query}` : ""}`, autoRun },
      };
    }
    case "grpc": {
      const body = {
        name: call.name,
        args: Object.fromEntries(args.map(([n, v]) => [n, jsonValue(v)])),
      };
      return { to: "/grpc", state: { grpcCommand: call.name, grpcBody: body, autoRun } };
    }
  }
}
