// Copyright (c) 2026 Kenneth Stott
// Canary: d3e7f291-6b14-4c22-8a59-1f93d205b7e8
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import type { ActionArg, DatasetColumn, InlineField } from "../../api/actions";

export const GRAPHQL_TYPES = ["String", "Int", "Float", "Boolean", "DateTime", "Date", "BigInt", "JSON"];

export const EMPTY_ARG: ActionArg = { name: "", type: "String" };
export const EMPTY_INLINE: InlineField = { name: "", type: "String" };

export type ActionType = "function" | "webhook";

export interface FormState {
  actionType: ActionType;
  name: string;
  sourceId: string;
  schemaName: string;
  functionName: string;
  returns: string;
  visibleTo: string;
  writablBy: string;
  domainId: string;
  description: string;
  arguments: ActionArg[];
  url: string;
  method: string;
  timeoutMs: number;
  inlineReturnType: InlineField[];
  kind: string;
  // REQ-1159: output is ALWAYS a dataset — either a registered table ("table") or an
  // authored IR-typed column list ("dataset"). The free-form JSON return schema is gone;
  // return_schema is derived from outputColumns (see deriveReturnSchema), never authored.
  returnSchemaMode: "table" | "dataset";
  // REQ-885: implementation kind + swappable binding (JSON keys per kind) + identity model.
  implKind: string;
  binding: Record<string, unknown>;
  materialize: boolean;
  // REQ-1159: canonical IR-typed output dataset contract (returnSchema is its GraphQL projection).
  outputColumns: DatasetColumn[];
}

// REQ-885: selectable implementation kinds for the function/command editor.
export const IMPL_KINDS = [
  { value: "source_procedure", label: "Source procedure" },
  { value: "script", label: "Script (local subprocess)" },
  { value: "http", label: "HTTP endpoint" },
  { value: "grpc", label: "gRPC service" },
  { value: "python", label: "Python (in-process)" },
];

export const ARG_KINDS = [
  { value: "column_value", label: "column_value (scalar)" },
  { value: "table_ref", label: "table_ref (lazy)" },
  { value: "result_set", label: "result_set (materialized)" },
];

// REQ-1159/REQ-846: the canonical IR type vocabulary (provisa.core.ir_types) — the ONE type system a
// dataset contract speaks. GraphQL/SQL spellings are edge projections, never authored here.
export const IR_TYPES = [
  "smallint",
  "integer",
  "bigint",
  "text",
  "boolean",
  "float",
  "double",
  "numeric",
  "date",
  "timestamp",
  "time",
  "uuid",
  "bytea",
  "json",
];

// REQ-1159: a dataset arg's argKind is a relation → it carries a column contract.
export const DATASET_ARG_KINDS = new Set(["table_ref", "result_set"]);
export const EMPTY_DATASET_COLUMN = { name: "", type: "text" };

export const EMPTY_FORM: FormState = {
  actionType: "function",
  name: "",
  sourceId: "",
  schemaName: "public",
  functionName: "",
  returns: "",
  visibleTo: "",
  writablBy: "",
  domainId: "",
  description: "",
  arguments: [],
  url: "",
  method: "POST",
  timeoutMs: 5000,
  inlineReturnType: [],
  kind: "mutation",
  returnSchemaMode: "table",
  implKind: "source_procedure",
  binding: {},
  materialize: false,
  outputColumns: [],
};

// REQ-1159: IR type → JSON-schema scalar. return_schema is the GraphQL projection of the
// output dataset; it is DERIVED here, never authored. Mirrors the backend IR type vocabulary.
function irToJsonSchemaType(irType: string): string {
  switch (irType) {
    case "smallint":
    case "integer":
      return "integer";
    case "float":
    case "double":
    case "numeric":
      return "number";
    case "boolean":
      return "boolean";
    // bigint/text/date/timestamp/time/uuid/bytea/json all project to GraphQL String.
    default:
      return "string";
  }
}

// JSON-schema scalar → IR type. Only used to reverse-project a legacy command that carries a
// hand-authored return_schema but no output_columns, so editing it surfaces an IR-typed dataset.
function jsonSchemaTypeToIr(jsType: string): string {
  switch (jsType) {
    case "integer":
      return "integer";
    case "number":
      return "double";
    case "boolean":
      return "boolean";
    default:
      return "text";
  }
}

// REQ-1159: derive the GraphQL projection (return_schema) from the canonical output dataset.
export function deriveReturnSchema(
  outputColumns: DatasetColumn[],
): Record<string, unknown> | null {
  const cols = outputColumns.filter((c) => c.name.trim() !== "");
  if (cols.length === 0) return null;
  const properties: Record<string, unknown> = {};
  for (const c of cols) properties[c.name] = { type: irToJsonSchemaType(c.type) };
  return { type: "array", items: { type: "object", properties } };
}

// Reverse-project a legacy return_schema into IR-typed dataset columns (edit-time only).
export function columnsFromReturnSchema(
  schema: Record<string, unknown> | null | undefined,
): DatasetColumn[] {
  if (!schema) return [];
  const top = (schema as { type?: string }).type ?? "object";
  const props =
    top === "array"
      ? ((schema as { items?: { properties?: Record<string, { type?: string }> } }).items
          ?.properties ?? {})
      : ((schema as { properties?: Record<string, { type?: string }> }).properties ?? {});
  return Object.entries(props).map(([name, v]) => ({
    name,
    type: jsonSchemaTypeToIr(v?.type ?? "string"),
  }));
}
