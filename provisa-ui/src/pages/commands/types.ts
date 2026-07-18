// Copyright (c) 2026 Kenneth Stott
// Canary: d3e7f291-6b14-4c22-8a59-1f93d205b7e8
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import type { ActionArg, InlineField } from "../../api/actions";

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
  returnSchemaMode: "table" | "custom";
  sampleJson: string;
  returnSchemaStr: string;
  // REQ-885: implementation kind + swappable binding (JSON keys per kind) + identity model.
  implKind: string;
  binding: Record<string, unknown>;
  materialize: boolean;
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
  sampleJson: "",
  returnSchemaStr: "",
  implKind: "source_procedure",
  binding: {},
  materialize: false,
};

export function inferJsonSchema(jsonStr: string): string {
  try {
    const obj = JSON.parse(jsonStr);
    const sample = Array.isArray(obj) ? obj[0] : obj;
    if (!sample || typeof sample !== "object") return "";
    const props: Record<string, unknown> = {};
    for (const [k, v] of Object.entries(sample)) {
      const t = typeof v;
      props[k] = {
        type:
          t === "number"
            ? Number.isInteger(v as number)
              ? "integer"
              : "number"
            : t === "boolean"
              ? "boolean"
              : "string",
      };
    }
    return JSON.stringify(
      {
        type: Array.isArray(obj) ? "array" : "object",
        ...(Array.isArray(obj)
          ? { items: { type: "object", properties: props } }
          : { properties: props }),
      },
      null,
      2,
    );
  } catch {
    return "";
  }
}
