// Copyright (c) 2026 Kenneth Stott
// Canary: 9b4e2f71-c6a8-4d13-8e5f-2a7d03b9c164
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1159 (amended 2026-11-27), REQ-1993, REQ-2226: a command written in the command form as
// entered code is saved with the mutations that already exist. Its code is the entered code of
// the Python code source of the command's name, written as the code source form writes it
// (codeMappingJson), saved by createSource or updateSource; the command is then registered on
// that source by naming its declared function, and the server reads its kind, arguments and
// columns from the declaration.

import type { Source } from "../../types/admin";
import type { saveFunction } from "../../api/actions";
import { codeFieldsFromMapping, codeMappingJson } from "../sources/codeSource";
import { CODE_COMMAND, CODE_COMMAND_SCHEMA, ENTERED_CODE_TYPE, enteredCodeOf } from "./types";
import type { FormState } from "./types";

type SaveFunctionInput = Parameters<typeof saveFunction>[0];

/** The source's mapping holding the form's code as entered code. A source already saved keeps
 *  every other setting of its mapping. */
function mappingOf(form: FormState, stored: Source | undefined): string {
  const kept = stored?.mappingJson ? codeFieldsFromMapping(stored.mappingJson) : {};
  return codeMappingJson(ENTERED_CODE_TYPE, {
    ...kept,
    code_origin: "entered",
    code_source: form.enteredCode,
    code_dependencies: form.enteredDependencies,
  });
}

/** The createSource or updateSource input saving the form's code. updateSource is a full
 *  upsert, so a source already saved is sent with every setting it holds. */
export function enteredSourceInput(
  sourceId: string,
  form: FormState,
  stored: Source | undefined,
): Record<string, unknown> {
  const mappingJson = mappingOf(form, stored);
  if (!stored) {
    return { id: sourceId, type: ENTERED_CODE_TYPE, host: "", port: 0, database: "", mappingJson };
  }
  return {
    id: stored.id,
    type: stored.type,
    host: stored.host ?? "",
    port: stored.port ?? 0,
    database: stored.database ?? "",
    path: stored.path,
    description: stored.description,
    allowedDomains: stored.allowedDomains,
    mappingJson,
    changeSignal: stored.changeSignal,
    loadProtected: stored.loadProtected,
    offPeakWindow: stored.offPeakWindow,
    offPeakTz: stored.offPeakTz,
    cacheEnabled: stored.cacheEnabled,
    cacheTtl: stored.cacheTtl,
    replicate: stored.replicate,
    gqlNamingConvention: stored.gqlNamingConvention,
    maxLiveConcurrency: stored.maxLiveConcurrency,
    sentinelPath: stored.sentinelPath,
    downloadFrom: stored.downloadFrom,
    freshnessGate: stored.freshnessGate,
  };
}

/** The command registered on the source by naming its declared function: no kind, arguments or
 *  columns, which the server reads from the declaration (actions_router._as_code_command). */
export function enteredCommandInput(
  sourceId: string,
  form: FormState,
  visibleTo: string[],
): SaveFunctionInput {
  return {
    name: form.name,
    sourceId,
    schemaName: CODE_COMMAND_SCHEMA,
    functionName: form.functionName,
    returns: "",
    arguments: [],
    visibleTo,
    domainId: form.domainId,
    description: form.description || undefined,
    returnSchema: null,
    outputColumns: null,
    implKind: CODE_COMMAND,
    binding: {},
    materialize: false,
    requiresApproval: form.requiresApproval,
    languageBindings: form.languageBindings,
  };
}

/** The form's entered code fields for a registered command, or null when it is not entered code:
 *  a code command on a source whose mapping holds entered code. */
export function enteredFieldsOf(
  implKind: string,
  source: Source | undefined,
): Pick<FormState, "enteredCode" | "enteredDependencies"> | null {
  if (implKind !== CODE_COMMAND || !source) return null;
  const code = enteredCodeOf(source.mappingJson);
  if (!code) return null;
  return { enteredCode: code.source, enteredDependencies: code.dependencies.join("\n") };
}
