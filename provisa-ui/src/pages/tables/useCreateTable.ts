// Copyright (c) 2026 Kenneth Stott
// Canary: 30e428d9-1d7a-4dba-ada7-76fd27258ef6
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2109 (settled): one Save registers a table or view with every setting its create form
// holds, in one registerTable: the server applies all of them in one transaction, and a refusal
// of any one registers nothing. A profiler result table's row rules are rules of their own, saved
// once the table exists.

import { useRegisterTable } from "../../hooks/useAdminQueries";
import { writeGrantInputs } from "../../hooks/useWriteGrants";
import { useUpsertRlsRule } from "../../hooks/useSecurityQueries";
import type { MutationResult, RegisteredTable } from "../../types/admin";
import { buildTableUpdateInput } from "./helpers";
import { declaredPaging, pagingInput } from "./paging";
import { registeredTable } from "./registeredTable";
import type { Registration } from "./draftTable";

/** What a create came to: the table it registered, or the answer that refused a step. */
export type CreateOutcome =
  | { registered: { id: number } | null; refused?: undefined }
  | { refused: MutationResult; registered?: undefined };

/** The registration input for `draft`: the create step's input with every setting of the form. */
export function registrationInput(
  draft: RegisteredTable,
  registration: Registration,
  cacheTtl: number | null,
): Record<string, unknown> {
  const paging = declaredPaging(draft.pagination);
  return {
    ...registration.input,
    ...buildTableUpdateInput(draft),
    // A region is sent only where regions exist: the create step put one in its input then.
    ...("region" in registration.input ? { region: draft.region } : {}),
    cacheTtl: cacheTtl ?? undefined,
    pagination: paging === null ? undefined : pagingInput(paging),
    // REQ-2074: write grants are governance (masking_config); sent only when the form set some.
    ...(draft.writeGrants && draft.writeGrants.length > 0
      ? { writeGrants: writeGrantInputs(draft.writeGrants) }
      : {}),
  };
}

export function useCreateTable() {
  const { registerTable } = useRegisterTable();
  const { upsertRlsRule } = useUpsertRlsRule();

  return async (
    draft: RegisteredTable,
    registration: Registration,
    cacheTtl: number | null,
  ): Promise<CreateOutcome> => {
    const result = await registerTable(registrationInput(draft, registration, cacheTtl));
    if (!result.success) return { refused: result };
    const made = registeredTable(result);
    // A request queued for approval registers nothing yet: there is no table to save rules on.
    if (made === null) return { registered: null };
    for (const rule of registration.rowRules.filter((r) => r.filter.trim())) {
      const saved: MutationResult = await upsertRlsRule({
        tableId: draft.tableName,
        roleId: rule.roleId,
        filterExpr: rule.filter.trim(),
        description: rule.description.trim(),
      });
      if (!saved.success) return { refused: saved };
    }
    return { registered: made };
  };
}
