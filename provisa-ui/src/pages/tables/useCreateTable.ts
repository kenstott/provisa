// Copyright (c) 2026 Kenneth Stott
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2109 (settled): one Save registers a table or view with every setting its create form
// holds. The registration input carries what TableInput takes; the settings it does not take are
// saved on the new table straight after, with the mutations the editor saves them with.

import {
  useRegisterTable,
  useUpdateTableNaming,
  useUpdateTableReplicate,
} from "../../hooks/useAdminQueries";
import { useUpdateTableRoleTtl } from "../../hooks/useAdminOpsQueries";
import { useSetTableDraft } from "../../hooks/useRegionQueries";
import { useSetTableWriteGrants } from "../../hooks/useWriteGrants";
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
    loadProtected: draft.loadProtected,
    offPeakWindow: draft.offPeakWindow,
    offPeakTz: draft.offPeakTz,
  };
}

export function useCreateTable() {
  const { registerTable } = useRegisterTable();
  const { updateTableNaming } = useUpdateTableNaming();
  const { updateTableReplicate } = useUpdateTableReplicate();
  const { updateTableRoleTtl } = useUpdateTableRoleTtl();
  const setTableDraft = useSetTableDraft();
  const { setTableWriteGrants } = useSetTableWriteGrants();
  const { upsertRlsRule } = useUpsertRlsRule();

  return async (
    draft: RegisteredTable,
    registration: Registration,
    cacheTtl: number | null,
  ): Promise<CreateOutcome> => {
    const result = await registerTable(registrationInput(draft, registration, cacheTtl));
    if (!result.success) return { refused: result };
    const made = registeredTable(result);
    // A request queued for approval registers nothing yet: there is no table to save more on.
    if (made === null) return { registered: null };
    const id = made.id;
    const steps: (() => Promise<MutationResult>)[] = [
      ...(draft.gqlNamingConvention
        ? [() => updateTableNaming(id, draft.gqlNamingConvention)]
        : []),
      ...(draft.roleTtl.length > 0 ? [() => updateTableRoleTtl(id, draft.roleTtl)] : []),
      ...(draft.replicate !== null ? [() => updateTableReplicate(id, draft.replicate)] : []),
      ...(draft.writeGrants && draft.writeGrants.length > 0
        ? [() => setTableWriteGrants(id, draft.writeGrants ?? [])]
        : []),
      ...registration.rowRules
        .filter((rule) => rule.filter.trim())
        .map(
          (rule) => () =>
            upsertRlsRule({
              tableId: draft.tableName,
              roleId: rule.roleId,
              filterExpr: rule.filter.trim(),
              description: rule.description.trim(),
            }),
        ),
      // Registered through the admin, a table starts as draft (REQ-1921): released last, once
      // everything else of it is saved.
      ...(draft.draft ? [] : [() => setTableDraft(id, false)]),
    ];
    for (const step of steps) {
      const done = await step();
      if (!done.success) return { refused: done };
    }
    return { registered: made };
  };
}
