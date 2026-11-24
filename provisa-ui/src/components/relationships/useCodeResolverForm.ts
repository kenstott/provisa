// Copyright (c) 2026 Kenneth Stott
// Canary: 2e8b4f17-9c3a-4d60-b5e1-7a0d6c9f3b28
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// Requirements: REQ-2249

import { useCallback, useState } from "react";
import { useCodeResolvers } from "../../hooks/useAdminQueries";
import { serverMessage } from "../../i18n/serverMessage";
import type { MutationResult } from "../../types/admin";
import { NEEDS_REBUILD_EVERY, rebuildEverySeconds } from "./incrementalOption";
import { EMPTY_FORM, type RelForm } from "./relationship-types";

interface CodeResolverFormPage {
  form: RelForm;
  setForm: (f: RelForm) => void;
  setShowForm: (show: boolean) => void;
  setSaveProblem: (problem: string) => void;
  /** The page's open panel (useDetailParam). */
  detail: { open: (id: string) => void; close: () => void };
  refusal: { refused: (result: MutationResult, subject: string) => boolean };
}

/**
 * A code resolver set on the Relationships page: one resolver picked, kept as a bridge view and
 * two relationships, saved, changed and retired as one. The add form's "code resolver" target
 * saves one; a relationship that is part of one opens it to change (the form then holds the set's
 * id as its original id), or retires all of it. The lists are read again by the mutations.
 */
export function useCodeResolverForm(page: CodeResolverFormPage) {
  const { codeResolvers, saveCodeResolver, retireCodeResolver } = useCodeResolvers();
  const [saving, setSaving] = useState(false);
  const { form, setForm, setShowForm, setSaveProblem, detail, refusal } = page;
  // The form changes an existing set, whose id and kind stay as they are.
  const changing = form.targetType === "resolver" && form.originalId !== "";

  const save = useCallback(async () => {
    if (!form.id || !form.sourceTableId || !form.sourceColumn) return;
    if (!form.targetTableId || !form.targetColumn) return;
    if (!form.resolver || !form.sourceMatch || !form.targetMatch) return;
    const rebuildEvery = form.incremental ? rebuildEverySeconds(form.rebuildEvery) : null;
    // Matches kept incrementally are rebuilt whole on a clock: said here as the server says it.
    if (form.incremental && rebuildEvery === null) {
      setSaveProblem(serverMessage({ code: NEEDS_REBUILD_EVERY, params: { table: form.id } }, ""));
      return;
    }
    setSaving(true);
    setSaveProblem("");
    let result: MutationResult;
    try {
      result = await saveCodeResolver(
        {
          id: form.id,
          resolver: form.resolver,
          sourceTable: form.sourceTableId,
          sourceColumn: form.sourceColumn,
          sourceMatch: form.sourceMatch,
          targetTable: form.targetTableId,
          targetColumn: form.targetColumn,
          targetMatch: form.targetMatch,
          refreshInterval: parseInt(form.refreshInterval) || 300,
          incremental: form.incremental,
          rebuildEvery,
        },
        !changing,
      );
    } catch (err) {
      setSaveProblem(err instanceof Error ? err.message : String(err));
      return;
    } finally {
      setSaving(false);
    }
    if (!result.success) {
      setSaveProblem(serverMessage(result, result.message));
      return;
    }
    // The set's one relationship opens: its junction, named by the set's id (REQ-2249).
    detail.open(form.id);
    setForm(EMPTY_FORM);
    setShowForm(false);
  }, [form, changing, saveCodeResolver, detail, setForm, setSaveProblem, setShowForm]);

  const retire = useCallback(
    async (id: string) => {
      const result = await retireCodeResolver(id);
      if (refusal.refused(result, id)) return false;
      if (!result.success) throw new Error(serverMessage(result, result.message));
      detail.close();
    },
    [retireCodeResolver, refusal, detail],
  );

  const change = useCallback(
    (id: string) => {
      const set = codeResolvers.find((c) => c.id === id);
      if (!set) return;
      setSaveProblem("");
      detail.close();
      setForm({
        ...EMPTY_FORM,
        id: set.id,
        originalId: set.id,
        targetType: "resolver",
        sourceTableId: set.sourceTable,
        sourceColumn: set.sourceColumn,
        targetTableId: set.targetTable,
        targetColumn: set.targetColumn,
        resolver: set.resolver,
        sourceMatch: set.sourceMatch,
        targetMatch: set.targetMatch,
        refreshInterval: String(set.refreshInterval),
        incremental: set.incremental,
        rebuildEvery: set.rebuildEvery === null ? "" : String(set.rebuildEvery),
      });
      setShowForm(true);
    },
    [codeResolvers, detail, setForm, setSaveProblem, setShowForm],
  );

  return { saving, changing, save, retire, change };
}
