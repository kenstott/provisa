// Copyright (c) 2026 Kenneth Stott
// Canary: 5f0a2d83-1c6e-4b97-8e34-b7d9c1a6f025
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// pii on a column (REQ-2145), in its tag picker. A column whose term is a Property is asked
// whether the mark goes on the term -- every column with the same meaning, those to come too -- or
// on this column alone, with a reason. A column pii through its term is unmarked on the term, or
// moved to a term that means what it holds.

import { useEffect, useState, type ReactNode } from "react";
import { useTranslation } from "react-i18next";
import { useNavigate } from "react-router-dom";
import { Button, Group, Stack, Text, TextInput } from "@mantine/core";
import { fetchGlossaryTermByRef, termRole, type GlossaryTermDetail } from "../api/glossary";
import { entityHref } from "../lib/entityHref";
import { serverMessage } from "../i18n/serverMessage";
import type { MutationResult, TagAssignment } from "../types/admin";

export function ColumnPiiForm({
  tableId,
  columnName,
  inherited,
  assign,
  unassign,
  onDone,
  ownForm,
}: {
  tableId: number;
  columnName: string;
  /** The column is pii through its term, not by a mark of its own. */
  inherited: boolean;
  assign: (input: TagAssignment) => Promise<MutationResult>;
  unassign: (input: TagAssignment) => Promise<MutationResult>;
  onDone: () => void;
  /** The picker's own form for the tag, for a column whose term is not a Property. */
  ownForm: ReactNode;
}) {
  const { t } = useTranslation();
  const navigate = useNavigate();
  // The column's term, once read: null when it has none.
  const [term, setTerm] = useState<GlossaryTermDetail | null | undefined>(undefined);
  const [justThis, setJustThis] = useState(false);
  const [reason, setReason] = useState("");
  const [refusal, setRefusal] = useState<MutationResult | null>(null);

  useEffect(() => {
    let live = true;
    void fetchGlossaryTermByRef(tableId, columnName).then((found) => live && setTerm(found));
    return () => {
      live = false;
    };
  }, [tableId, columnName]);

  const column: TagAssignment = { tagId: "pii", objectType: "column", tableId, columnName };
  const act = async (run: () => Promise<MutationResult>) => {
    const result = await run();
    if (result.success) onDone();
    else setRefusal(result);
  };
  const said = refusal && (
    <Stack gap={4} align="flex-start">
      <Text size="xs" c="red" data-testid="pii-refusal">
        {serverMessage(refusal, refusal.message)}
      </Text>
      {/* A draft term carries no tag: its definition is what admits it. */}
      {refusal.code === "tag.term_not_live" && term && (
        <Button
          size="compact-xs"
          variant="default"
          onClick={() => navigate(entityHref("term", term.id))}
          data-testid="pii-define-term"
        >
          {t("tagControl.piiOpenTerm", { term: term.name })}
        </Button>
      )}
    </Stack>
  );

  if (term === undefined) {
    return (
      <Text size="xs" c="dimmed">
        {t("common.loading")}
      </Text>
    );
  }

  if (inherited && term !== null) {
    return (
      <Stack gap="0.25rem" data-testid="pii-inherited">
        <Text size="xs">{t("tagControl.piiInherited", { term: term.name })}</Text>
        <Group gap="0.25rem">
          <Button
            size="compact-xs"
            onClick={() =>
              void act(() => unassign({ tagId: "pii", objectType: "term", termId: term.id }))
            }
            data-testid="pii-unmark-term"
          >
            {t("tagControl.piiUnmarkTerm", { term: term.name })}
          </Button>
          {/* A column that means something else moves to that term, on the term's page. */}
          <Button
            size="compact-xs"
            variant="default"
            onClick={() => navigate(entityHref("term", term.id))}
            data-testid="pii-move-column"
          >
            {t("tagControl.piiMoveColumn")}
          </Button>
        </Group>
        {said}
      </Stack>
    );
  }

  const onTerm = term !== null && termRole(term) === "property";
  if (onTerm && !justThis) {
    const others = term.ref_count - 1;
    return (
      <Stack gap="0.25rem" data-testid="pii-also-term">
        <Text size="xs">
          {others > 0
            ? t("tagControl.piiAlsoTerm", { count: others, term: term.name })
            : t("tagControl.piiAlsoTermNone", { term: term.name })}
        </Text>
        <Group gap="0.25rem">
          <Button
            size="compact-xs"
            onClick={() => void act(() => assign({ ...column, alsoTerm: true }))}
            data-testid="pii-yes"
          >
            {t("tagControl.piiYes")}
          </Button>
          <Button
            size="compact-xs"
            variant="default"
            onClick={() => setJustThis(true)}
            data-testid="pii-just-this"
          >
            {t("tagControl.piiJustThis")}
          </Button>
        </Group>
        {said}
      </Stack>
    );
  }

  // A column with no term, or one whose term is not a Property, is marked as any other tag is.
  if (!onTerm) return <>{ownForm}</>;

  // On this column alone: why it, and not the columns with the same meaning, says the reason.
  // On this column alone: why it, and not the columns with the same meaning, says the reason.
  return (
    <Stack gap="0.25rem" data-testid="pii-column">
      <TextInput
        size="xs"
        label={t("tagControl.reasonLabel")}
        required
        placeholder={t("tagControl.reasonRequired")}
        value={reason}
        onChange={(e) => setReason(e.currentTarget.value)}
        data-testid="pii-reason"
      />
      <Group gap="0.25rem">
        <Button
          size="compact-xs"
          disabled={reason.trim() === ""}
          onClick={() => void act(() => assign({ ...column, reason: reason.trim() }))}
          data-testid="pii-apply"
        >
          {t("tagControl.apply")}
        </Button>
        <Button size="compact-xs" variant="default" onClick={onDone} data-testid="pii-cancel">
          {t("tagControl.cancel")}
        </Button>
      </Group>
      {said}
    </Stack>
  );
}
