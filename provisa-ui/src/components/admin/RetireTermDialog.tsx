// Copyright (c) 2026 Kenneth Stott
// Canary: 6e2f8a41-c37d-4b95-8d10-f4a9b27c5e63
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// Retire a term (REQ-2144): a reason, and the successor its columns and tables move to -- required
// while it holds any, and of its own role or a concept. A move that takes tags off columns comes
// back as what each column loses and gains (REQ-2145), the losses first; the reader moves anyway,
// moves and marks the successor with the lost tags, or goes back.

import { useEffect, useState } from "react";
import { useTranslation } from "react-i18next";
import { Alert, Button, Modal, Select, Stack, Text, Textarea } from "@mantine/core";
import {
  listTermTables,
  retireGlossaryTerm,
  TagLossRefused,
  type ColumnTagChange,
  type GlossaryTermDetail,
  type GlossaryTermSummary,
  type GlossaryTermTable,
} from "../../api/glossary";
import { FormField, FormFooter, FormLayout } from "../form/FormLayout";
import { successorOptions } from "./retireSuccessors";
import { TagLossSummary } from "./TagLossSummary";
import { tagLossTitle } from "./tagLoss";

/** Drawn while it is open: each opening starts from an empty form. */
export function RetireTermDialog({
  term,
  terms,
  onClose,
  onRetired,
}: {
  term: GlossaryTermDetail;
  terms: GlossaryTermSummary[];
  onClose: () => void;
  onRetired: () => void;
}) {
  const { t } = useTranslation();
  const [reason, setReason] = useState("");
  const [successor, setSuccessor] = useState<string | null>(null);
  // The tables it is grounded in, read when it has any.
  const [tables, setTables] = useState<GlossaryTermTable[] | null>(
    term.table_count > 0 ? null : [],
  );
  const [loss, setLoss] = useState<ColumnTagChange[] | null>(null);
  const [problem, setProblem] = useState("");
  const [saving, setSaving] = useState(false);
  const holdsData = term.refs.length > 0 || term.table_count > 0;
  // Picked from successorOptions, which are drawn from ``terms``.
  const successorTerm =
    successor === null
      ? null
      : (terms.find((o) => String(o.id) === successor) as GlossaryTermSummary);

  useEffect(() => {
    if (term.table_count > 0) void listTermTables(term.id).then(setTables);
  }, [term.id, term.table_count]);

  const retire = async (resolve?: { confirm_tag_loss?: string[]; carry_tags?: boolean }) => {
    setSaving(true);
    setProblem("");
    try {
      await retireGlossaryTerm(term.id, {
        reason: reason.trim(),
        successor: successor === null ? null : Number(successor),
        ...resolve,
      });
      onRetired();
    } catch (e) {
      if (e instanceof TagLossRefused) setLoss(e.columns);
      else setProblem((e as Error).message);
    } finally {
      setSaving(false);
    }
  };

  // A move is refused for its tags only when something moves, which only a successor takes.
  const into = successorTerm === null ? null : successorTerm.name;

  return (
    <Modal
      opened
      onClose={onClose}
      title={
        loss === null
          ? t("glossaryTab.retireTitle", { term: term.name })
          : tagLossTitle(t, loss, into)
      }
      size="lg"
    >
      {loss === null ? (
        <FormLayout>
          <FormField size="full">
            <Textarea
              label={t("glossaryTab.retireReason")}
              value={reason}
              onChange={(e) => setReason(e.currentTarget.value)}
              autosize
              minRows={2}
              required
              data-testid="glossary-retire-reason"
            />
          </FormField>
          <FormField
            size="long"
            help={holdsData ? t("glossaryTab.retireSuccessorHint") : t("glossaryTab.retireNoData")}
          >
            <Select
              label={t("glossaryTab.retireSuccessor")}
              data={successorOptions(term, terms)}
              value={successor}
              onChange={setSuccessor}
              searchable
              clearable
              required={holdsData}
              data-testid="glossary-retire-successor"
            />
          </FormField>
          {holdsData && (
            <FormField size="full">
              <Stack gap={2} data-testid="glossary-retire-moves">
                {term.refs.length > 0 && (
                  <Text size="xs">
                    {t("glossaryTab.retireMovesColumns", {
                      columns: term.refs.map((r) => `${r.table_name}.${r.column_name}`).join(", "),
                    })}
                  </Text>
                )}
                {tables !== null && tables.length > 0 && (
                  <Text size="xs">
                    {t("glossaryTab.retireMovesTables", {
                      tables: tables.map((g) => `${g.domain_id}.${g.table_name}`).join(", "),
                    })}
                  </Text>
                )}
              </Stack>
            </FormField>
          )}
          {problem && (
            <FormField size="full">
              <Alert color="red" data-testid="glossary-retire-error">
                {problem}
              </Alert>
            </FormField>
          )}
          <FormFooter
            onCancel={onClose}
            action={
              <Button
                color="orange"
                onClick={() => void retire()}
                loading={saving}
                disabled={!reason.trim() || (holdsData && successor === null)}
                data-testid="glossary-retire-submit"
              >
                {t("glossaryTab.retire")}
              </Button>
            }
          />
        </FormLayout>
      ) : (
        <TagLossSummary
          columns={loss}
          into={into}
          saving={saving}
          onMove={(confirm_tag_loss) => void retire({ confirm_tag_loss })}
          onCarry={() => void retire({ carry_tags: true })}
          onCancel={() => setLoss(null)}
        />
      )}
    </Modal>
  );
}
