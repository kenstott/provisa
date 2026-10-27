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
import { Stack, Text } from "@mantine/core";
import {
  listTermTables,
  retireGlossaryTerm,
  TagLossRefused,
  type GlossaryTermDetail,
  type GlossaryTermSummary,
  type GlossaryTermTable,
} from "../../api/glossary";
import { RetireDialog } from "../lifecycle/RetireDialog";
import { successorOptions } from "./retireSuccessors";
import { TagLossSummary } from "./TagLossSummary";
import { tagLossTitle } from "./tagLoss";

type Resolve = { confirm_tag_loss?: string[]; carry_tags?: boolean };

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
  // The tables it is grounded in, read when it has any.
  const [tables, setTables] = useState<GlossaryTermTable[] | null>(
    term.table_count > 0 ? null : [],
  );
  const holdsData = term.refs.length > 0 || term.table_count > 0;

  useEffect(() => {
    if (term.table_count > 0) void listTermTables(term.id).then(setTables);
  }, [term.id, term.table_count]);

  // A move is refused for its tags only when something moves, which only a successor takes.
  const nameOf = (successor: string | null) =>
    successor === null ? null : (terms.find((o) => String(o.id) === successor)?.name ?? null);

  return (
    <RetireDialog<Resolve>
      text={{
        title: t("glossaryTab.retireTitle", { term: term.name }),
        reason: t("glossaryTab.retireReason"),
        successor: t("glossaryTab.retireSuccessor"),
        successorHint: holdsData
          ? t("glossaryTab.retireSuccessorHint")
          : t("glossaryTab.retireNoData"),
        submit: t("glossaryTab.retire"),
      }}
      successorOptions={successorOptions(term, terms)}
      successorRequired={holdsData}
      moves={
        holdsData && (
          <Stack gap={2}>
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
        )
      }
      retire={(reason, successor, resolve) =>
        retireGlossaryTerm(term.id, {
          reason,
          successor: successor === null ? null : Number(successor),
          ...resolve,
        })
      }
      refusal={(e, successor, retry, back, saving) =>
        e instanceof TagLossRefused
          ? {
              title: tagLossTitle(t, e.columns, nameOf(successor)),
              body: (
                <TagLossSummary
                  columns={e.columns}
                  into={nameOf(successor)}
                  saving={saving}
                  onMove={(confirm_tag_loss) => retry({ confirm_tag_loss })}
                  onCarry={() => retry({ carry_tags: true })}
                  onCancel={back}
                />
              ),
            }
          : null
      }
      onClose={onClose}
      onRetired={onRetired}
      testId="glossary"
    />
  );
}
