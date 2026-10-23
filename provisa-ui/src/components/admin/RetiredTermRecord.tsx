// Copyright (c) 2026 Kenneth Stott
// Canary: 8c3e6a19-5b72-4d04-a1f8-e29d70b4c635
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// A retired term's record (REQ-2144): the term that replaces it (its REPLACES edge), and what it
// applied to when it was retired -- its columns and tables as the model held them the commit
// before, since its retirement moved them to the successor.

import { useEffect, useState } from "react";
import { useTranslation } from "react-i18next";
import { Anchor, Stack, Text, Title } from "@mantine/core";
import {
  fetchAppliedHistory,
  type AppliedHistory,
  type GlossaryTermDetail,
} from "../../api/glossary";

export function RetiredTermRecord({
  term,
  navigateToTerm,
}: {
  term: GlossaryTermDetail;
  navigateToTerm: (termId: number) => void;
}) {
  const { t } = useTranslation();
  // Kept with the term it was read for; null once read is "no retirement in the history".
  const [read, setRead] = useState<{ termId: number; history: AppliedHistory | null } | null>(null);
  const [problem, setProblem] = useState("");
  useEffect(() => {
    let live = true;
    fetchAppliedHistory(term.id).then(
      (history) => live && setRead({ termId: term.id, history }),
      (e: Error) => live && setProblem(e.message),
    );
    return () => {
      live = false;
    };
  }, [term.id]);
  const successors = term.edges_in.filter((e) => e.rel_type === "REPLACES");
  const shown = read !== null && read.termId === term.id ? read : null;
  return (
    <Stack gap={4} data-testid="glossary-retired-record">
      {successors.map((e) => (
        <Text size="sm" key={e.term_id}>
          {t("glossaryTab.replacedBy")}{" "}
          <Anchor
            component="button"
            type="button"
            onClick={() => navigateToTerm(e.term_id)}
            data-testid={`glossary-replaced-by-${e.term_id}`}
          >
            {e.name}
          </Anchor>
        </Text>
      ))}
      <Title order={5}>{t("glossaryTab.lastAppliedTo")}</Title>
      {problem ? (
        <Text size="xs" c="red">
          {problem}
        </Text>
      ) : shown === null ? (
        <Text size="xs" c="dimmed">
          {t("common.loading")}
        </Text>
      ) : shown.history === null ? (
        <Text size="xs" c="dimmed" data-testid="glossary-applied-none">
          {t("serverErrors.glossary.no_retirement_history")}
        </Text>
      ) : (
        <Stack gap={2} data-testid="glossary-applied-history">
          <Text size="xs">
            {t("glossaryTab.lastAppliedColumns", {
              columns: shown.history.columns.map((c) => `${c.table}.${c.column}`).join(", ") || "—",
            })}
          </Text>
          <Text size="xs">
            {t("glossaryTab.lastAppliedTables", {
              tables: shown.history.tables.join(", ") || "—",
            })}
          </Text>
        </Stack>
      )}
    </Stack>
  );
}
