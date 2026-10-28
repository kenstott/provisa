// Copyright (c) 2026 Kenneth Stott
// Canary: 3c9e7b52-84a1-4f06-bd2e-5a7f1c0d9e84
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2144 (A STEWARD SEES THE IMPACT OF EACH ACTION): before a steward approves, retires or binds
// a term, what it changes -- the terms becoming grounded, ~grounded or reaching no data, and the
// tables agents gain or lose a path to. Read from the server, which changes nothing to answer.

import { useEffect, useState } from "react";
import { useTranslation } from "react-i18next";
import { Alert, Group, Loader, Stack, Text } from "@mantine/core";
import { previewTermImpact, type ImpactAction, type TermImpact } from "../../api/glossary";

export interface ImpactExtra {
  successor?: number | null;
  table_id?: number;
  column_name?: string;
}

/** The impact of ``action`` on ``termId``, read again whenever what it asks changes. */
export function TermImpactPreview({
  termId,
  action,
  extra = {},
}: {
  termId: number;
  action: ImpactAction;
  extra?: ImpactExtra;
}) {
  const { t } = useTranslation();
  const key = JSON.stringify([termId, action, extra]);
  const [read, setRead] = useState<{ key: string; impact: TermImpact | null; error: string }>();
  useEffect(() => {
    let live = true;
    previewTermImpact(termId, action, extra).then(
      (impact) => live && setRead({ key, impact, error: "" }),
      (e: unknown) =>
        live && setRead({ key, impact: null, error: e instanceof Error ? e.message : String(e) }),
    );
    return () => {
      live = false;
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps -- ``key`` is what the request asks.
  }, [key]);

  if (read === undefined || read.key !== key) {
    return (
      <Group gap="xs" data-testid="glossary-impact-loading">
        <Loader size="xs" />
        <Text size="xs" c="dimmed">
          {t("glossaryTab.impactLoading")}
        </Text>
      </Group>
    );
  }
  if (read.impact === null) {
    return (
      <Alert color="red" data-testid="glossary-impact-error">
        {read.error}
      </Alert>
    );
  }
  const impact = read.impact;
  const names = (items: { name: string }[]) => items.map((i) => i.name).join(", ");
  const lines: { key: string; text: string; colour?: string }[] = [];
  if (impact.becomes_grounded.length > 0)
    lines.push({
      key: "grounded",
      text: t("glossaryTab.impactGrounded", {
        count: impact.becomes_grounded.length,
        terms: names(impact.becomes_grounded),
      }),
      colour: "teal",
    });
  if (impact.becomes_pending.length > 0)
    lines.push({
      key: "pending",
      text: t("glossaryTab.impactPending", {
        count: impact.becomes_pending.length,
        terms: names(impact.becomes_pending),
      }),
      colour: "yellow.8",
    });
  if (impact.becomes_not_grounded.length > 0)
    lines.push({
      key: "not-grounded",
      text: t("glossaryTab.impactNotGrounded", {
        count: impact.becomes_not_grounded.length,
        terms: names(impact.becomes_not_grounded),
      }),
      colour: "red",
    });
  if (impact.tables_gained > 0)
    lines.push({
      key: "gained",
      text: t("glossaryTab.impactTablesGained", { count: impact.tables_gained }),
      colour: "teal",
    });
  if (impact.tables_lost > 0)
    lines.push({
      key: "lost",
      text: t("glossaryTab.impactTablesLost", { count: impact.tables_lost }),
      colour: "red",
    });
  return (
    <Stack gap={4} data-testid="glossary-impact">
      {lines.length === 0 ? (
        <Text size="sm" c="dimmed" data-testid="glossary-impact-none">
          {t("glossaryTab.impactNone")}
        </Text>
      ) : (
        lines.map((line) => (
          <Text key={line.key} size="sm" c={line.colour} data-testid={`glossary-impact-${line.key}`}>
            {line.text}
          </Text>
        ))
      )}
    </Stack>
  );
}
