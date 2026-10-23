// Copyright (c) 2026 Kenneth Stott
// Canary: 9b41d6e0-2f7c-4a88-b315-c0e2d97a46f1
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// A term's To review section: one chip per finding, each opening to what was found, in the reader's
// language (serverErrors, by its code), and the acts that fix it. A finding whose fix is not
// offered here says what was found alone.

import { useState } from "react";
import { useTranslation } from "react-i18next";
import { Badge, Button, Group, Paper, Stack, Text, Title } from "@mantine/core";
import type { GlossaryFinding } from "../../api/glossary";
import { serverMessage } from "../../i18n/serverMessage";

export interface FindingFix {
  label: string;
  run: () => void;
  testId: string;
}

export function TermToReview({
  findings,
  fixesOf,
}: {
  findings: GlossaryFinding[];
  /** The acts that fix a finding, where the caller may take them. */
  fixesOf: (finding: GlossaryFinding) => FindingFix[];
}) {
  const { t } = useTranslation();
  const [open, setOpen] = useState<number | null>(null);
  if (findings.length === 0) return null;
  const shown = open === null ? undefined : findings[open];
  return (
    <Stack gap={6} data-testid="glossary-to-review">
      <Title order={5}>{t("glossaryTab.toReview")}</Title>
      <Group gap={6}>
        {findings.map((f, i) => (
          <Badge
            key={`${f.code}-${i}`}
            component="button"
            type="button"
            variant={open === i ? "filled" : "light"}
            color="orange"
            style={{ cursor: "pointer" }}
            aria-pressed={open === i}
            onClick={() => setOpen(open === i ? null : i)}
            data-testid={`glossary-finding-${i}`}
            data-kind={f.kind}
          >
            {t(`glossaryTab.finding_${f.kind}`)}
          </Badge>
        ))}
      </Group>
      {shown && (
        <Paper withBorder p="xs" data-testid="glossary-finding-detail">
          <Stack gap={6} align="flex-start">
            <Text size="sm">{serverMessage(shown, shown.message)}</Text>
            <Group gap="xs">
              {fixesOf(shown).map((fix) => (
                <Button
                  key={fix.testId}
                  size="xs"
                  variant="default"
                  onClick={fix.run}
                  data-testid={fix.testId}
                >
                  {fix.label}
                </Button>
              ))}
            </Group>
          </Stack>
        </Paper>
      )}
    </Stack>
  );
}
