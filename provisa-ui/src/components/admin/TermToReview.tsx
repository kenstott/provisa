// Copyright (c) 2026 Kenneth Stott
// Canary: 9b41d6e0-2f7c-4a88-b315-c0e2d97a46f1
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// A term's To review section: one chip per finding, each opening to what was found and the act that
// fixes it. A finding whose fix is not offered here says what was found alone.

import { useState } from "react";
import { useTranslation } from "react-i18next";
import { Badge, Button, Group, Paper, Stack, Text, Title } from "@mantine/core";
import type { FindingKind, TermFinding } from "./termFindings";

export type FindingFixes = Partial<Record<FindingKind, (finding: TermFinding) => void>>;

export function TermToReview({
  findings,
  fixes,
}: {
  findings: TermFinding[];
  /** The act that fixes each kind, where the caller may take it. */
  fixes: FindingFixes;
}) {
  const { t } = useTranslation();
  const [open, setOpen] = useState<FindingKind | null>(null);
  if (findings.length === 0) return null;
  const shown = findings.find((f) => f.kind === open);
  const fix = shown && fixes[shown.kind];
  return (
    <Stack gap={6} data-testid="glossary-to-review">
      <Title order={5}>{t("glossaryTab.toReview")}</Title>
      <Group gap={6}>
        {findings.map((f) => (
          <Badge
            key={f.kind}
            component="button"
            type="button"
            variant={open === f.kind ? "filled" : "light"}
            color="orange"
            style={{ cursor: "pointer" }}
            aria-pressed={open === f.kind}
            onClick={() => setOpen(open === f.kind ? null : f.kind)}
            data-testid={`glossary-finding-${f.kind}`}
          >
            {t(`glossaryTab.finding_${f.kind}`)}
          </Badge>
        ))}
      </Group>
      {shown && (
        <Paper withBorder p="xs" data-testid={`glossary-finding-${shown.kind}-detail`}>
          <Stack gap={6} align="flex-start">
            <Text size="sm">{t(`glossaryTab.findingMessage_${shown.kind}`, shown.params)}</Text>
            {fix && (
              <Button
                size="xs"
                variant="default"
                onClick={() => fix(shown)}
                data-testid={`glossary-finding-${shown.kind}-fix`}
              >
                {t(`glossaryTab.findingFix_${shown.kind}`)}
              </Button>
            )}
          </Stack>
        </Paper>
      )}
    </Stack>
  );
}
