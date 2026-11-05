// Copyright (c) 2026 Kenneth Stott
// Canary: 8b3e1f60-7a24-4c95-b2d8-5f0c9e6a1b47
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// An object's To review section -- a term's (REQ-2146), a data product's: one chip per finding, each
// opening to what was found, in the reader's language (serverErrors, by its code), and the acts that
// fix it. A finding whose fix is not offered here says what was found alone.

import { useState } from "react";
import { Badge, Button, Group, Paper, Stack, Text, Title } from "@mantine/core";
import { serverMessage } from "../../i18n/serverMessage";

/** A finding as the server reports it: its kind names the chip, its code and params the message. */
export interface Finding {
  kind: string;
  code: string;
  message: string;
  params?: Record<string, unknown>;
}

export interface FindingFix {
  label: string;
  run: () => void;
  testId: string;
}

export function ToReview<F extends Finding>({
  title,
  findings,
  chipLabel,
  fixesOf,
  testId,
}: {
  title: string;
  findings: F[];
  /** The chip's words for a finding's kind. */
  chipLabel: (finding: F) => string;
  /** The acts that fix a finding, where the caller may take them. */
  fixesOf: (finding: F) => FindingFix[];
  /** The section's; each chip is ``<testId>-finding-<i>`` and the open one's detail
   * ``<testId>-finding-detail``. */
  testId: string;
}) {
  const [open, setOpen] = useState<number | null>(null);
  if (findings.length === 0) return null;
  const shown = open === null ? undefined : findings[open];
  return (
    <Stack gap={6} data-testid={`${testId}-to-review`}>
      <Title order={5}>{title}</Title>
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
            data-testid={`${testId}-finding-${i}`}
            data-kind={f.kind}
          >
            {chipLabel(f)}
          </Badge>
        ))}
      </Group>
      {shown && (
        <Paper withBorder p="xs" data-testid={`${testId}-finding-detail`}>
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
