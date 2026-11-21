// Copyright (c) 2026 Kenneth Stott
// Canary: 7c2e9b40-6f1a-4d83-95e7-a3b8d0c4f612
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-413: the Relationships page's discovery button, which animates while a run is in progress
// and still opens the dialog (where the run and its Stop are), and the one line the page shows
// about the latest run (amended 2026-11-16): going, with its progress, or ended, with how many of
// its suggestions wait, and Open, which shows it in the dialog. The line goes once the run's
// suggestions are all decided; the run's summary is in the dialog, never on the page.

import { useTranslation } from "react-i18next";
import { ActionIcon, Button, Group, Loader, Text } from "@mantine/core";
import { Sparkles } from "lucide-react";
import type { DiscoveryRun } from "../../api/admin";
import { runLine } from "./runLine";

export function DiscoverButton({ running, onOpen }: { running: boolean; onOpen: () => void }) {
  const { t } = useTranslation();
  return (
    <ActionIcon
      data-tour="rels-suggest"
      variant="subtle"
      aria-label={
        running ? t("relationshipsPage.discovering") : t("relationshipsPage.suggestWithAi")
      }
      onClick={onOpen}
      data-running={running ? "true" : undefined}
      data-testid="rels-discover"
    >
      {running ? <Loader size={14} /> : <Sparkles size={14} />}
    </ActionIcon>
  );
}

export function DiscoverRunLine({
  latest,
  waiting,
  onOpen,
}: {
  latest: DiscoveryRun | null;
  /** How many of the ended run's suggestions wait; null while not counted. */
  waiting: number | null;
  onOpen: () => void;
}) {
  const { t } = useTranslation();
  if (latest === null) return null;
  const going = latest.status === "running";
  if (!going && (waiting === null || waiting === 0)) return null;
  return (
    <Group gap="xs" mb="md" wrap="nowrap" data-testid="rels-discover-line" aria-live="polite">
      {going && <Loader size={14} aria-hidden />}
      <Text fz="0.875rem" c={latest.status === "failed" ? "red" : undefined}>
        {runLine(latest, waiting, t)}
      </Text>
      <Button variant="subtle" size="compact-sm" onClick={onOpen} data-testid="rels-discover-open">
        {t("relationshipsPage.openRun")}
      </Button>
    </Group>
  );
}
