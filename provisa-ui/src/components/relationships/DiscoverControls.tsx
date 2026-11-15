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
// and still opens the dialog (where the run and its Stop are), and the banner that says what the
// run last watched found once it has ended, on the page as well as in the dialog.

import { useTranslation } from "react-i18next";
import { ActionIcon, Alert, Loader, Text } from "@mantine/core";
import { Sparkles } from "lucide-react";
import { isReading, type DiscoveryRun } from "../../api/admin";
import { discoveryResultLines } from "./discoveryResult";

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

export function DiscoverResultBanner({ run }: { run: DiscoveryRun | null }) {
  const { t } = useTranslation();
  if (!run || run.status === "running" || run.report === null || isReading(run.report)) {
    return null;
  }
  return (
    <Alert color="blue" mb="md" data-testid="rels-discover-result">
      {discoveryResultLines(run.report, t).map((line) => (
        <Text key={line} fz="0.875rem">
          {line}
        </Text>
      ))}
    </Alert>
  );
}
