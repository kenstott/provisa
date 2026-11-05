// Copyright (c) 2026 Kenneth Stott
// Canary: 9683491b-009b-44a0-9fd6-3e782e22bc2a
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2147 (CONTAINED VALUES ARE HINTS): a person asks for a scan of a table's text columns for
// sensitive kinds held inside their values -- a social security number in a note -- from the
// column access settings, while the table is being registered or afterwards. The scan runs in the
// background; its hints show beside each column's grant and masks. A hint is never a
// classification and is never applied: what to grant, mask or fake stays the person's choice.

import { useTranslation } from "react-i18next";
import { Button, Group, Stack, Text } from "@mantine/core";
import { Tip } from "../../components/Tip";
import type { SensitiveHint } from "../../api/profiler";
import type { SensitiveScanHandle } from "./useSensitiveScan";

/** The scan action and what became of the latest scan. */
export function SensitiveScanBar({ scan }: { scan: SensitiveScanHandle }) {
  const { t } = useTranslation();
  const latest = scan.state?.scan ?? null;
  let status: string | null = null;
  if (!scan.saved) status = t("sensitiveScan.saveFirst");
  else if (latest?.status === "running") status = t("sensitiveScan.running");
  else if (latest?.status === "succeeded")
    status = t("sensitiveScan.succeeded", {
      rows: latest.sampledRows ?? 0,
      columns: latest.scannedColumns ?? 0,
    });
  else if (latest?.status === "failed")
    status = t("sensitiveScan.failed", { error: latest.error ?? "" });
  return (
    <Group gap="sm" mb="xs" wrap="wrap" data-testid="sensitive-scan-bar">
      <Tip text={t("sensitiveScan.help")}>
        <Button
          variant="light"
          size="xs"
          onClick={scan.start}
          disabled={!scan.saved || scan.running}
          loading={scan.starting}
          data-testid="sensitive-scan-start"
        >
          {t("sensitiveScan.scan")}
        </Button>
      </Tip>
      {status && (
        <Text
          size="sm"
          c={latest?.status === "failed" ? "red" : "dimmed"}
          data-testid="sensitive-scan-status"
        >
          {status}
        </Text>
      )}
      {scan.error && (
        <Text size="sm" c="red" data-testid="sensitive-scan-error">
          {scan.error}
        </Text>
      )}
    </Group>
  );
}

/** A column's hints, in words: "Matches a US social security number in 12% of sampled values". */
export function SensitiveHintText({ column, hints }: { column: string; hints: SensitiveHint[] }) {
  const { t } = useTranslation();
  if (hints.length === 0) return null;
  return (
    <Stack gap={2} data-testid={`sensitive-hint-${column}`}>
      {hints.map((h) => (
        <Text key={h.kind} size="xs" c="orange.8">
          {t("sensitiveScan.hint", {
            kind: t(`classificationKinds.noun.${h.kind}`),
            percent: Math.round(h.share * 100),
          })}
        </Text>
      ))}
    </Stack>
  );
}
