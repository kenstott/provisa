// Copyright (c) 2026 Kenneth Stott
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2180 (QUALITY IS STATUS AND COUNTS, NOT A SCORE): a quality summary drawn as its worst
// outcome and its counts, the failing checks named and when they last ran -- e.g. "Failing 22
// checks, 1 failing: customer_id unique · checked 2 hours ago". Never a score.

import { useTranslation } from "react-i18next";
import { Badge, Group, Text } from "@mantine/core";
import { useQualityWords, type QualitySummary, type Worst } from "./productQuality";

const COLOR: Record<Worst, string> = { fail: "red", notRun: "gray", pass: "green" };

/** The worst outcome as a badge. */
export function WorstBadge({ worst, testId }: { worst: Worst; testId?: string }) {
  const { t } = useTranslation();
  return (
    <Badge size="sm" variant="light" color={COLOR[worst]} data-testid={testId}>
      {t(`dataProductOffers.worst_${worst}`)}
    </Badge>
  );
}

/** A quality summary: its worst outcome, then its counts in words -- on one line, cut with an
 *  ellipsis, in a summary row; whole, wrapping, where it is the detail. */
export function QualityStatus({
  quality,
  testId,
  oneLine = false,
}: {
  quality: QualitySummary;
  testId: string;
  oneLine?: boolean;
}) {
  const words = useQualityWords();
  return (
    <Group gap={6} wrap="nowrap" align="flex-start" style={{ minWidth: 0 }} data-testid={testId}>
      {quality.worst && <WorstBadge worst={quality.worst} testId={`${testId}-worst`} />}
      <Text
        size="xs"
        c="dimmed"
        truncate={oneLine ? "end" : undefined}
        data-testid={`${testId}-words`}
      >
        {words(quality)}
      </Text>
    </Group>
  );
}
