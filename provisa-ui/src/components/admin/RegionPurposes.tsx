// Copyright (c) 2026 Kenneth Stott
// Canary: 8a718c60-6474-471f-bed2-be054fff2179
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1922: why a platform region exists. A region's purposes explain; they change no routing,
// policy or validation. Each purpose shows its one-line explanation wherever regions are listed:
// as a tooltip on its badge, and in the legend under the list.

import { useTranslation } from "react-i18next";
import { Badge, Group, Stack, Text } from "@mantine/core";
import { REGION_PURPOSES, type RegionPurpose } from "../../api/regions";
import { Tip } from "../Tip";

/** A region's purposes, each a badge whose tooltip is its explanation. */
export function PurposeBadges({ purposes }: { purposes: RegionPurpose[] }) {
  const { t } = useTranslation();
  return (
    <Group gap={4} wrap="wrap">
      {purposes.map((p) => (
        <Tip key={p} text={t(`regionPurposes.explain.${p}`)}>
          <Badge size="xs" variant="light" data-testid={`region-purpose-${p}`}>
            {t(`regionPurposes.name.${p}`)}
          </Badge>
        </Tip>
      ))}
    </Group>
  );
}

/** Every purpose and its one-line explanation, and that purposes explain and do not act. */
export function PurposesLegend() {
  const { t } = useTranslation();
  return (
    <Stack gap={4} data-testid="region-purposes-legend">
      <Text size="xs" fw={600}>
        {t("regionPurposes.legendTitle")}
      </Text>
      {REGION_PURPOSES.map((p) => (
        <Text key={p} size="xs" c="dimmed" data-testid={`region-purposes-legend-${p}`}>
          <Text span size="xs" fw={600} c="var(--mantine-color-text)">
            {t(`regionPurposes.name.${p}`)}
          </Text>
          {" — "}
          {t(`regionPurposes.explain.${p}`)}
        </Text>
      ))}
      <Text size="xs" c="dimmed" fs="italic">
        {t("regionPurposes.explainOnly")}
      </Text>
    </Stack>
  );
}
