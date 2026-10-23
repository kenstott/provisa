// Copyright (c) 2026 Kenneth Stott
// Canary: a7c8de11-e140-4c11-9a23-adf9f24eb871
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// How close a force-laid-out 3D graph draws its nodes: one slider, loose to tight, in each such
// graph's toolbar, read "Loose ——— Tight" and named Attraction. 1 is the usual spacing. The value
// lasts for the browser session per graph (this tab, never the model); a graph with a fixed layout
// (the terrain, the strata) has none.

import { useTranslation } from "react-i18next";
import { Group, Slider, Text } from "@mantine/core";
import { ATTRACTION_MAX, ATTRACTION_MIN } from "./attractionState";
import { Tip } from "../../../components/Tip";

export function AttractionSlider({
  value,
  onChange,
  testId,
}: {
  value: number;
  onChange: (v: number) => void;
  testId: string;
}) {
  const { t } = useTranslation();
  return (
    <Tip text={t("profile3d.attraction")}>
      <Group gap={6} wrap="nowrap" data-testid={`${testId}-attraction`}>
        <Text size="xs" c="dimmed">
          {t("profile3d.attractionLoose")}
        </Text>
        <Slider
          w={110}
          size="xs"
          min={ATTRACTION_MIN}
          max={ATTRACTION_MAX}
          step={0.1}
          value={value}
          onChange={onChange}
          label={null}
          thumbLabel={t("profile3d.attraction")}
          aria-label={t("profile3d.attraction")}
          data-testid={`${testId}-attraction-slider`}
        />
        <Text size="xs" c="dimmed">
          {t("profile3d.attractionTight")}
        </Text>
      </Group>
    </Tip>
  );
}
