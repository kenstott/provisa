// Copyright (c) 2026 Kenneth Stott
// Canary: 1b169835-e357-4970-aa2e-63b3ae70304e
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1922: one region selector, the same on every admin list of objects that can name a region. It
// starts at the connected region (its default comes from useRegionSelection), offers all regions, no
// region, and each other region by name, and says how many objects the selection hides. It is absent
// when the deployment declares no regions.

import { Group, Select, Text } from "@mantine/core";
import { useTranslation } from "react-i18next";
import {
  REGION_ALL,
  REGION_NONE,
  selectionValues,
  type RegionSelection,
} from "../hooks/regionFilter";

export interface RegionSelectorProps {
  regions: string[];
  connected: string | null;
  value: RegionSelection;
  onChange: (selection: RegionSelection) => void;
  /** How many rows the current selection hides, shown beside the selector so nothing looks missing. */
  hidden: number;
  /** REQ-1922: the region-choices query failed. Shown as an error, never hidden — a failure must not
   * read as "this deployment has no regions". */
  error?: boolean;
}

export function RegionSelector({
  regions,
  connected,
  value,
  onChange,
  hidden,
  error,
}: RegionSelectorProps) {
  const { t } = useTranslation();
  // A failed load is an error to show, not an absence: a 503 must never masquerade as "no regions".
  if (error) {
    return (
      <Text size="xs" c="red" data-testid="region-load-error">
        {t("regionSelector.loadError")}
      </Text>
    );
  }
  // Absent when the deployment declares no regions (like every region control).
  if (regions.length === 0) return null;

  const data = selectionValues(regions, connected).map((v) => ({
    value: v,
    label:
      v === REGION_ALL
        ? t("regionSelector.allRegions")
        : v === REGION_NONE
          ? t("regionSelector.noRegion")
          : v === connected
            ? t("regionSelector.connectedRegion", { region: v })
            : v,
  }));

  return (
    <Group gap="xs" align="center" wrap="nowrap">
      <Select
        aria-label={t("regionSelector.label")}
        data={data}
        value={value}
        onChange={(v) => v && onChange(v)}
        allowDeselect={false}
        size="xs"
        w={220}
      />
      {hidden > 0 && (
        <Text size="xs" c="dimmed" data-testid="region-hidden-count">
          {t("regionSelector.hidden", { count: hidden })}
        </Text>
      )}
    </Group>
  );
}
