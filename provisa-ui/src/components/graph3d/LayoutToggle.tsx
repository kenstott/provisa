// Copyright (c) 2026 Kenneth Stott
// Canary: 2d7a9c40-5e13-4b8f-a6d2-c0e48b1f7395
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2174: a network graph's Force / Layered choice. Layered is disabled where a cycle among the
// edges that rank or flow makes layers impossible.

import { SegmentedControl } from "@mantine/core";
import { useTranslation } from "react-i18next";
import type { LayoutKind } from "./graphModel";

export function LayoutToggle({
  layout,
  layeredPossible,
  onChange,
  testId,
}: {
  layout: LayoutKind;
  layeredPossible: boolean;
  onChange: (layout: LayoutKind) => void;
  testId: string;
}) {
  const { t } = useTranslation();
  return (
    <SegmentedControl
      size="xs"
      value={layout}
      onChange={(v) => onChange(v as LayoutKind)}
      data={[
        { value: "force", label: t("graph3d.layoutForce") },
        { value: "layered", label: t("graph3d.layoutLayered"), disabled: !layeredPossible },
      ]}
      aria-label={t("graph3d.layout")}
      data-testid={`${testId}-layout`}
    />
  );
}
