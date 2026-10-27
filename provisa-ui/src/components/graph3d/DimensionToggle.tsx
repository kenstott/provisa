// Copyright (c) 2026 Kenneth Stott
// Canary: db7ede16-1597-47c5-88b0-8a0f14e427a7
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2174: a network graph's 2D/3D choice. In 2D the same stage lays the graph out flat and looks
// at it face-on through an orthographic camera, panned and zoomed, never turning.

import { SegmentedControl } from "@mantine/core";
import { useTranslation } from "react-i18next";

export function DimensionToggle({
  flat,
  onChange,
  testId,
}: {
  flat: boolean;
  onChange: (flat: boolean) => void;
  testId: string;
}) {
  const { t } = useTranslation();
  return (
    <SegmentedControl
      size="xs"
      value={flat ? "2d" : "3d"}
      onChange={(v) => onChange(v === "2d")}
      data={[
        { value: "2d", label: t("graph3d.view2d") },
        { value: "3d", label: t("graph3d.view3d") },
      ]}
      aria-label={t("graph3d.dimension")}
      data-testid={`${testId}-dimension`}
    />
  );
}
