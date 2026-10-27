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
