// Copyright (c) 2026 Kenneth Stott
// Canary: 996c93ba-f215-4f6c-a479-a259b5a4fa28
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// The floor grid's checkbox in a 3D network graph's toolbar (one of the graph's remembered settings, graphSettings.ts).

import { useTranslation } from "react-i18next";
import { Chip } from "@mantine/core";

export function GridToggle({
  value,
  onChange,
  testId,
}: {
  value: boolean;
  onChange: (shown: boolean) => void;
  testId: string;
}) {
  const { t } = useTranslation();
  return (
    <Chip size="xs" checked={value} onChange={onChange} data-testid={`${testId}-grid`}>
      {t("profile3d.grid")}
    </Chip>
  );
}
