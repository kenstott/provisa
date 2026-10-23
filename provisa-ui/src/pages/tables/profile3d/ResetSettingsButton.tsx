// Copyright (c) 2026 Kenneth Stott
// Canary: 7d93b43c-2424-41be-aaa9-51c652122a92
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// A 3D graph's "Reset view settings": clears what the graph remembers (graphSettings.ts) and puts
// its layout, layers, toggles and filters back to their defaults. The camera is "Reset view".

import { useTranslation } from "react-i18next";
import { Button } from "@mantine/core";
import { Tip } from "../../../components/Tip";

export function ResetSettingsButton({ onReset, testId }: { onReset: () => void; testId: string }) {
  const { t } = useTranslation();
  return (
    <Tip text={t("profile3d.resetSettingsHelp")}>
      <Button size="xs" variant="subtle" onClick={onReset} data-testid={`${testId}-reset-settings`}>
        {t("profile3d.resetSettings")}
      </Button>
    </Tip>
  );
}
