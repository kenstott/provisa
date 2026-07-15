// Copyright (c) 2026 Kenneth Stott
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { ActionIcon, useMantineColorScheme } from "@mantine/core";
import { Moon, Sun } from "lucide-react";
import { useTranslation } from "react-i18next";

/** Light/Dark color-scheme toggle (REQ-1011). Persists via Mantine's
 *  localStorage-backed color-scheme manager. */
export function ColorSchemeToggle() {
  const { colorScheme, setColorScheme } = useMantineColorScheme();
  const { t } = useTranslation();
  const isDark = colorScheme === "dark";
  return (
    <ActionIcon
      variant="default"
      size="lg"
      aria-label={t("theme.toggle")}
      data-testid="color-scheme-toggle"
      onClick={() => setColorScheme(isDark ? "light" : "dark")}
    >
      {isDark ? <Sun size={18} aria-hidden /> : <Moon size={18} aria-hidden />}
    </ActionIcon>
  );
}
