// Copyright (c) 2026 Kenneth Stott
// Canary: 27629eaa-91f7-4758-a270-7be638d0f5b7
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { Box, List, Text } from "@mantine/core";
import { useTranslation } from "react-i18next";

interface ErrorsPanelProps {
  errors: string[];
}

export function ErrorsPanel({ errors }: ErrorsPanelProps) {
  const { t } = useTranslation();

  if (errors.length === 0) {
    return (
      <Box p="lg" ta="center">
        <Text c="dimmed" fz="sm">
          {t("errorsPanel.noUnsupportedConditions")}
        </Text>
      </Box>
    );
  }

  return (
    <Box p="sm">
      <Text c="var(--destructive)" fz="sm" fw={600} mb="xs">
        {t("errorsPanel.unsupportedConditionsTitle")}
      </Text>
      <List spacing="xs" size="sm">
        {errors.map((e, i) => (
          <List.Item
            key={i}
            styles={{
              itemLabel: {
                fontFamily: "monospace",
                color: "var(--destructive)",
                fontSize: "0.8rem",
              },
            }}
          >
            {e}
          </List.Item>
        ))}
      </List>
    </Box>
  );
}
