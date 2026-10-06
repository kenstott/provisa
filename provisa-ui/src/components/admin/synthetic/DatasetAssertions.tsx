// Copyright (c) 2026 Kenneth Stott
// Canary: 5e8964b8-313e-4c7f-9f22-86b77649658d
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useTranslation } from "react-i18next";
import { ActionIcon, Button, Group, Stack, Text, Textarea, Title } from "@mantine/core";

// REQ-1939, ASSERTIONS: statements over the generated tables, each returning true or false. Each
// runs after generation and its result is reported; none rejects or alters the generated data.
export function DatasetAssertions({
  value,
  onChange,
}: {
  value: string[];
  onChange: (next: string[]) => void;
}) {
  const { t } = useTranslation();
  return (
    <Stack gap="xs" data-testid="synthetic-assertions">
      <Title order={6}>{t("syntheticDatasets.assertionsTitle")}</Title>
      <Text size="xs" c="dimmed">
        {t("syntheticDatasets.assertionsHelp")}
      </Text>
      {value.map((a, i) => (
        <Group key={i} align="start" data-testid={`synthetic-assertion-${i}`}>
          <Textarea
            aria-label={t("syntheticDatasets.assertion")}
            autosize
            minRows={1}
            style={{ flex: 1 }}
            placeholder="SELECT COUNT(*) = 0 FROM sales.orders WHERE amount < 0"
            value={a}
            onChange={(e) => {
              const v = e.currentTarget.value;
              onChange(value.map((x, j) => (j === i ? v : x)));
            }}
          />
          <ActionIcon
            variant="subtle"
            aria-label={t("syntheticDatasets.remove")}
            onClick={() => onChange(value.filter((_, j) => j !== i))}
          >
            ×
          </ActionIcon>
        </Group>
      ))}
      <Group>
        <Button
          size="xs"
          variant="light"
          onClick={() => onChange([...value, ""])}
          data-testid="synthetic-add-assertion"
        >
          {t("syntheticDatasets.addAssertion")}
        </Button>
      </Group>
    </Stack>
  );
}
