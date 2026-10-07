// Copyright (c) 2026 Kenneth Stott
// Canary: 2e6f9a40-1c7b-4d85-b3a2-9f50e8d17c34
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useTranslation } from "react-i18next";
import { Badge, Button, Group, Modal, Paper, SimpleGrid, Stack, Text, UnstyledButton } from "@mantine/core";
import { Check } from "lucide-react";
import type { TopicId } from "./tourSteps";

/**
 * REQ-1945: the Deep Dives menu -- one card per topic with its title, one sentence, its step count
 * and a mark once the viewer has completed it. Each topic is its own tour; Done or leaving one
 * returns here. A topic none of whose steps this viewer's rights open is not listed.
 */
export function TourMenu({
  opened,
  topics,
  completed,
  onPick,
  onCore,
  onClose,
}: {
  opened: boolean;
  /** Topics to list, in menu order, with the number of steps this viewer would be shown. */
  topics: { id: TopicId; steps: number }[];
  completed: readonly TopicId[];
  onPick: (id: TopicId) => void;
  /** Begin the core tour at its first step. */
  onCore: () => void;
  onClose: () => void;
}) {
  const { t } = useTranslation();
  return (
    <Modal
      opened={opened}
      onClose={onClose}
      title={t("tour.menu.title")}
      centered
      size="xl"
      transitionProps={{ duration: 0 }}
      data-testid="tour-menu"
    >
      <Stack gap="md">
        <Group justify="space-between" align="flex-start" wrap="nowrap">
          <Text size="sm">{t("tour.menu.intro")}</Text>
          <Button variant="light" size="xs" onClick={onCore} data-testid="tour-menu-core-tour">
            {t("tour.menu.coreTour")}
          </Button>
        </Group>
        <SimpleGrid cols={{ base: 1, sm: 2 }} spacing="sm">
          {topics.map(({ id, steps }) => (
            <UnstyledButton
              key={id}
              data-testid={`tour-topic-${id}`}
              onClick={() => onPick(id)}
              aria-label={t(`tour.topics.${id}.title`)}
            >
              <Paper withBorder p="md" radius="md" h="100%">
                <Stack gap={6}>
                  <Group justify="space-between" wrap="nowrap" align="flex-start">
                    <Text fw={600}>{t(`tour.topics.${id}.title`)}</Text>
                    {completed.includes(id) && (
                      <Badge
                        color="green"
                        variant="light"
                        leftSection={<Check size={12} aria-hidden />}
                        data-testid={`tour-topic-done-${id}`}
                      >
                        {t("tour.menu.completed")}
                      </Badge>
                    )}
                  </Group>
                  <Text size="sm" c="dimmed">
                    {t(`tour.topics.${id}.summary`)}
                  </Text>
                  <Text size="xs" c="dimmed">
                    {t("tour.menu.steps", { count: steps })}
                  </Text>
                </Stack>
              </Paper>
            </UnstyledButton>
          ))}
        </SimpleGrid>
      </Stack>
    </Modal>
  );
}
