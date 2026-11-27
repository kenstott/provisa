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
import {
  Badge,
  Button,
  Group,
  Paper,
  SimpleGrid,
  Stack,
  Text,
  UnstyledButton,
} from "@mantine/core";
import { ModalLayout } from "../components/modal/ModalLayout";
import { FormFooter } from "../components/form/FormLayout";
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
  demo,
  onDemo,
  onPick,
  onCore,
  onClose,
}: {
  opened: boolean;
  /** Topics to list, in menu order, with the number of steps this viewer would be shown. */
  topics: { id: TopicId; steps: number }[];
  completed: readonly TopicId[];
  /** REQ-2010: the named demo's own track, listed first, or null when no named demo has one. */
  demo: { name: string; steps: number; done: boolean } | null;
  /** Begin the named demo's track at its first step. */
  onDemo: () => void;
  onPick: (id: TopicId) => void;
  /** Begin the core tour at its first step; null when the org lacks what it shows (REQ-2013). */
  onCore: (() => void) | null;
  onClose: () => void;
}) {
  const { t } = useTranslation();
  return (
    <ModalLayout
      opened={opened}
      onClose={onClose}
      title={t("tour.menu.title")}
      centered
      size="xl"
      transitionProps={{ duration: 0 }}
    >
      {/* The test id is on the menu's content: Mantine's Modal root has no box of its own, so a
          browser test reading it for visibility sees it hidden while the menu is on screen. */}
      <Stack gap="md" data-testid="tour-menu">
        <Text size="sm">{t("tour.menu.intro")}</Text>
        {/* REQ-2277 rule 10: the menu's own action, in the dialog's bar. */}
        {onCore && (
          <FormFooter
            action={
              <Button variant="light" onClick={onCore} data-testid="tour-menu-core-tour">
                {t("tour.menu.coreTour")}
              </Button>
            }
          />
        )}
        {demo && (
          <UnstyledButton
            data-testid="tour-demo-track"
            onClick={onDemo}
            aria-label={t(`tour.demos.${demo.name}.title`)}
          >
            <Paper withBorder p="md" radius="md">
              <Stack gap={6}>
                <Group justify="space-between" wrap="nowrap" align="flex-start">
                  <Group gap="xs" wrap="nowrap">
                    <Badge variant="light">{t("tour.menu.thisDemo")}</Badge>
                    <Text fw={600}>{t(`tour.demos.${demo.name}.title`)}</Text>
                  </Group>
                  {demo.done && (
                    <Badge
                      color="green"
                      variant="light"
                      leftSection={<Check size={12} aria-hidden />}
                      data-testid="tour-demo-track-done"
                    >
                      {t("tour.menu.completed")}
                    </Badge>
                  )}
                </Group>
                <Text size="sm" c="dimmed">
                  {t(`tour.demos.${demo.name}.summary`)}
                </Text>
                <Text size="xs" c="dimmed">
                  {t("tour.menu.steps", { count: demo.steps })}
                </Text>
              </Stack>
            </Paper>
          </UnstyledButton>
        )}
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
    </ModalLayout>
  );
}
