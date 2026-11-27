// Copyright (c) 2026 Kenneth Stott
// Canary: c9ea3478-802e-43ee-b3eb-a8a142de1e3d
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// What's new in the release this UI was built with (lib/releaseNotes.ts).

import { useTranslation } from "react-i18next";
import { List, Stack, Text, Title } from "@mantine/core";
import { ModalLayout } from "./modal/ModalLayout";
import type { ReleaseNotes } from "../lib/releaseNotes";

export function WhatsNewModal({
  notes,
  opened,
  onClose,
}: {
  notes: ReleaseNotes;
  opened: boolean;
  onClose: () => void;
}) {
  const { t } = useTranslation();
  const empty = notes.features.length === 0 && notes.changes.length === 0;
  return (
    <ModalLayout
      opened={opened}
      onClose={onClose}
      size="lg"
      title={t("whatsNew.title", { version: notes.version })}
      data-testid="whats-new"
    >
      <Stack gap="md">
        <Text size="sm" c="dimmed">
          {t("whatsNew.released", { date: notes.date })}
        </Text>
        {empty && <Text size="sm">{t("whatsNew.empty")}</Text>}
        {notes.features.length > 0 && (
          <Stack gap="xs" data-testid="whats-new-features">
            <Title order={4}>{t("whatsNew.features")}</Title>
            {notes.features.map((group) => (
              <Stack key={group.category} gap={4}>
                <Title order={5}>{group.category}</Title>
                <List size="sm" spacing={4}>
                  {group.entries.map((entry) => (
                    <List.Item key={entry.text}>
                      {entry.text}
                      {entry.changes.length > 0 && (
                        <List size="sm" withPadding>
                          {entry.changes.map((change) => (
                            <List.Item key={change}>{change}</List.Item>
                          ))}
                        </List>
                      )}
                    </List.Item>
                  ))}
                </List>
              </Stack>
            ))}
          </Stack>
        )}
        {notes.changes.length > 0 && (
          <Stack gap="xs" data-testid="whats-new-changes">
            <Title order={4}>{t("whatsNew.changes")}</Title>
            {notes.changes.map((group) => (
              <Stack key={group.scope} gap={4}>
                <Title order={5}>{group.scope || t("whatsNew.general")}</Title>
                <List size="sm" spacing={4}>
                  {group.entries.map((text) => (
                    <List.Item key={text}>{text}</List.Item>
                  ))}
                </List>
              </Stack>
            ))}
          </Stack>
        )}
      </Stack>
    </ModalLayout>
  );
}
