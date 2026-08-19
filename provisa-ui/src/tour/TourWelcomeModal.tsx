// Copyright (c) 2026 Kenneth Stott
// Canary: 7fabd614-445d-4230-8a92-e0c321c09105
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useState } from "react";
import { useTranslation } from "react-i18next";
import { Button, Checkbox, Group, Modal, Stack, Text, Title } from "@mantine/core";
import { BrandMark } from "../components/BrandMark";
import { declineTour, tourResumeStep, useTour } from "./useTour";

/**
 * Offers the guided tour instead of launching it unasked.
 *
 * The caller decides when this renders (once per session, never after the tour has been taken or
 * permanently declined); this component owns only the choice the modal presents.
 */
export function TourWelcomeModal({ onClose }: { onClose: () => void }) {
  const { t } = useTranslation();
  const { startTour } = useTour();
  const [never, setNever] = useState(false);
  const resumeStep = tourResumeStep();

  const close = () => {
    if (never) declineTour();
    onClose();
  };

  return (
    <Modal
      opened
      onClose={close}
      // The offer is the product introducing itself, so it carries the mark and the wordmark the
      // navbar and the landing page carry — the same BrandMark, not a second drawing of it.
      title={
        <Group gap="sm" align="center" c="var(--primary)">
          <BrandMark size={28} />
          <Stack gap={0}>
            <Text size="sm" fw={700} lh={1.1}>
              {t("navBar.brand")}
            </Text>
            <Title order={4} c="var(--text)" lh={1.2}>
              {t("tour.welcome.title")}
            </Title>
          </Stack>
        </Group>
      }
      centered
      size="lg"
    >
      <Stack gap="md">
        <Text size="sm">{t("tour.welcome.body")}</Text>
        <Checkbox
          label={t("tour.welcome.never")}
          checked={never}
          onChange={(e) => setNever(e.currentTarget.checked)}
        />
        <Group justify="flex-end">
          <Button variant="subtle" onClick={close}>
            {t("tour.welcome.later")}
          </Button>
          {resumeStep !== null && (
            <Button
              variant="light"
              onClick={() => {
                close();
                startTour({ restart: true });
              }}
            >
              {t("tour.welcome.restart")}
            </Button>
          )}
          <Button
            onClick={() => {
              close();
              // A saved step resumes; otherwise this is a fresh run from the top.
              startTour(resumeStep === null ? { restart: true } : undefined);
            }}
          >
            {resumeStep === null ? t("tour.welcome.start") : t("tour.welcome.resume")}
          </Button>
        </Group>
      </Stack>
    </Modal>
  );
}
