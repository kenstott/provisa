// Copyright (c) 2026 Kenneth Stott
// Canary: 6e0f8b57-2c14-4a63-9d5e-1b7f3a90c862
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useTranslation } from "react-i18next";
import { useNavigate } from "react-router-dom";
import { Anchor, Button, Code, Group, Stack, Text } from "@mantine/core";
import { ModalLayout } from "./modal/ModalLayout";
import { FormFooter } from "./form/FormLayout";
import { Globe, Palette } from "lucide-react";
import { CopyButton } from "./CopyButton";
import { BrandMark } from "./BrandMark";
import { Tip } from "./Tip";

/**
 * The org's own address, told to the administrator once the org exists.
 *
 * REQ-1276 gives every org a subdomain of the deployment, and REQ-1486 lets that org dress it in
 * its own name, colours and logo — together they are the thing an administrator hands to their
 * stakeholders. Neither is discoverable from the workspace, so creation says it out loud.
 *
 * `url` is what the caller resolved from the host (lib/authHost `orgOrigin`); a deployment that
 * addresses no org by name has no such URL and does not render this at all.
 */
export function OrgAddressModal({
  url,
  opened,
  onClose,
}: {
  url: string;
  opened: boolean;
  onClose: () => void;
}) {
  const { t } = useTranslation();
  const navigate = useNavigate();

  return (
    <ModalLayout
      opened={opened}
      onClose={onClose}
      centered
      size="lg"
      data-testid="org-address-modal"
      title={
        <Group gap="sm" align="center" c="var(--primary)">
          <BrandMark size={24} />
          <Text fw={700}>{t("orgAddress.title")}</Text>
        </Group>
      }
    >
      <Stack gap="md">
        <Group gap="sm" align="flex-start" wrap="nowrap">
          <Globe size={18} />
          <Stack gap={4}>
            <Text size="sm">{t("orgAddress.body")}</Text>
            <Group gap="xs" wrap="nowrap">
              <Code data-testid="org-address-url">{url}</Code>
              <Tip text={t("orgAddress.copy")} named>
                <CopyButton text={url} aria-label={t("orgAddress.copy")} />
              </Tip>
            </Group>
            <Anchor href={url} target="_blank" rel="noreferrer" size="sm">
              {t("orgAddress.open")}
            </Anchor>
          </Stack>
        </Group>

        <Group gap="sm" align="flex-start" wrap="nowrap">
          <Palette size={18} />
          <Stack gap={4}>
            <Text size="sm">{t("orgAddress.brandingBody")}</Text>
          </Stack>
        </Group>

        {/* Actions start where the reading does: the one that finishes first. */}
        <FormFooter
          action={
            <Button data-testid="org-address-done" onClick={onClose}>
              {t("orgAddress.doneButton")}
            </Button>
          }
        >
          <Button
            variant="light"
            data-testid="org-address-brand"
            onClick={() => {
              onClose();
              navigate("/team?section=branding");
            }}
          >
            {t("orgAddress.brandingButton")}
          </Button>
        </FormFooter>
      </Stack>
    </ModalLayout>
  );
}
