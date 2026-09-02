// Copyright (c) 2026 Kenneth Stott
// Canary: 36902871-f5a8-4277-aa7b-ad3e2da7cb87
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useState, type ReactNode } from "react";
import { Alert, Badge, Box, Button, Group, Modal, Stack, Text, Tooltip } from "@mantine/core";
import { Info } from "lucide-react";
import { useTranslation } from "react-i18next";

interface Props {
  children: ReactNode;
  /**
   * The gate owns a route body: the page itself is the demonstration, so it gets a banner across
   * the top saying what it is and the page beneath it goes inert. Inline gates get a badge beside
   * the control instead.
   */
  block?: boolean;
  /**
   * A link INTO a demonstrated region (a nav entry). It stays clickable -- the demonstration is on
   * the far side of it, and a nav entry that refuses the click shows nothing at all.
   */
  navigable?: boolean;
  /**
   * The right being demonstrated. A right with a `demonstrated.purpose.<right>` entry opens that
   * explanation over the region on arrival: the banner says the surface is not available, and the
   * question a visitor is left with is what it would have been FOR.
   */
  capability?: string;
}

/**
 * REQ-1602: a surface the current role is SHOWN but may not use.
 *
 * The sandbox role (REQ-1597) is org_admin minus six rights, and a visitor holding it is being
 * shown the product. Removing those surfaces from the page would advertise a smaller product than
 * the one being sold, so they stay where they are -- labelled with what they are: part of the
 * production system, not of the sandbox. The children are rendered exactly as a holder of the right
 * would see them; what is taken away is the ability to act through them.
 */
export function DemonstratedFeature({ children, block, navigable, capability }: Props) {
  const { t, i18n } = useTranslation();
  const purpose = `capabilityGate.demonstrated.purpose.${capability}`;
  // Only a right that HAS an explanation gets the modal -- an absent key would otherwise open a
  // dialog showing its own name.
  const explained = block && capability !== undefined && i18n.exists(`${purpose}.title`);
  const [explaining, setExplaining] = useState(true);
  const badge = (
    <Badge size="sm" variant="light" color="gray" data-testid="demonstrated-badge">
      {t("capabilityGate.demonstrated.badge")}
    </Badge>
  );
  if (block) {
    // `inert` is what keeps the demonstration truthful: the page keeps its real appearance, and the
    // browser itself refuses the click, the focus and the keyboard rather than each control
    // remembering to. Screen readers are told the same thing by aria-disabled.
    return (
      <Box>
        {explained && (
          <Modal
            opened={explaining}
            onClose={() => setExplaining(false)}
            title={t(`${purpose}.title`)}
            centered
            size="lg"
            data-testid="demonstrated-purpose-modal"
          >
            <Stack gap="md">
              {t(`${purpose}.body`)
                .split("\n\n")
                .map((para) => (
                  <Text key={para}>{para}</Text>
                ))}
              <Group justify="flex-end">
                <Button onClick={() => setExplaining(false)}>
                  {t("capabilityGate.demonstrated.dismiss")}
                </Button>
              </Group>
            </Stack>
          </Modal>
        )}
        <Alert
          icon={<Info size={16} aria-hidden />}
          color="gray"
          variant="light"
          mb="md"
          title={t("capabilityGate.demonstrated.badge")}
          data-testid="demonstrated-banner"
        >
          {t("capabilityGate.demonstrated.explanation")}
        </Alert>
        <Box aria-disabled inert style={{ opacity: 0.55 }} data-testid="demonstrated-children">
          {children}
        </Box>
      </Box>
    );
  }
  return (
    <Tooltip label={t("capabilityGate.demonstrated.explanation")} multiline w={280} withArrow>
      <Group gap="xs" wrap="nowrap" display="inline-flex">
        {navigable ? (
          // Clickable, and dimmed rather than greyed out: it leads somewhere, and where it leads
          // says the rest.
          <Box component="span" display="inline-block" style={{ opacity: 0.7 }}>
            {children}
          </Box>
        ) : (
          <Box
            component="span"
            display="inline-block"
            aria-disabled
            inert
            style={{ opacity: 0.55, filter: "grayscale(1)" }}
            data-testid="demonstrated-children"
          >
            {children}
          </Box>
        )}
        {badge}
      </Group>
    </Tooltip>
  );
}
