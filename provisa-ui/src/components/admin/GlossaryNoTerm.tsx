// Copyright (c) 2026 Kenneth Stott
// Canary: 7a2d5c90-e4b1-4f36-9c08-b3e61f7d2a45
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useTranslation } from "react-i18next";
import { Stack, Text } from "@mantine/core";
import { EntityPageLink } from "../EntityLink";

/** REQ-2259: the Ontology page with no term open -- a pointer to Context Health, where the
 * stewardship summary (REQ-2144) now lives. */
export function GlossaryNoTerm() {
  const { t } = useTranslation();
  return (
    <Stack gap={4} py="md" align="flex-start" data-testid="glossary-no-term">
      <Text size="sm" c="dimmed">
        {t("glossaryTab.selectTerm")}
      </Text>
      <Text size="sm">
        <EntityPageLink href="/context/health" testId="glossary-health-link">
          {t("glossaryTab.stewardshipOnHealth")}
        </EntityPageLink>
      </Text>
    </Stack>
  );
}
