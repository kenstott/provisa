// Copyright (c) 2026 Kenneth Stott
// Canary: d83ae114-81d4-4d5c-b79a-0412df654833
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { Button, Modal, Stack, Text } from "@mantine/core";
import { useTranslation } from "react-i18next";
import { useAuth } from "../context/AuthContext";

export function OrgSelector() {
  const { t } = useTranslation();
  const { orgMemberships, activeOrgId, selectOrg } = useAuth();

  const opened = !activeOrgId && orgMemberships.length > 1;

  return (
    <Modal
      opened={opened}
      onClose={() => {}}
      title={t("orgSelector.title")}
      centered
      closeOnClickOutside={false}
      closeOnEscape={false}
      withCloseButton={false}
      data-testid="org-selector-modal"
    >
      <Text mb="md">{t("orgSelector.description")}</Text>
      <Stack gap="xs">
        {orgMemberships.map((m) => (
          <Button
            key={m.org_id}
            onClick={() => selectOrg(m.org_id)}
            data-testid={`org-selector-option-${m.org_id}`}
          >
            {m.org_name}
          </Button>
        ))}
      </Stack>
    </Modal>
  );
}
