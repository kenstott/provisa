// Copyright (c) 2026 Kenneth Stott
// Canary: 0fe5bb75-3b16-49c0-bad8-b22b5eb80b75
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useEffect, useState } from "react";
import { Button, Modal, Stack, Text } from "@mantine/core";
import { useTranslation } from "react-i18next";
import { useAuth } from "../context/AuthContext";
import { orgSelectionRequired, subscribeOrgSelection } from "../lib/orgSelection";

/**
 * REQ-1935: the answer to a request refused because it named no org.
 *
 * No org is implied for anyone, a platform operator included, so a session with none selected is
 * refused by the server on every data surface. Rather than each page showing that refusal as its
 * own error, this asks for the one thing that resolves it: an org. It offers the orgs the user
 * belongs to -- naming any other is refused too (REQ-1327) -- and says so when there are none.
 */
export function OrgSelectionPrompt() {
  const { t } = useTranslation();
  const { orgMemberships, selectOrg } = useAuth();
  const [required, setRequired] = useState(orgSelectionRequired);

  useEffect(() => subscribeOrgSelection(setRequired), []);

  return (
    <Modal
      opened={required}
      onClose={() => {
        /* not dismissible: every data request is refused until an org is selected */
      }}
      title={t("orgSelectionPrompt.title")}
      centered
      closeOnClickOutside={false}
      closeOnEscape={false}
      withCloseButton={false}
      data-testid="org-selection-prompt"
    >
      {orgMemberships.length > 0 ? (
        <>
          <Text mb="md">{t("orgSelectionPrompt.description")}</Text>
          <Stack gap="xs">
            {orgMemberships.map((m) => (
              <Button
                key={m.org_id}
                onClick={() => selectOrg(m.org_id)}
                data-testid={`org-selection-option-${m.org_id}`}
              >
                {m.org_name}
              </Button>
            ))}
          </Stack>
        </>
      ) : (
        <Text data-testid="org-selection-none">{t("orgSelectionPrompt.noMemberships")}</Text>
      )}
    </Modal>
  );
}
