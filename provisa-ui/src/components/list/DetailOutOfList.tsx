// Copyright (c) 2026 Kenneth Stott
// Canary: ec5f602f-9f6a-45a1-851f-4026007ade3b
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2277 rule 9: an address naming an item the list leaves out opens no detail. The page says
// once, above the list, which item the address named and why it is not listed, with the one action
// that brings it into view. Its close drops the item from the address.

import { Alert, Button, Group, Text } from "@mantine/core";
import { useTranslation } from "react-i18next";
import type { OutOfList } from "./useDetailParam";

export function DetailOutOfList({ outOfList }: { outOfList: OutOfList | null }) {
  const { t } = useTranslation();
  if (outOfList === null) return null;
  return (
    <Alert
      color="yellow"
      mb="sm"
      withCloseButton
      closeButtonLabel={t("detailInList.dismiss")}
      onClose={outOfList.dismiss}
      title={t("detailInList.title", { name: outOfList.name })}
      data-testid="detail-out-of-list"
    >
      <Group justify="space-between" gap="xs" wrap="wrap">
        <Text size="sm">{t("detailInList.why", { reasons: outOfList.reasons.join("; ") })}</Text>
        <Button
          size="xs"
          variant="light"
          onClick={outOfList.bringIn}
          data-testid="detail-out-of-list-show"
        >
          {outOfList.action}
        </Button>
      </Group>
    </Alert>
  );
}
