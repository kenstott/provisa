// Copyright (c) 2026 Kenneth Stott
// Canary: 55f45bb1-1565-47a4-9784-bd8dd6eb3854
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useTranslation } from "react-i18next";
import { Center, Loader, Stack, Text } from "@mantine/core";

/**
 * REQ-1430: the route-level loading state. A bare "Loading..." pinned to the top-left corner reads
 * as a page that rendered wrong; the spinner and its message sit in the middle of the viewport so
 * it reads as work in progress.
 */
export function PageLoading({ message }: { message?: string }) {
  const { t } = useTranslation();
  return (
    <Center mih="60vh" data-testid="page-loading">
      <Stack align="center" gap="sm">
        <Loader size={56} />
        <Text size="sm" c="dimmed">
          {message ?? t("common.loading")}
        </Text>
      </Stack>
    </Center>
  );
}
