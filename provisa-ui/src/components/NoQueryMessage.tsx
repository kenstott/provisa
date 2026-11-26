// Copyright (c) 2026 Kenneth Stott
// Canary: d4a82c57-1e9b-4f36-b7c0-5a28e613f9d4
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// THE message of an Explore editor run with no statement in it (see lib/noStatement.ts): said
// where the result would be, before anything is sent. One text, every editor.

import { Text } from "@mantine/core";
import { useTranslation } from "react-i18next";

export function NoQueryMessage({ testId, className }: { testId: string; className?: string }) {
  const { t } = useTranslation();
  return (
    <Text role="status" size="sm" c="dimmed" p="sm" className={className} data-testid={testId}>
      {t("common.noQuery")}
    </Text>
  );
}
