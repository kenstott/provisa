// Copyright (c) 2026 Kenneth Stott
// Canary: 72c4e9b0-5d1a-4f38-9b6e-a0f3d8c5e217
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2160: INPUT PORTS AND CONSUMERS ARE ALWAYS SHOWN -- what a list read from the product's
// lineage shows before it is read, and what any reader but the owner sees in its place.

import { useTranslation } from "react-i18next";
import { Text } from "@mantine/core";
import { Empty } from "../DetailCard";

/** Any reader but the owner or team sees that the list exists and whose it is, never a list and
 *  never "None" (an empty list is the owner's to read). */
export function OwnerOnlyList() {
  const { t } = useTranslation();
  return <Empty text={t("dataProductCore.ownerOnlyList")} />;
}

/** The owner's list while lineage is unread: loading, or the read's error; never a false "None". */
export function LineagePending({ loading, error }: { loading: boolean; error: string }) {
  const { t } = useTranslation();
  if (loading) return <Empty text={t("dataProductsTab.detail.lineageLoading")} />;
  return error ? (
    <Text size="sm" c="red">
      {error}
    </Text>
  ) : null;
}
