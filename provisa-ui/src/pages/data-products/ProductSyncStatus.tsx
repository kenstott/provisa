// Copyright (c) 2026 Kenneth Stott
// Canary: 2a9d4f71-6c3e-4b08-a5e2-7f1b0c8d3e96
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2259: each data product's sync status on the Data Products list, from the record of every
// publish: its latest outcome on each data catalog that exports. It links to Context Sync filtered
// to the product, drawn as a link only for a viewer holding that page's right.

import { useTranslation } from "react-i18next";
import { Badge, Stack, Text } from "@mantine/core";
import type { ProductSyncStatus } from "../../api/syncing";
import { EntityPageLink } from "../../components/EntityLink";
import { TipSurface } from "../../components/Tip";
import { syncHref, syncLabel, syncState } from "./productSync";

export function ProductSyncCell({
  productId,
  statuses,
}: {
  productId: string;
  statuses: ProductSyncStatus[];
}) {
  const { t, i18n } = useTranslation();
  const state = syncState(statuses);
  const testId = `data-products-sync-${productId}`;
  if (state === "none") {
    return (
      <Text size="xs" c="dimmed" data-testid={testId}>
        {syncLabel(statuses, t)}
      </Text>
    );
  }
  const when = (iso: string) =>
    new Date(iso).toLocaleString(i18n.language, { dateStyle: "medium", timeStyle: "short" });
  return (
    <TipSurface
      content={
        <Stack gap={2}>
          {statuses.map((s) => (
            <Text key={s.entry} size="xs">
              {t(
                s.outcome === "failed"
                  ? "dataProductsTab.syncEntryFailed"
                  : "dataProductsTab.syncEntryPublished",
                { entry: s.entry, when: when(s.published_at) },
              )}
              {s.error !== null && ` — ${s.error}`}
            </Text>
          ))}
        </Stack>
      }
    >
      <span>
        <EntityPageLink href={syncHref(productId)} testId={testId}>
          <Badge size="sm" variant="light" color={state === "failed" ? "red" : "green"}>
            {syncLabel(statuses, t)}
          </Badge>
        </EntityPageLink>
      </span>
    </TipSurface>
  );
}
