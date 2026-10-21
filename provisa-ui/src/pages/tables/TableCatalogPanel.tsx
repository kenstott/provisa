// Copyright (c) 2026 Kenneth Stott
// Canary: 5b9e2d14-6c38-4f71-a0d5-8e3f7b2c6a90
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2122: a table's catalogs in its editor. For each catalog of the table's domain it shows the
// asset the table is linked to and when its descriptions were last imported from there, offers
// Refresh from catalog on a linked one, and lets the steward lock the table to one catalog. The
// lock makes that catalog the authority of the table's descriptive fields: the editor shows them
// read-only with a link to edit them in the catalog. Provisa claims no authority it cannot know,
// so without a lock the imported descriptions stay editable.

import { useTranslation } from "react-i18next";
import { Anchor, Button, Group, Paper, Select, Stack, Text } from "@mantine/core";
import { ExternalLink, Lock, RefreshCw } from "lucide-react";
import type { TableCatalog } from "../../api/catalogLinks";

export function TableCatalogPanel({
  catalogs,
  editUrl,
  onRefresh,
  onLock,
  locking,
}: {
  catalogs: TableCatalog[];
  /** The page for the locked asset in its catalog, where its descriptions are edited. */
  editUrl: string | null;
  onRefresh: (catalog: string) => void;
  onLock: (catalog: string | null) => void;
  locking: boolean;
}) {
  const { t, i18n } = useTranslation();
  if (catalogs.length === 0) return null;
  const locked = catalogs.find((c) => c.locked) ?? null;
  const when = (iso: string) =>
    new Date(iso).toLocaleString(i18n.language, { dateStyle: "medium", timeStyle: "short" });
  return (
    <Paper withBorder p="sm" data-testid="table-catalogs">
      <Stack gap="xs">
        {catalogs.map((c) => (
          <Group
            key={c.id}
            justify="space-between"
            wrap="nowrap"
            data-testid={`table-catalog-${c.id}`}
          >
            <Stack gap={0}>
              <Text size="sm" fw={500}>
                {c.id} · {c.provider}
              </Text>
              <Text size="xs" c="dimmed" data-testid={`table-catalog-status-${c.id}`}>
                {c.link
                  ? c.imported_at
                    ? t("catalogLink.importedFrom", {
                        catalog: c.id,
                        when: when(c.imported_at),
                        asset: c.link.name,
                      })
                    : t("catalogLink.linkedTo", { asset: c.link.name })
                  : t("catalogLink.notLinked")}
              </Text>
            </Stack>
            {c.link && (
              <Button
                variant="default"
                size="compact-sm"
                style={{ flexShrink: 0 }}
                leftSection={<RefreshCw size={14} />}
                onClick={() => onRefresh(c.id)}
                data-testid={`table-catalog-refresh-${c.id}`}
              >
                {t("catalogLink.refresh")}
              </Button>
            )}
          </Group>
        ))}
        <Group gap="sm" align="flex-end">
          <Select
            label={t("catalogLink.lockLabel")}
            description={t("catalogLink.lockHelp")}
            size="xs"
            w={320}
            data={[
              { value: "", label: t("catalogLink.lockNone") },
              ...catalogs
                .filter((c) => c.link)
                .map((c) => ({ value: c.id, label: `${c.id} · ${c.provider}` })),
            ]}
            value={locked?.id ?? ""}
            disabled={locking}
            onChange={(v) => onLock(v ? v : null)}
            allowDeselect={false}
            data-testid="table-catalog-lock"
          />
        </Group>
        {locked && (
          <Group gap={6} data-testid="table-catalog-locked">
            <Lock size={14} />
            <Text size="xs">{t("catalogLink.lockedTo", { catalog: locked.id })}</Text>
            {editUrl && (
              <Anchor href={editUrl} target="_blank" rel="noreferrer" size="xs">
                <Group gap={4}>
                  {t("catalogLink.editInCatalog", { catalog: locked.id })}
                  <ExternalLink size={12} />
                </Group>
              </Anchor>
            )}
          </Group>
        )}
      </Stack>
    </Paper>
  );
}
