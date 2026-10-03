// Copyright (c) 2026 Kenneth Stott
// Canary: 0ceef83f-384a-42af-8fcc-86591b4f11e6
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { Anchor, Button, Group, List, Modal, Stack, Text } from "@mantine/core";
import type { ReactNode } from "react";
import { Link } from "react-router-dom";
import { useTranslation } from "react-i18next";
import type { Dependent } from "../lib/dependents";

/** The admin page where an object of each kind is removed or changed. */
const PAGE_OF_KIND: Record<string, string> = {
  table: "/tables",
  column: "/tables",
  tag_assignment: "/tables",
  relationship: "/relationships",
  role: "/security/roles",
  role_assignment: "/team",
  row_filter: "/security/rls",
  metric: "/metrics",
  materialized_view: "/views",
  command: "/commands",
  webhook: "/commands",
  data_product: "/data-products",
  source: "/sources",
  remote_registration: "/sources",
  domain: "/admin",
  glossary_term: "/admin/glossary",
  // What refers to an environment (REQ-1918): the people pinned to it are on the team page.
  membership: "/team",
};

interface DialogProps {
  /** What the operator asked to delete, as they know it. */
  subject: string;
  dependents: Dependent[];
  onClose: () => void;
  /** What can be done about one dependent right here (a role's grant removed), if anything. */
  itemAction?: (dependent: Dependent) => ReactNode;
}

/** "Cannot delete X yet": every object that still refers to it, grouped by kind, each linked to
 *  the page where it is removed or changed. There is nothing to confirm — the delete did not
 *  happen — so the only action is to close. */
export function DependentsDialog({ subject, dependents, onClose, itemAction }: DialogProps) {
  const { t } = useTranslation();
  const kinds = Array.from(new Set(dependents.map((d) => d.kind)));
  return (
    <Modal
      opened
      onClose={onClose}
      title={t("dependentsDialog.title", { name: subject })}
      centered
      size="lg"
    >
      <Text mb="md">{t("dependentsDialog.intro")}</Text>
      <Stack gap="md">
        {kinds.map((kind) => {
          const page = PAGE_OF_KIND[kind];
          const ofKind = dependents.filter((d) => d.kind === kind);
          return (
            <div key={kind} data-testid={`dependents-${kind}`}>
              <Group justify="space-between" mb={4}>
                <Text fw={600}>{t(`dependentsDialog.kind.${kind}`)}</Text>
                {page && (
                  <Anchor component={Link} to={page} onClick={onClose} size="sm">
                    {t("dependentsDialog.open")}
                  </Anchor>
                )}
              </Group>
              <List size="sm">
                {ofKind.map((d) => (
                  <List.Item key={`${d.kind}:${d.id}`}>
                    <Group gap="xs" wrap="nowrap">
                      <span>{d.name || String(d.id)}</span>
                      {itemAction?.(d)}
                    </Group>
                  </List.Item>
                ))}
              </List>
            </div>
          );
        })}
      </Stack>
      <Group justify="flex-end" mt="lg">
        <Button onClick={onClose}>{t("dependentsDialog.close")}</Button>
      </Group>
    </Modal>
  );
}
