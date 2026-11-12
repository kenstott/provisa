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
import { commandId, detailHref } from "./list/useDetailParam";

/** The link into each kind's detail panel on its page (REQ-2240). A kind whose page has no panel
 *  to open (a role assignment, a member, an environment) has none. */
/** What the server reports beside a dependent's id for the kinds linked through another object. */
function owner(d: Dependent, key: "table_id" | "source_id"): string | number {
  const value = d[key];
  if (value === undefined) throw new Error(`a ${d.kind} dependent without its ${key}`);
  return value;
}

const LINK_OF_KIND: Record<string, (d: Dependent) => string> = {
  table: (d) => detailHref("tables", d.id),
  // A column grant is changed on its table's Columns tab.
  column: (d) => detailHref("tables", owner(d, "table_id"), "columns"),
  relationship: (d) => detailHref("relationships", d.id),
  role: (d) => detailHref("roles", d.id),
  row_filter: (d) => detailHref("rls", d.id),
  metric: (d) => detailHref("metrics", d.id),
  // A function and a webhook may share a name: each opens its own panel.
  command: (d) => detailHref("commands", commandId("function", String(d.id))),
  webhook: (d) => detailHref("commands", commandId("webhook", String(d.id))),
  data_product: (d) => detailHref("products", d.id),
  source: (d) => detailHref("sources", d.id),
  remote_registration: (d) => detailHref("sources", owner(d, "source_id")),
  domain: (d) => detailHref("domains", d.id),
  glossary_term: (d) => detailHref("ontology", d.id),
};

interface DialogProps {
  /** What the operator asked to delete, as they know it. */
  subject: string;
  dependents: Dependent[];
  onClose: () => void;
  /** What can be done about one dependent right here (a role's grant removed), if anything. */
  itemAction?: (dependent: Dependent) => ReactNode;
}

/** "Cannot delete X yet": every object that still refers to it, grouped by kind, each named as
 *  its own page names it and linked to its detail panel there. There is nothing to confirm -- the
 *  delete did not happen -- so the only action is to close. */
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
          const linkOf = LINK_OF_KIND[kind];
          const ofKind = dependents.filter((d) => d.kind === kind);
          return (
            <div key={kind} data-testid={`dependents-${kind}`}>
              <Text fw={600} mb={4}>
                {t(`dependentsDialog.kind.${kind}`)}
              </Text>
              <List size="sm">
                {ofKind.map((d) => (
                  <List.Item key={`${d.kind}:${d.id}:${d.label}`}>
                    <Group gap="xs" wrap="nowrap">
                      {linkOf ? (
                        <Anchor component={Link} to={linkOf(d)} onClick={onClose}>
                          {d.label}
                        </Anchor>
                      ) : (
                        <span>{d.label}</span>
                      )}
                      {itemAction?.(d)}
                    </Group>
                  </List.Item>
                ))}
              </List>
            </div>
          );
        })}
      </Stack>
      <Group justify="flex-start" mt="lg">
        <Button onClick={onClose}>{t("dependentsDialog.close")}</Button>
      </Group>
    </Modal>
  );
}
