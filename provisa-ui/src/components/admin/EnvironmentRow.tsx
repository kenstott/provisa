// Copyright (c) 2026 Kenneth Stott
// Canary: 9a1ce59c-2e33-4511-b20a-68f4525e715b
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// One environment's row on Admin › Environments: its name, what it inherits, its stage, where its
// branch stands against the remote, who made it, when it expires and whether it is protected. The
// row only selects: a click opens the environment's panel, whose head holds what can be done to it
// (REQ-2277, EnvironmentActions).

import { Badge, Group, Table, Text } from "@mantine/core";
import { useTranslation } from "react-i18next";
import { STAGE_COLOR, isBase } from "../../api/environments";
import type { Environment } from "../../api/environments";
import { ListRow } from "../list/ListTable";
import { Tip } from "../Tip";

export interface SyncBadge {
  color: string;
  label: string;
  id: string;
}

export function EnvironmentRow({
  e,
  selected,
  onOpen,
  badge,
}: {
  e: Environment;
  selected: boolean;
  onOpen: () => void;
  /** Where the branch stands against the remote; null while unknown or with no remote. */
  badge: SyncBadge | null;
}) {
  const { t } = useTranslation();
  return (
    <ListRow key={e.name} testId={`env-row-${e.name}`} selected={selected} onClick={onOpen}>
      <Table.Td>
        <Group gap="xs">
          <Text size="sm">{e.name}</Text>
          {e.drifted && (
            <Tip text={t("environmentsTab.driftedHelp")}>
              <Badge color="orange" data-testid={`env-drifted-${e.name}`}>
                {t("environmentsTab.drifted")}
              </Badge>
            </Tip>
          )}
        </Group>
      </Table.Td>
      <Table.Td>
        {isBase(e) ? (
          <Text size="sm" c="dimmed">
            {t("environmentsTab.ownConnections")}
          </Text>
        ) : (
          <Badge variant="light" data-testid={`env-inherits-${e.name}`}>
            {t("environmentsTab.branchOf", { env: e.parent })}
          </Badge>
        )}
      </Table.Td>
      <Table.Td>
        <Badge variant="light" color={STAGE_COLOR[e.stage]} data-testid={`env-stage-${e.name}`}>
          {t(`environmentStage.${e.stage}`)}
        </Badge>
      </Table.Td>
      <Table.Td>
        {/* No row for this branch means the sync call has not answered yet, which is not the
          same as being in sync and is not drawn as if it were. */}
        {badge === null ? (
          <Text size="sm" c="dimmed">
            —
          </Text>
        ) : (
          <Badge
            color={badge.color}
            variant="light"
            data-testid={`env-sync-${e.name}`}
            data-state={badge.id}
          >
            {badge.label}
          </Badge>
        )}
      </Table.Td>
      <Table.Td>
        <Text size="sm">{e.created_by ?? "—"}</Text>
      </Table.Td>
      <Table.Td>
        <Text size="sm">{e.expires_at ?? "—"}</Text>
        {Object.keys(e.expired_kept_by).length > 0 && (
          // REQ-1918: past its expiry and still standing, because something still
          // refers to it. Said on the row so nobody has to read the server's log.
          <Tip
            text={t("environmentsTab.expiredKeptHint", {
              reasons: Object.entries(e.expired_kept_by)
                .map(([kind, count]) => `${t(`dependentsDialog.kind.${kind}`)}: ${count}`)
                .join("; "),
            })}
          >
            <Badge
              size="xs"
              color="yellow"
              variant="light"
              data-testid={`env-expired-kept-${e.name}`}
            >
              {t("environmentsTab.expiredKept")}
            </Badge>
          </Tip>
        )}
      </Table.Td>
      <Table.Td>
        <Text size="sm">{e.protected ? t("detailFields.yes") : t("detailFields.no")}</Text>
      </Table.Td>
    </ListRow>
  );
}
