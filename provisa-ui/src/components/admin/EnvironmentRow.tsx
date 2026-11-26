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
// branch stands against the remote, who made it, when it expires, whether it is protected, and
// what can be done to it. A click on the row opens the environment's panel (REQ-2277); the
// controls in it act without opening it.

import { ActionIcon, Badge, Button, Group, Select, Switch, Table, Text } from "@mantine/core";
import { ArrowDownToLine, ArrowUpFromLine, GitMerge, Trash2 } from "lucide-react";
import { useTranslation } from "react-i18next";
import { ENVIRONMENT_STAGES, isBase } from "../../api/environments";
import type { Environment, EnvironmentStage } from "../../api/environments";
import { ListRow } from "../list/ListTable";
import { Tip } from "../Tip";

const PROD = "prod";

export interface SyncBadge {
  color: string;
  label: string;
  id: string;
}

export function EnvironmentRow({
  e,
  selected,
  onOpen,
  busy,
  manageable,
  canAdminister,
  badge,
  onRebuild,
  onStage,
  onToggleProtected,
  onMerge,
  onPush,
  onPull,
  onDelete,
}: {
  e: Environment;
  selected: boolean;
  onOpen: () => void;
  /** A repository action is running against this environment. */
  busy: boolean;
  /** REQ-2261, REQ-2155: the viewer may change this environment's stage. */
  manageable: boolean;
  canAdminister: boolean;
  /** Where the branch stands against the remote; null while unknown or with no remote. */
  badge: SyncBadge | null;
  onRebuild: () => void;
  onStage: (stage: EnvironmentStage) => void;
  onToggleProtected: () => void;
  onMerge: () => void;
  onPush: () => void;
  onPull: () => void;
  onDelete: () => void;
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
          {e.drifted && (
            <Button
              size="compact-xs"
              variant="light"
              loading={busy}
              onClick={(ev) => {
                ev.stopPropagation();
                onRebuild();
              }}
              data-testid={`env-rebuild-${e.name}`}
            >
              {t("environmentsTab.rebuild")}
            </Button>
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
      <Table.Td onClick={(ev) => ev.stopPropagation()}>
        <Select
          size="xs"
          w={124}
          allowDeselect={false}
          disabled={!manageable}
          value={e.stage}
          data={ENVIRONMENT_STAGES.map((stage) => ({
            value: stage,
            label: t(`environmentStage.${stage}`),
          }))}
          onChange={(value) => value !== null && onStage(value as EnvironmentStage)}
          aria-label={t("environmentsTab.colStage")}
          data-testid={`env-stage-select-${e.name}`}
        />
      </Table.Td>
      <Table.Td>
        {(() => {
          // No row for this branch means the sync call has not answered yet, which
          // is not the same as being in sync and is not drawn as if it were.
          return badge === null ? (
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
          );
        })()}
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
      <Table.Td onClick={(ev) => ev.stopPropagation()}>
        <Switch
          checked={e.protected}
          disabled={!canAdminister}
          onChange={onToggleProtected}
          data-testid={`env-protected-${e.name}`}
          aria-label={t("environmentsTab.colProtected")}
        />
      </Table.Td>
      <Table.Td onClick={(ev) => ev.stopPropagation()}>
        <Group gap="xs">
          <Button
            size="compact-sm"
            variant="light"
            leftSection={<GitMerge size={14} aria-hidden />}
            onClick={onMerge}
            data-testid={`env-merge-${e.name}`}
          >
            {t("environmentsTab.merge")}
          </Button>
          {/* REQ-1546: the repair for a branch the best-effort mirror could not
          send, and the way to take what the remote holds. Both are asked for
          here rather than run on a timer. */}
          <Tip text={t("environmentsTab.push")} named>
            <ActionIcon
              variant="subtle"
              loading={busy}
              onClick={onPush}
              aria-label={t("environmentsTab.push")}
              data-testid={`env-push-${e.name}`}
            >
              <ArrowUpFromLine size={14} aria-hidden />
            </ActionIcon>
          </Tip>
          <Tip text={t("environmentsTab.pull")} named>
            <ActionIcon
              variant="subtle"
              loading={busy}
              onClick={onPull}
              aria-label={t("environmentsTab.pull")}
              data-testid={`env-pull-${e.name}`}
            >
              <ArrowDownToLine size={14} aria-hidden />
            </ActionIcon>
          </Tip>
          {/* REQ-1552: stepping this environment's history is offered where the
          change was made -- the environment switcher -- not here. */}
          {/* prod exists from the org's creation and is refused by the server; the
          button is withheld rather than left to fail (REQ-1487). */}
          {e.name !== PROD && canAdminister && (
            <Button
              size="compact-sm"
              color="red"
              variant="subtle"
              leftSection={<Trash2 size={14} aria-hidden />}
              onClick={onDelete}
              data-testid={`env-delete-${e.name}`}
            >
              {t("environmentsTab.delete")}
            </Button>
          )}
        </Group>
      </Table.Td>
    </ListRow>
  );
}
