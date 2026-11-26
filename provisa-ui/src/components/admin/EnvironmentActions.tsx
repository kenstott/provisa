// Copyright (c) 2026 Kenneth Stott
// Canary: 24a24266-a769-47b4-8568-a532897021a9
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// What can be done to one environment, in the head of its panel on Admin › Environments
// (REQ-2277): its stage, whether it is protected, the repair for a drifted projection, merge,
// push, pull and delete. The row only selects; these act on the environment the panel shows.

import { ActionIcon, Button, Group, Menu } from "@mantine/core";
import {
  ArrowDownToLine,
  ArrowUpFromLine,
  ChevronDown,
  GitMerge,
  Lock,
  LockOpen,
  Trash2,
} from "lucide-react";
import { useTranslation } from "react-i18next";
import { ENVIRONMENT_STAGES } from "../../api/environments";
import type { Environment, EnvironmentStage } from "../../api/environments";
import { PanelActions } from "../list/SplitPane";
import { Tip } from "../Tip";

const PROD = "prod";

export function EnvironmentActions({
  e,
  busy,
  manageable,
  canAdminister,
  onRebuild,
  onStage,
  onToggleProtected,
  onMerge,
  onPush,
  onPull,
  onDelete,
}: {
  e: Environment;
  /** A repository action is running against this environment. */
  busy: boolean;
  /** REQ-2261, REQ-2155: the viewer may change this environment's stage. */
  manageable: boolean;
  canAdminister: boolean;
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
    <PanelActions>
      <fieldset className="panel-action-set">
        <Group gap="xs" wrap="wrap">
          {/* REQ-2261: the stage, chosen from a menu of the stages. */}
          <Menu position="bottom-end" withinPortal transitionProps={{ duration: 0 }}>
            <Menu.Target>
              <Button
                disabled={!manageable}
                rightSection={<ChevronDown size={14} aria-hidden />}
                aria-label={`${t("environmentsTab.colStage")}: ${t(`environmentStage.${e.stage}`)}`}
                data-testid={`env-panel-stage-${e.name}`}
              >
                {t(`environmentStage.${e.stage}`)}
              </Button>
            </Menu.Target>
            <Menu.Dropdown>
              {ENVIRONMENT_STAGES.map((stage) => (
                <Menu.Item
                  key={stage}
                  disabled={stage === e.stage}
                  onClick={() => onStage(stage)}
                  data-testid={`env-panel-stage-${e.name}-${stage}`}
                >
                  {t(`environmentStage.${stage}`)}
                </Menu.Item>
              ))}
            </Menu.Dropdown>
          </Menu>
          {/* REQ-1504: a protected environment takes merges and pulls as proposals. Pressed while
            it is protected. */}
          <Button
            disabled={!canAdminister}
            aria-pressed={e.protected}
            leftSection={
              e.protected ? <Lock size={14} aria-hidden /> : <LockOpen size={14} aria-hidden />
            }
            onClick={onToggleProtected}
            data-testid={`env-panel-protected-${e.name}`}
          >
            {t("environmentsTab.colProtected")}
          </Button>
          {/* REQ-1524: the repair for a drifted projection, offered where the drift is shown. */}
          {e.drifted && (
            <Button loading={busy} onClick={onRebuild} data-testid={`env-panel-rebuild-${e.name}`}>
              {t("environmentsTab.rebuild")}
            </Button>
          )}
          <Button
            leftSection={<GitMerge size={14} aria-hidden />}
            onClick={onMerge}
            data-testid={`env-panel-merge-${e.name}`}
          >
            {t("environmentsTab.merge")}
          </Button>
          {/* REQ-1546: the repair for a branch the best-effort mirror could not send, and the way
            to take what the remote holds. Both are asked for here rather than run on a timer. */}
          <Tip text={t("environmentsTab.push")} named>
            <ActionIcon
              loading={busy}
              onClick={onPush}
              aria-label={t("environmentsTab.push")}
              data-testid={`env-panel-push-${e.name}`}
            >
              <ArrowUpFromLine size={14} aria-hidden />
            </ActionIcon>
          </Tip>
          <Tip text={t("environmentsTab.pull")} named>
            <ActionIcon
              loading={busy}
              onClick={onPull}
              aria-label={t("environmentsTab.pull")}
              data-testid={`env-panel-pull-${e.name}`}
            >
              <ArrowDownToLine size={14} aria-hidden />
            </ActionIcon>
          </Tip>
          {/* REQ-1552: stepping this environment's history is offered where the change was made
            -- the environment switcher -- not here. prod exists from the org's creation and is
            refused by the server; delete is withheld rather than left to fail (REQ-1487). Its
            dialog asks what goes with the environment (REQ-1550). */}
          {e.name !== PROD && canAdminister && (
            <Tip text={t("environmentsTab.delete")} named>
              <ActionIcon
                color="red"
                onClick={onDelete}
                aria-label={t("environmentsTab.delete")}
                data-testid={`env-panel-delete-${e.name}`}
              >
                <Trash2 size={14} aria-hidden />
              </ActionIcon>
            </Tip>
          )}
        </Group>
      </fieldset>
    </PanelActions>
  );
}
