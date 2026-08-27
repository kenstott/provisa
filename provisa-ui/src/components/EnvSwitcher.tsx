// Copyright (c) 2026 Kenneth Stott
// Canary: 3b7d1f04-96ac-4e58-8d21-c05f7ab9e236
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useEffect, useState } from "react";
import { Badge, Button, Group, Menu, Text } from "@mantine/core";
import { Check, ChevronDown, GitBranch, Redo2, Undo2 } from "lucide-react";
import { useTranslation } from "react-i18next";
import { useAuth } from "../context/AuthContext";
import { useCapability, useDemonstrated } from "../hooks/useCapability";
import { DemonstratedFeature } from "./DemonstratedFeature";
import { ENV_STORAGE_KEY, selectedEnv } from "../lib/authFetch";
import { modelReplaced } from "../apolloClient";
import { notifications } from "@mantine/notifications";
import {
  ENVIRONMENTS_CHANGED_EVENT,
  fetchBranchSync,
  fetchEnvironments,
  redoEnvironment,
  undoEnvironment,
} from "../api/environments";
import type { BranchSync, Environment } from "../api/environments";

/** The environment every request is served from when none is named (provisa/api/env_routing.py). */
const DEFAULT_ENV = "prod";

/**
 * REQ-1487: which environment of the org's model this browser is reading.
 *
 * REQ-1554: an environment is the control plane's state under that name, and the branch is that
 * state's history — one branch, so choosing the environment is the whole of the choice and there is
 * no ref to name afterwards. The name is written to localStorage, where the fetch interceptor and
 * the Apollo link both read it, and the CACHE IS THEN RESET: it holds the model of the environment
 * just left, and a re-render against it would paint one environment's schema beside another's rows.
 *
 * Resetting rather than reloading the page. The reload was doing this and throwing the application
 * away around it, so a two-click gesture put the loading splash back on screen; what is actually
 * needed is that no view keeps rendering the model that was left, and re-running the active queries
 * against the environment now selected is that. The confirmation is a notification, where the rest
 * of this menu's confirmations already are.
 */
export function EnvSwitcher() {
  const { t } = useTranslation();
  const { activeOrgId, loading } = useAuth();
  // REQ-1573: being served by an environment other than prod is its own right. An analyst holds
  // neither it nor `environment_management`, and the server answers 403 `env.switch_forbidden` to a
  // request naming one — so the menu that names them is not shown to a caller who cannot be served.
  const maySwitch = useCapability("environment_switch");
  // REQ-1602: a role that is shown the right without holding it (the sandbox, REQ-1597) gets the
  // control where a holder's is, inert and badged -- the environments themselves are never fetched,
  // so what it demonstrates is that the product has the switch, not what this org's branches are.
  const demonstrating = useDemonstrated("environment_switch");
  const [envs, setEnvs] = useState<Environment[] | null>(null);
  // REQ-1552: the state of the branch being worked in, WHERE the work happens. The admin page has
  // the same counts, but somebody editing the model is not on the admin page — and a change that
  // has not reached the remote is exactly what they need told without going looking.
  const [sync, setSync] = useState<Record<string, BranchSync>>({});
  const [remoteConfigured, setRemoteConfigured] = useState<boolean | null>(null);
  const [stepping, setStepping] = useState(false);
  // REQ-1553: every model edit moves the environment's cursor server-side (env_repo.write_through),
  // and nothing on the model canvas announces that to this menu — so what the row said at mount is
  // what it would keep saying, and undo stayed grey after a change that made it available. Opening
  // the menu is the moment the answer is wanted, so opening it is what re-reads the rows.
  const [reread, setReread] = useState(0);
  const active = selectedEnv() ?? DEFAULT_ENV;

  useEffect(() => {
    // A bootstrap in flight carries no capabilities yet, which is not a withdrawal of the right
    // (the same reason CapabilityGate waits on `loading`).
    if (loading) return;
    if (!maySwitch) {
      // A selection made while the right was held would otherwise keep riding on every request
      // after it was withdrawn, and the server answers each one 403 (REQ-1573). Dropping the name
      // is the repair for a selection that can no longer be served, the same as the deleted-branch
      // repair below.
      if (selectedEnv() !== null) {
        localStorage.removeItem(ENV_STORAGE_KEY);
        window.location.reload();
      }
      return;
    }
    if (!activeOrgId) return;
    let live = true;
    const load = () =>
      fetchEnvironments(activeOrgId)
        .then((rows) => {
          if (!live) return;
          setEnvs(rows);
          // A stored name absent from the org's list names an environment that has been deleted, and
          // every request carrying it is answered 404 by design (an unknown environment never falls
          // back to prod). Dropping it here is the repair for a selection the server no longer has,
          // not a fallback around a missing value.
          const stored = selectedEnv();
          if (stored !== null && !rows.some((e) => e.name === stored)) {
            localStorage.removeItem(ENV_STORAGE_KEY);
            window.location.reload();
          }
        })
        .catch(() => {
          if (live) setEnvs(null);
        });
    const loadSync = () =>
      fetchBranchSync(activeOrgId)
        .then((answer) => {
          if (!live) return;
          setSync(answer.branches);
          setRemoteConfigured(answer.remote_configured);
        })
        .catch(() => {
          if (live) setRemoteConfigured(null);
        });
    load();
    loadSync();
    // A branch created or deleted on the admin page is a change to the list this menu shows, and
    // that page is a different tree — so the list is re-read on the announcement rather than only
    // at mount, which is what left a just-created branch unselectable until a reload.
    const both = () => {
      load();
      loadSync();
    };
    window.addEventListener(ENVIRONMENTS_CHANGED_EVENT, both);
    return () => {
      live = false;
      window.removeEventListener(ENVIRONMENTS_CHANGED_EVENT, both);
    };
  }, [activeOrgId, loading, maySwitch, reread]);

  if (loading || !activeOrgId) return null;
  if (!maySwitch) {
    if (!demonstrating) return null;
    return (
      <DemonstratedFeature>
        <Button
          variant="default"
          size="compact-sm"
          leftSection={<GitBranch size={14} aria-hidden />}
          rightSection={<ChevronDown size={14} aria-hidden />}
          data-testid="env-switcher-trigger"
          aria-label={t("envSwitcher.label")}
        >
          {t("envSwitcher.env", { env: DEFAULT_ENV })}
        </Button>
      </DemonstratedFeature>
    );
  }
  if (envs === null) return null;
  const orgId = activeOrgId;

  async function select(name: string) {
    if (name === active) return;
    // prod is the environment a request naming none is served, so selecting it is clearing the
    // selection rather than sending its name.
    if (name === DEFAULT_ENV) localStorage.removeItem(ENV_STORAGE_KEY);
    else localStorage.setItem(ENV_STORAGE_KEY, name);
    await modelReplaced();
    notifications.show({ color: "green", message: t("envSwitcher.switched", { env: name }) });
  }

  const label = t("envSwitcher.env", { env: active });
  const here = sync[active];
  // REQ-1553: the environment being read is the one these controls move, so its own row is what
  // says whether either direction is open.
  const standingAt = envs.find((e) => e.name === active) ?? null;

  /**
   * What the branch being worked in owes the remote, in one word (REQ-1552).
   *
   * Null when there is nothing to say — no remote at all, or no answer yet — because a badge that
   * appears whatever the state is says nothing when it matters.
   */
  function standing() {
    // Whether the org mirrors anywhere at all is a property of the org, not of the branch being
    // worked in, so it is said once on the environments page (REQ-1552) and not here.
    if (remoteConfigured === false) return null;
    if (here === undefined) return null;
    if (here.diverged) return { color: "red", label: t("envSwitcher.diverged"), id: "diverged" };
    if (here.remote === null)
      return { color: "orange", label: t("envSwitcher.unpushed"), id: "unpushed" };
    if (here.ahead !== null && here.ahead > 0)
      return { color: "orange", label: t("envSwitcher.ahead", { n: here.ahead }), id: "ahead" };
    if (here.behind !== null && here.behind > 0)
      return { color: "blue", label: t("envSwitcher.behind", { n: here.behind }), id: "behind" };
    return null;
  }

  /**
   * Step the environment being worked in back along its own history, or forward again (REQ-1543).
   *
   * It lives beside the branch name because that is where a person is when they want it: the model
   * they just changed belongs to this environment, and undoing it is a move of this branch.
   */
  async function step(back: boolean) {
    setStepping(true);
    try {
      const answer = back
        ? await undoEnvironment(orgId, active)
        : await redoEnvironment(orgId, active);
      notifications.show({
        color: "green",
        message: t(back ? "envSwitcher.undone" : "envSwitcher.redone", {
          env: active,
          sha: answer.deployed_sha.slice(0, 7),
        }),
      });
      // The model on screen is the one that was just replaced, and Apollo's cache holds it — the
      // same reason choosing an environment resets the store.
      await modelReplaced();
    } catch (err) {
      notifications.show({ color: "red", message: (err as Error).message });
    } finally {
      setStepping(false);
    }
  }

  const state = standing();

  return (
    <Menu
      position="bottom-end"
      withinPortal
      transitionProps={{ duration: 0 }}
      onOpen={() => setReread((n) => n + 1)}
    >
      <Menu.Target>
        <Button
          variant="default"
          size="compact-sm"
          leftSection={<GitBranch size={14} aria-hidden />}
          rightSection={<ChevronDown size={14} aria-hidden />}
          data-testid="env-switcher-trigger"
          aria-label={t("envSwitcher.label")}
        >
          <Group gap="xs" wrap="nowrap">
            {label}
            {state !== null && (
              <Badge
                size="xs"
                variant="light"
                color={state.color}
                data-testid="env-switcher-state"
                data-state={state.id}
              >
                {state.label}
              </Badge>
            )}
          </Group>
        </Button>
      </Menu.Target>
      <Menu.Dropdown>
        {envs.map((e) => {
          const selected = e.name === active;
          return (
            <Menu.Item
              key={e.name}
              role="option"
              aria-selected={selected}
              aria-current={selected ? "true" : undefined}
              leftSection={selected ? <Check size={14} aria-hidden /> : undefined}
              onClick={() => select(e.name)}
            >
              {e.name}
              {e.drifted && (
                <Text span size="xs" c="orange" ml="xs">
                  {t("envSwitcher.drifted")}
                </Text>
              )}
            </Menu.Item>
          );
        })}
        <Menu.Divider />
        {/* REQ-1543/REQ-1552: the change was made here, so the way back is here. REQ-1553: and
            each direction is offered only where there is something that way. */}
        <Menu.Item
          leftSection={<Undo2 size={14} aria-hidden />}
          disabled={stepping || standingAt?.can_undo !== true}
          closeMenuOnClick={false}
          onClick={() => step(true)}
          data-testid="env-switcher-undo"
        >
          {t("envSwitcher.undo", { env: active })}
        </Menu.Item>
        <Menu.Item
          leftSection={<Redo2 size={14} aria-hidden />}
          disabled={stepping || standingAt?.can_redo !== true}
          closeMenuOnClick={false}
          onClick={() => step(false)}
          data-testid="env-switcher-redo"
        >
          {t("envSwitcher.redo", { env: active })}
        </Menu.Item>
      </Menu.Dropdown>
    </Menu>
  );
}
