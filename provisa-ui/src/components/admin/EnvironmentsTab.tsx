// Copyright (c) 2026 Kenneth Stott
// Canary: 2f60d94b-8ea1-4c37-b05d-63a97e1cf824
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useCallback, useEffect, useState } from "react";
import {
  ActionIcon,
  Accordion,
  Alert,
  Badge,
  Button,
  Checkbox,
  Group,
  List,
  Modal,
  Select,
  Stack,
  Switch,
  Table,
  Tabs,
  Text,
  TextInput,
  Textarea,
  Title,
  Tooltip,
} from "@mantine/core";
import { notifications } from "@mantine/notifications";
import { ArrowDownToLine, ArrowUpFromLine, GitBranch, GitMerge, Trash2 } from "lucide-react";
import { useTranslation } from "react-i18next";
import { useAuth } from "../../context/AuthContext";
import { HelpBubble } from "../HelpBubble";
import {
  createEnvironment,
  deleteEnvironment,
  fetchBranchSync,
  fetchEnvironments,
  mergeEnvironment,
  patchEnvironment,
  previewMerge,
  pullEnvironment,
  DivergedError,
  pushEnvironment,
  requestReview,
} from "../../api/environments";
import { isBase } from "../../api/environments";
import type { BranchSync, Conflict, CopyReport, Environment } from "../../api/environments";
import type { NotificationData } from "@mantine/notifications";
import { MergeRequestsPanel } from "./MergeRequestsPanel";
import { RepoBrowser } from "./RepoBrowser";
import { RepoIntegrationPanel } from "./RepoIntegrationPanel";

const PROD = "prod";

/**
 * The objects a pull collided on, inside the notification that reports it (REQ-1556).
 *
 * A pull has no preview to have named them in, so this is the only place they are said. Named and
 * not offered: the refused pull applied nothing and the fast-forward already applied everything,
 * so there is nothing here to choose.
 */
function ConflictNotice({ head, conflicts }: { head: string; conflicts: Conflict[] }) {
  const { t } = useTranslation();
  return (
    <div data-testid="env-pull-conflicts">
      <Text size="sm">{head}</Text>
      <List size="sm" withPadding>
        {conflicts.map((c) => (
          <List.Item key={c.path} data-testid="env-pull-conflict">
            {t("environmentsTab.conflictLine", {
              path: c.path,
              source: t(`environmentsTab.conflictSide.${c.source}`),
              target: t(`environmentsTab.conflictSide.${c.target}`),
            })}
          </List.Item>
        ))}
      </List>
    </div>
  );
}

/**
 * REQ-1487..REQ-1529: the org's environments, and everything that is done to one.
 *
 * An environment IS a branch of the org's model — creating one copies the model it is branched
 * from, and merging carries a model back by identity. Nothing here checks out anything: choosing
 * which environment this browser reads is the NavBar's switcher, and this page is about what the
 * org holds rather than about what is being looked at.
 */
export function EnvironmentsTab() {
  const { t } = useTranslation();
  const { activeOrgId, capabilities } = useAuth();
  const canAdminister = capabilities.includes("org_settings");

  const [envs, setEnvs] = useState<Environment[]>([]);
  const [name, setName] = useState("");
  const [from, setFrom] = useState<string>(PROD);
  // REQ-1538: OFF by default. Off means the new environment gets the model and none of the
  // connections; on means it resolves them from `from`, production included. The safe answer is
  // the one you reach by touching nothing.
  const [inheritConnections, setInheritConnections] = useState(false);
  // REQ-1529: giving an environment its OWN connections is an org_admin's act, so a member who
  // cannot administer the org has exactly one way to create one — inheriting. The box is shown to
  // them checked and disabled rather than hidden, because a hidden box would leave them looking at
  // a Create button that answers 403 with nothing on screen to explain it.
  const inherits = inheritConnections || !canAdminister;
  const [creating, setCreating] = useState(false);

  // REQ-1549: the environment whose Merge button was pressed is the SOURCE of the merge, and the
  // environment it was branched from is where it goes back to. Nobody picks the target for the
  // ordinary case; the picker below exists to send the work somewhere else than home.
  const [mergeSource, setMergeSource] = useState<Environment | null>(null);
  const [mergeInto, setMergeInto] = useState<string | null>(null);
  const [removals, setRemovals] = useState(false);
  const [message, setMessage] = useState("");
  // REQ-1550: what happens to the source once its work has landed. Both are off by default, and
  // the remote one is a modifier on the local one rather than a choice of its own.
  const [retireSource, setRetireSource] = useState(false);
  const [retireRemote, setRetireRemote] = useState(false);
  const [preview, setPreview] = useState<CopyReport | null>(null);
  const [merging, setMerging] = useState(false);
  const [reviewing, setReviewing] = useState(false);

  // REQ-1546: where each branch stands against the remote, computed from refs and never from the
  // network — rendering this page dials nobody's git host.
  const [sync, setSync] = useState<Record<string, BranchSync>>({});
  // REQ-1552: an org with no remote is not "in sync" — nothing it holds is mirrored anywhere, and
  // the page says so once rather than leaving every row looking like a branch nobody has pushed.
  const [remoteConfigured, setRemoteConfigured] = useState<boolean | null>(null);
  // The one environment a repository action is running against, so its row shows the wait rather
  // than the whole table going busy.
  const [busy, setBusy] = useState<string | null>(null);

  // REQ-1550: deleting an environment asks the same two questions a merge that retires its source
  // asks, so it is a dialog rather than a button that acts on the way down.
  const [deleteTarget, setDeleteTarget] = useState<Environment | null>(null);
  const [deleteBranch, setDeleteBranch] = useState(false);
  const [deleteRemote, setDeleteRemote] = useState(false);

  /**
   * REQ-1556: a refused pull names the objects both lines moved, and the notification says so
   * rather than only that they diverged -- whoever now decides whose work survives is deciding
   * about particular objects.
   */
  const fail = (err: Error) =>
    notifications.show(
      err instanceof DivergedError
        ? { color: "red", message: <ConflictNotice head={err.message} conflicts={err.conflicts} /> }
        : { color: "red", message: err.message },
    );

  const reload = useCallback(() => {
    if (!activeOrgId) return;
    fetchEnvironments(activeOrgId).then(setEnvs).catch(fail);
    fetchBranchSync(activeOrgId)
      .then((answer) => {
        setSync(answer.branches);
        setRemoteConfigured(answer.remote_configured);
      })
      .catch(fail);
  }, [activeOrgId]);

  useEffect(reload, [reload]);

  if (!activeOrgId) return null;
  const orgId = activeOrgId;

  async function create() {
    setCreating(true);
    try {
      const made = await createEnvironment(orgId, {
        name,
        from_env: from,
        inherit_connections: inherits,
      });
      notifications.show({
        color: "green",
        message: t("environmentsTab.created", {
          env: made.environment.name,
          added: made.copy.added,
        }),
      });
      setName("");
      reload();
    } catch (err) {
      fail(err as Error);
    } finally {
      setCreating(false);
    }
  }

  function openDelete(env: Environment) {
    setDeleteTarget(env);
    setDeleteBranch(false);
    setDeleteRemote(false);
  }

  async function runDelete() {
    if (!deleteTarget) return;
    const env = deleteTarget;
    setBusy(env.name);
    try {
      await deleteEnvironment(orgId, env.name, {
        deleteBranch,
        deleteRemoteBranch: deleteRemote,
      });
      // Unless the branch was named too, the ref survives: deleting an environment drops its
      // schema and its row, and the ref is what keeps the state it held loadable (REQ-1524).
      notifications.show({
        color: "gray",
        message: deleteBranch
          ? t("environmentsTab.deletedWithBranch", { env: env.name })
          : t("environmentsTab.deleted", { env: env.name }),
      });
      setDeleteTarget(null);
      reload();
    } catch (err) {
      fail(err as Error);
    } finally {
      setBusy(null);
    }
  }

  /**
   * Run one repository action against one environment and say what it did (REQ-1546, REQ-1543).
   *
   * Both of these — push and pull — answer with the new state rather than with an
   * acknowledgement, so the table is reloaded from the server afterwards instead of being patched
   * from what the call returned.
   */
  async function act(env: Environment, run: () => Promise<NotificationData>) {
    setBusy(env.name);
    try {
      notifications.show(await run());
      reload();
    } catch (err) {
      fail(err as Error);
    } finally {
      setBusy(null);
    }
  }

  const push = (env: Environment) =>
    act(env, async () => {
      const answer = await pushEnvironment(orgId, env.name);
      return {
        color: "green",
        message: t("environmentsTab.pushed", { env: env.name, sha: answer.pushed?.slice(0, 7) }),
      };
    });

  const pull = (env: Environment) =>
    act(env, async () => {
      const answer = await pullEnvironment(orgId, env.name);
      // A pull that found nothing to take is reported as such rather than as a change (REQ-1547).
      if (!answer.applied) {
        return { color: "green", message: t("environmentsTab.pullUpToDate", { env: env.name }) };
      }
      // REQ-1556: a fast-forward is not refused and nothing about it looks dangerous, yet it can
      // still carry away an edit sitting in this environment that no commit holds. Named here
      // because there is no preview of a pull for it to have been named in.
      const carried = answer.report?.conflicts ?? [];
      if (carried.length === 0) {
        return { color: "green", message: t("environmentsTab.pulled", { env: env.name }) };
      }
      return {
        color: "yellow",
        message: (
          <ConflictNotice
            head={t("environmentsTab.pulledOverwriting", {
              env: env.name,
              count: carried.length,
            })}
            conflicts={carried}
          />
        ),
      };
    });

  async function toggleProtected(env: Environment) {
    try {
      await patchEnvironment(orgId, env.name, { protected: !env.protected });
      reload();
    } catch (err) {
      fail(err as Error);
    }
  }

  function openMerge(env: Environment) {
    setMergeSource(env);
    // A base environment has no `branched_from`, so it opens with nothing chosen and the merge is
    // refused until somebody names a target — the server refuses it too (REQ-1549).
    setMergeInto(env.branched_from);
    setRemovals(false);
    setMessage("");
    setRetireSource(false);
    setRetireRemote(false);
    setPreview(null);
  }

  async function runPreview() {
    if (!mergeSource || mergeInto === null) return;
    try {
      const answer = await previewMerge(orgId, mergeInto, mergeSource.name, removals);
      setPreview(answer.report);
    } catch (err) {
      fail(err as Error);
    }
  }

  async function runMerge() {
    if (!mergeSource || mergeInto === null) return;
    setMerging(true);
    try {
      const answer = await mergeEnvironment(orgId, mergeInto, {
        from_env: mergeSource.name,
        removals,
        message,
        retire_source: retireSource,
        retire_remote: retireRemote,
      });
      // A protected target is not refused — it is proposed to, and the answer carries the request
      // somebody else will decide (REQ-1504).
      // REQ-1555: a merge that carried somebody else's work away says so at the moment it applied,
      // not only in the preview somebody may not have run.
      const carried = answer.report?.conflicts?.length ?? 0;
      notifications.show({
        color: answer.applied ? (carried > 0 ? "yellow" : "green") : "blue",
        message: answer.applied
          ? carried > 0
            ? t("environmentsTab.mergedOverwriting", { env: mergeInto, count: carried })
            : t("environmentsTab.merged", { env: mergeInto })
          : t("environmentsTab.mergeProposed", { env: mergeInto }),
      });
      setMergeSource(null);
      reload();
    } catch (err) {
      fail(err as Error);
    } finally {
      setMerging(false);
    }
  }

  /**
   * Ask for the merge to be reviewed on the org's git host (REQ-1551).
   *
   * Where the target branch is governed by pull requests, an approval recorded in Provisa decides
   * nothing — the host refuses the push a local merge produces. The server pushes this branch and
   * opens the request; asking twice returns the request that is already open rather than a second.
   */
  async function runReview() {
    if (!mergeSource || mergeInto === null) return;
    setReviewing(true);
    try {
      const answer = await requestReview(orgId, mergeSource.name, {
        message,
        into: mergeInto,
      });
      notifications.show({
        color: "blue",
        message: t("environmentsTab.reviewOpened", { url: answer.pull_request.url }),
      });
      setMergeSource(null);
      reload();
    } catch (err) {
      fail(err as Error);
    } finally {
      setReviewing(false);
    }
  }

  const names = envs.map((e) => e.name);

  /**
   * What one branch's standing against the remote says, in one badge (REQ-1546).
   *
   * Counts are null when one side has no branch at all — unknown rather than zero — so the two
   * one-sided cases are named outright instead of being rendered as "0 to push".
   */
  function syncBadge(name: string) {
    // With no remote there is nothing to be ahead of or behind, so the counts are not drawn at all
    // — the banner above the table says the one thing there is to say (REQ-1552).
    if (remoteConfigured === false) return null;
    const state = sync[name];
    if (state === undefined) return null;
    if (state.diverged)
      return { color: "red", label: t("environmentsTab.syncDiverged"), id: "diverged" };
    if (state.local === null)
      return { color: "blue", label: t("environmentsTab.syncRemoteOnly"), id: "remote-only" };
    if (state.remote === null)
      return { color: "orange", label: t("environmentsTab.syncNotOnRemote"), id: "unpushed" };
    if (state.ahead !== null && state.ahead > 0)
      return {
        color: "orange",
        label: t("environmentsTab.syncAhead", { n: state.ahead }),
        id: "ahead",
      };
    if (state.behind !== null && state.behind > 0)
      return {
        color: "blue",
        label: t("environmentsTab.syncBehind", { n: state.behind }),
        id: "behind",
      };
    return { color: "green", label: t("environmentsTab.syncInSync"), id: "in-sync" };
  }
  // Whether the target is protected decides whether the button proposes or merges, so the row is
  // looked up rather than carried: the modal holds a NAME, and the row for it is here (REQ-1504).
  const intoEnv = envs.find((e) => e.name === mergeInto) ?? null;

  return (
    <Stack gap="md" data-testid="environments-tab">
      {/* REQ-1552: the state of the org's mirror is true of every tab here, not of the environment
          list alone, so it sits above them. Collapsed, because the prose behind it is read once --
          what has to stay visible is the single line saying nothing is mirrored anywhere. */}
      <Accordion variant="contained" chevronPosition="left" data-testid="env-help">
        <Accordion.Item value="about">
          <Accordion.Control>
            <Group gap="xs" wrap="nowrap">
              <Text size="sm">{t("environmentsTab.aboutTitle")}</Text>
              {remoteConfigured === false && (
                <Badge color="orange" variant="light" size="xs" data-testid="env-no-remote">
                  {t("environmentsTab.noRemoteShort")}
                </Badge>
              )}
            </Group>
          </Accordion.Control>
          <Accordion.Panel>
            <Stack gap="xs">
              <Text size="sm">{t("environmentsTab.branchIsEnvironment")}</Text>
              {remoteConfigured === false && (
                <Text size="sm" data-testid="env-no-remote-detail">
                  {t("environmentsTab.noRemote")}
                </Text>
              )}
            </Stack>
          </Accordion.Panel>
        </Accordion.Item>
      </Accordion>
      <Tabs defaultValue="environments">
        <Tabs.List>
          <Tabs.Tab value="environments">{t("environmentsTab.tabEnvironments")}</Tabs.Tab>
          <Tabs.Tab value="requests">{t("environmentsTab.tabRequests")}</Tabs.Tab>
          <Tabs.Tab value="repository">{t("environmentsTab.tabRepository")}</Tabs.Tab>
          <Tabs.Tab value="integration">{t("environmentsTab.tabIntegration")}</Tabs.Tab>
        </Tabs.List>

        <Tabs.Panel value="environments" pt="md">
          <Stack gap="md">
            <Title order={4}>{t("environmentsTab.title")}</Title>
            <Group align="end">
              <TextInput
                label={t("environmentsTab.newName")}
                value={name}
                onChange={(e) => setName(e.currentTarget.value)}
                data-testid="env-new-name"
              />
              <Select
                label={t("environmentsTab.branchFrom")}
                data={names}
                value={from}
                onChange={(v) => setFrom(v ?? PROD)}
                data-testid="env-new-from"
              />
              <Group gap={4} align="center">
                <Checkbox
                  label={t("environmentsTab.inheritConnections")}
                  checked={inherits}
                  disabled={!canAdminister}
                  onChange={(e) => setInheritConnections(e.currentTarget.checked)}
                  data-testid="env-new-inherit"
                />
                <HelpBubble
                  title={t("environmentsTab.inheritTitle")}
                  paragraphs={[t("environmentsTab.inheritHelp"), t("environmentsTab.inheritHelp2")]}
                  ariaLabel={t("environmentsTab.inheritTitle")}
                  testId="env-inherit-help"
                />
              </Group>
              <Button
                leftSection={<GitBranch size={14} aria-hidden />}
                onClick={create}
                loading={creating}
                disabled={name.trim() === ""}
                data-testid="env-create"
              >
                {t("environmentsTab.create")}
              </Button>
            </Group>

            <Table striped highlightOnHover>
              <Table.Thead>
                <Table.Tr>
                  <Table.Th>{t("environmentsTab.colName")}</Table.Th>
                  <Table.Th>{t("environmentsTab.colKind")}</Table.Th>
                  <Table.Th>{t("environmentsTab.colRepo")}</Table.Th>
                  <Table.Th>{t("environmentsTab.colCreatedBy")}</Table.Th>
                  <Table.Th>{t("environmentsTab.colExpires")}</Table.Th>
                  <Table.Th>
                    <Group gap={4} align="center" wrap="nowrap">
                      {t("environmentsTab.colProtected")}
                      <HelpBubble
                        title={t("environmentsTab.protectedTitle")}
                        paragraphs={[
                          t("environmentsTab.protectedHelp"),
                          t("environmentsTab.protectedHelp2"),
                        ]}
                        ariaLabel={t("environmentsTab.protectedTitle")}
                        testId="env-protected-help"
                      />
                    </Group>
                  </Table.Th>
                  <Table.Th>{t("environmentsTab.colActions")}</Table.Th>
                </Table.Tr>
              </Table.Thead>
              <Table.Tbody>
                {envs.map((e) => (
                  <Table.Tr key={e.name} data-testid={`env-row-${e.name}`}>
                    <Table.Td>
                      <Group gap="xs">
                        <Text size="sm">{e.name}</Text>
                        {e.drifted && (
                          <Tooltip label={t("environmentsTab.driftedHelp")}>
                            <Badge color="orange" data-testid={`env-drifted-${e.name}`}>
                              {t("environmentsTab.drifted")}
                            </Badge>
                          </Tooltip>
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
                          {t("environmentsTab.branchOf", { env: e.branched_from })}
                        </Badge>
                      )}
                    </Table.Td>
                    <Table.Td>
                      {(() => {
                        const badge = syncBadge(e.name);
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
                    </Table.Td>
                    <Table.Td>
                      <Switch
                        checked={e.protected}
                        disabled={!canAdminister}
                        onChange={() => toggleProtected(e)}
                        data-testid={`env-protected-${e.name}`}
                        aria-label={t("environmentsTab.colProtected")}
                      />
                    </Table.Td>
                    <Table.Td>
                      <Group gap="xs">
                        <Button
                          size="compact-sm"
                          variant="light"
                          leftSection={<GitMerge size={14} aria-hidden />}
                          onClick={() => openMerge(e)}
                          data-testid={`env-merge-${e.name}`}
                        >
                          {t("environmentsTab.merge")}
                        </Button>
                        {/* REQ-1546: the repair for a branch the best-effort mirror could not
                            send, and the way to take what the remote holds. Both are asked for
                            here rather than run on a timer. */}
                        <Tooltip label={t("environmentsTab.push")}>
                          <ActionIcon
                            variant="subtle"
                            loading={busy === e.name}
                            onClick={() => push(e)}
                            aria-label={t("environmentsTab.push")}
                            data-testid={`env-push-${e.name}`}
                          >
                            <ArrowUpFromLine size={14} aria-hidden />
                          </ActionIcon>
                        </Tooltip>
                        <Tooltip label={t("environmentsTab.pull")}>
                          <ActionIcon
                            variant="subtle"
                            loading={busy === e.name}
                            onClick={() => pull(e)}
                            aria-label={t("environmentsTab.pull")}
                            data-testid={`env-pull-${e.name}`}
                          >
                            <ArrowDownToLine size={14} aria-hidden />
                          </ActionIcon>
                        </Tooltip>
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
                            onClick={() => openDelete(e)}
                            data-testid={`env-delete-${e.name}`}
                          >
                            {t("environmentsTab.delete")}
                          </Button>
                        )}
                      </Group>
                    </Table.Td>
                  </Table.Tr>
                ))}
              </Table.Tbody>
            </Table>
          </Stack>
        </Tabs.Panel>

        <Tabs.Panel value="requests" pt="md">
          <MergeRequestsPanel orgId={orgId} canDecide={canAdminister} />
        </Tabs.Panel>
        <Tabs.Panel value="repository" pt="md">
          <RepoBrowser orgId={orgId} />
        </Tabs.Panel>
        <Tabs.Panel value="integration" pt="md">
          <RepoIntegrationPanel orgId={orgId} />
        </Tabs.Panel>
      </Tabs>

      <Modal
        opened={mergeSource !== null}
        onClose={() => setMergeSource(null)}
        title={t("environmentsTab.mergeTitle", { env: mergeSource?.name })}
      >
        <Stack gap="sm">
          {/* REQ-1549: the target arrives already chosen — this picker is how the work is sent
              somewhere other than where the branch came from. */}
          <Select
            label={t("environmentsTab.mergeIntoLabel")}
            description={t("environmentsTab.mergeIntoHelp")}
            data={names.filter((n) => n !== mergeSource?.name)}
            value={mergeInto}
            onChange={setMergeInto}
            data-testid="env-merge-into"
          />
          {mergeInto === null && (
            <Alert color="orange" data-testid="env-merge-no-target">
              {t("environmentsTab.noMergeTarget")}
            </Alert>
          )}
          {/* REQ-1550: required, and required of every merge rather than only of a proposed one —
              the merge lands as one squashed commit, so this sentence is the whole account of it. */}
          <Textarea
            label={t("environmentsTab.mergeMessage")}
            description={t("environmentsTab.mergeMessageHelp")}
            value={message}
            onChange={(e) => setMessage(e.currentTarget.value)}
            required
            data-testid="env-merge-message"
          />
          <Tooltip label={t("environmentsTab.removalsHelp")} multiline w={320}>
            <Checkbox
              label={t("environmentsTab.removals")}
              checked={removals}
              onChange={(e) => setRemovals(e.currentTarget.checked)}
              data-testid="env-merge-removals"
            />
          </Tooltip>
          <Tooltip label={t("environmentsTab.retireSourceHelp")} multiline w={320}>
            <Checkbox
              label={t("environmentsTab.retireSource", { env: mergeSource?.name })}
              checked={retireSource}
              onChange={(e) => {
                setRetireSource(e.currentTarget.checked);
                // The remote option cannot outlive the box it hangs off: leaving the environment
                // standing while deleting its only off-volume copy is never offered (REQ-1550).
                if (!e.currentTarget.checked) setRetireRemote(false);
              }}
              data-testid="env-merge-retire"
            />
          </Tooltip>
          {retireSource && (
            <Tooltip label={t("environmentsTab.retireRemoteHelp")} multiline w={320}>
              <Checkbox
                ml="lg"
                label={t("environmentsTab.retireRemote")}
                checked={retireRemote}
                onChange={(e) => setRetireRemote(e.currentTarget.checked)}
                data-testid="env-merge-retire-remote"
              />
            </Tooltip>
          )}
          {preview && (
            <Alert color="gray" data-testid="env-merge-preview">
              {t("environmentsTab.reportCounts", {
                added: preview.added,
                changed: preview.changed,
                removed: preview.removed,
              })}
            </Alert>
          )}
          {/* REQ-1555: the objects this merge would carry away from whoever changed them in the
              target. Named, not offered: there is nothing to choose here, because the merge applies
              the source either way — what was missing was the sentence saying whose work went. */}
          {preview && preview.compared === false && (
            <Alert color="gray" data-testid="env-merge-not-compared">
              {t("environmentsTab.conflictsNotCompared")}
            </Alert>
          )}
          {preview?.conflicts && preview.conflicts.length > 0 && (
            <Alert color="yellow" data-testid="env-merge-conflicts">
              <Text size="sm" fw={600}>
                {t("environmentsTab.conflictsTitle", { count: preview.conflicts.length })}
              </Text>
              <List size="sm" withPadding>
                {preview.conflicts.map((c) => (
                  <List.Item key={c.path} data-testid="env-merge-conflict">
                    {t("environmentsTab.conflictLine", {
                      path: c.path,
                      source: t(`environmentsTab.conflictSide.${c.source}`),
                      target: t(`environmentsTab.conflictSide.${c.target}`),
                    })}
                  </List.Item>
                ))}
              </List>
            </Alert>
          )}
          <Group>
            <Button
              variant="default"
              onClick={runPreview}
              disabled={mergeInto === null}
              data-testid="env-merge-preview-run"
            >
              {t("environmentsTab.preview")}
            </Button>
            {/* REQ-1551: for a target branch the git host governs with pull requests, the review
                happens there — this opens it, and the comment above becomes its description. */}
            <Tooltip label={t("environmentsTab.requestReviewHelp")} multiline w={320}>
              <Button
                variant="subtle"
                onClick={runReview}
                loading={reviewing}
                disabled={mergeInto === null || message.trim() === ""}
                data-testid="env-merge-review"
              >
                {t("environmentsTab.requestReview")}
              </Button>
            </Tooltip>
            <Button
              onClick={runMerge}
              loading={merging}
              disabled={mergeInto === null || message.trim() === ""}
              data-testid="env-merge-run"
            >
              {intoEnv?.protected
                ? t("environmentsTab.propose")
                : t("environmentsTab.mergeConfirm")}
            </Button>
          </Group>
        </Stack>
      </Modal>

      <Modal
        opened={deleteTarget !== null}
        onClose={() => setDeleteTarget(null)}
        title={t("environmentsTab.deleteTitle", { env: deleteTarget?.name })}
      >
        <Stack gap="sm">
          <Text size="sm">{t("environmentsTab.deleteHelp")}</Text>
          <Tooltip label={t("environmentsTab.deleteBranchHelp")} multiline w={320}>
            <Checkbox
              label={t("environmentsTab.deleteBranch")}
              checked={deleteBranch}
              onChange={(e) => {
                setDeleteBranch(e.currentTarget.checked);
                // Same rule the merge dialog follows: the remote copy is what survives a lost
                // volume, so it is never deleted while the branch it mirrors stays (REQ-1550).
                if (!e.currentTarget.checked) setDeleteRemote(false);
              }}
              data-testid="env-delete-branch"
            />
          </Tooltip>
          {deleteBranch && (
            <Tooltip label={t("environmentsTab.deleteRemoteBranchHelp")} multiline w={320}>
              <Checkbox
                ml="lg"
                label={t("environmentsTab.deleteRemoteBranch")}
                checked={deleteRemote}
                onChange={(e) => setDeleteRemote(e.currentTarget.checked)}
                data-testid="env-delete-remote-branch"
              />
            </Tooltip>
          )}
          <Group>
            <Button variant="default" onClick={() => setDeleteTarget(null)}>
              {t("environmentsTab.cancel")}
            </Button>
            <Button
              color="red"
              onClick={runDelete}
              loading={busy === deleteTarget?.name}
              data-testid="env-delete-run"
            >
              {t("environmentsTab.deleteConfirm")}
            </Button>
          </Group>
        </Stack>
      </Modal>
    </Stack>
  );
}
