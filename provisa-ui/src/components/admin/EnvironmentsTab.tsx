// Copyright (c) 2026 Kenneth Stott
// Canary: 2f60d94b-8ea1-4c37-b05d-63a97e1cf824
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { DestructiveConfirmButton } from "../form/DestructiveConfirmButton";
import { useCallback, useEffect, useRef, useState } from "react";
import {
  Alert,
  Badge,
  Button,
  Checkbox,
  Group,
  List,
  Select,
  Stack,
  Tabs,
  Text,
  TextInput,
  Textarea,
} from "@mantine/core";
import { ModalLayout } from "../modal/ModalLayout";
import { notifications } from "@mantine/notifications";
import { GitBranch, X } from "lucide-react";
import { PageTitle } from "../PageTitle";
import { DetailFields } from "../detail/DetailFields";
import { SplitPane } from "../list/SplitPane";
import { useRememberedDetail } from "../list/useDetailParam";
import { detailListOf, searchNarrowing, shownBy } from "../list/leftOut";
import { useUrlFilter } from "../list/useUrlFilter";
import { FilterInput } from "./FilterInput";
import { useTranslation } from "react-i18next";
import { CollapsibleItem, Collapsibles } from "../collapsible/Collapsible";
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
  rebuildEnvironment,
  requestReview,
} from "../../api/environments";
import { ENVIRONMENT_STAGES, EnvironmentInUse, isBase } from "../../api/environments";
import { DependentsDialog } from "../DependentsDialog";
import type { Dependent } from "../../lib/dependents";
import type {
  BranchSync,
  Conflict,
  CopyReport,
  DataMode,
  Environment,
  EnvironmentStage,
} from "../../api/environments";
import type { NotificationData } from "@mantine/notifications";
import { MergeRequestsPanel } from "./MergeRequestsPanel";
import { RepoBrowser } from "./RepoBrowser";
import { RepoIntegrationPanel } from "./RepoIntegrationPanel";
import { SyntheticDatasetsPanel } from "./SyntheticDatasetsPanel";
import { EnvironmentActions } from "./EnvironmentActions";
import { EnvironmentRow } from "./EnvironmentRow";
import { SyntheticSeedFields, SyntheticSeedTick } from "./SyntheticSeedFields";
import { FormChecks, FormField, FormFooter, FormLayout } from "../form/FormLayout";
import { useRequiredFields } from "../form/useRequiredFields";
import type { SyntheticSeed } from "./syntheticSeed";
import { SortGroupTable } from "../list/SortGroupTable";
import { Tip } from "../Tip";
import { PageActions } from "../PageActions";

const PROD = "prod";

/** REQ-1942: the data modes a new environment can be created with, in the order offered. */
const DATA_MODES: DataMode[] = ["inherit", "unbound", "test_fake", "test_synthetic"];
const DATA_MODE_LABELS: Record<DataMode, string> = {
  inherit: "environmentsTab.dataModeInherit",
  unbound: "environmentsTab.dataModeUnbound",
  test_fake: "environmentsTab.dataModeTestFake",
  test_synthetic: "environmentsTab.dataModeTestSynthetic",
};
const DATA_MODE_HINTS: Record<DataMode, string> = {
  inherit: "environmentsTab.dataModeInheritHint",
  unbound: "environmentsTab.dataModeUnboundHint",
  test_fake: "environmentsTab.dataModeTestFakeHint",
  test_synthetic: "environmentsTab.dataModeTestSyntheticHint",
};

/**
 * The objects a pull collided on, inside the notification that reports it (REQ-1556).
 *
 * A pull has no preview to have named them in, so this is the only place they are said. Named and
 * not offered: the refused pull applied nothing and the fast-forward already applied everything,
 * so there is nothing here to choose.
 */
/**
 * The objects a merge preview offers for selection (REQ-1506): everything it would add, change or
 * remove, plus whatever is already selected -- a scoped preview names only its own objects, and a
 * box that vanished on being ticked could never be unticked.
 */
function offered(preview: CopyReport, selection: string[]): string[] {
  const paths = preview.paths;
  if (!paths) return selection;
  const pulledIn = new Set(preview.pulled_in ?? []);
  const named = [...paths.added, ...paths.changed, ...paths.removed].filter(
    (path) => !pulledIn.has(path),
  );
  return [...new Set([...named, ...selection])].sort();
}

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
/**
 * REQ-2277: the object management anatomy -- the header (title, the environments' filter, New
 * environment), the environments listed beside an environment's panel (?environment=<name>), and
 * the org's repository surfaces as the page's tabs. New environment opens its form in the panel.
 */
export function EnvironmentsTab({ title }: { title: string }) {
  const { t } = useTranslation();
  const { activeOrgId, capabilities, userId } = useAuth();
  // REQ-1573, REQ-1327: what _guard asks of the org's environments.
  const canAdminister = capabilities.includes("environment_management");
  // REQ-2261, REQ-2155: what _guard_owned asks of one environment -- the org-wide right, or the
  // owner's right over the environments it created.
  const canManage = (env: Environment) =>
    canAdminister ||
    (capabilities.includes("environment_owned_management") &&
      userId !== null &&
      env.created_by === userId);

  const [envs, setEnvs] = useState<Environment[]>([]);
  const [envsLoaded, setEnvsLoaded] = useState(false);
  const [search, setSearch] = useUrlFilter();
  // The new-environment form, open in the panel from the header.
  const [adding, setAdding] = useState(false);
  const [name, setName] = useState("");
  const [from, setFrom] = useState<string>(PROD);
  // REQ-1942: Unbound by default -- the whole model and none of the connections. The safe answer
  // is the one you reach by touching nothing.
  const [dataMode, setDataMode] = useState<DataMode>("unbound");
  // REQ-1529: giving an environment its OWN connections is an org_admin's act, so a member who
  // cannot administer the org has exactly one way to create one -- inheriting. The choice is shown
  // to them set to Inherit and disabled rather than hidden, because a hidden one would leave them
  // looking at a Create button that answers 403 with nothing on screen to explain it.
  const mode: DataMode = canAdminister ? dataMode : "inherit";
  const [creating, setCreating] = useState(false);
  const [tab, setTab] = useState<string | null>("environments");
  // REQ-1546: where each branch stands against the remote, computed from refs and never from the
  // network — rendering this page dials nobody's git host.
  const [sync, setSync] = useState<Record<string, BranchSync>>({});
  // REQ-1552: an org with no remote is not "in sync" — nothing it holds is mirrored anywhere, and
  // the page says so once rather than leaving every row looking like a branch nobody has pushed.
  const [remoteConfigured, setRemoteConfigured] = useState<boolean | null>(null);
  // Every text an environment's row (EnvironmentRow) shows.
  const envRowTexts = (e: Environment) => [
    e.name,
    e.drifted ? t("environmentsTab.drifted") : "",
    isBase(e)
      ? t("environmentsTab.ownConnections")
      : t("environmentsTab.branchOf", { env: e.parent }),
    t(`environmentStage.${e.stage}`),
    syncBadge(e.name)?.label,
    e.created_by,
    e.expires_at,
    Object.keys(e.expired_kept_by).length > 0 ? t("environmentsTab.expiredKept") : "",
    e.protected ? t("detailFields.yes") : t("detailFields.no"),
  ];
  // REQ-2277 rule 9: an environment is open only while the list, under the search, shows its row.
  const narrowings = [searchNarrowing<Environment>(t, search, () => setSearch(""), envRowTexts)];
  const detail = useRememberedDetail(
    "environments",
    detailListOf(
      t,
      envsLoaded && tab === "environments" ? envs : null,
      (e) => e.name,
      (e) => e.name,
      narrowings,
    ),
  );
  const newName = useRef<HTMLInputElement>(null);
  const [synthetic, setSynthetic] = useState<SyntheticSeed | null>(null); // REQ-1939

  // REQ-1549: the environment whose Merge button was pressed is the SOURCE of the merge, and the
  // environment it was branched from is where it goes back to. Nobody picks the target for the
  // ordinary case; the picker below exists to send the work somewhere else than home.
  const [mergeSource, setMergeSource] = useState<Environment | null>(null);
  const [mergeInto, setMergeInto] = useState<string | null>(null);
  const [removals, setRemovals] = useState(false);
  // REQ-1506: the objects the merge is scoped to. Empty is the whole model.
  const [selection, setSelection] = useState<string[]>([]);
  const [message, setMessage] = useState("");
  // REQ-1550: what happens to the source once its work has landed. Both are off by default, and
  // the remote one is a modifier on the local one rather than a choice of its own.
  const [retireSource, setRetireSource] = useState(false);
  const [retireRemote, setRetireRemote] = useState(false);
  const [preview, setPreview] = useState<CopyReport | null>(null);
  const [merging, setMerging] = useState(false);
  const [reviewing, setReviewing] = useState(false);
  // REQ-1968: a new environment needs a name; a merge needs a target and a message. Each is checked
  // when its action is pressed.
  const createRequired = useRequiredFields({ name });
  const mergeRequired = useRequiredFields({ into: mergeInto, message });

  // The one environment a repository action is running against, so its row shows the wait rather
  // than the whole table going busy.
  const [busy, setBusy] = useState<string | null>(null);

  // REQ-1550: deleting an environment asks the same two questions a merge that retires its source
  // asks, so it is a dialog rather than a button that acts on the way down.
  const [deleteTarget, setDeleteTarget] = useState<Environment | null>(null);
  const [inUse, setInUse] = useState<{ env: string; dependents: Dependent[] } | null>(null);
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
    fetchEnvironments(activeOrgId)
      .then((found) => {
        setEnvs(found);
        setEnvsLoaded(true);
      })
      .catch(fail);
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
    if (!createRequired.validate()) return;
    setCreating(true);
    try {
      const made = await createEnvironment(orgId, {
        name,
        from_env: from,
        data_mode: mode,
        ...(synthetic ? { synthetic } : {}),
      });
      notifications.show({
        color: "green",
        message: t("environmentsTab.created", {
          env: made.environment.name,
          added: made.copy.added,
        }),
      });
      setName("");
      setSynthetic(null);
      createRequired.reset();
      setAdding(false);
      reload();
      detail.open(made.environment.name);
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
      if (err instanceof EnvironmentInUse) {
        // REQ-1918: refused, nothing removed. What still refers to it is listed in its own
        // dialog, in place of the confirmation the person just answered.
        setDeleteTarget(null);
        setInUse({ env: env.name, dependents: err.dependents });
      } else {
        fail(err as Error);
      }
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

  // REQ-1524: the repair for a drifted projection, offered where the drift is shown.
  const rebuild = (env: Environment) =>
    act(env, async () => {
      const answer = await rebuildEnvironment(orgId, env.name);
      return answer.drifted
        ? { color: "red", message: t("environmentsTab.rebuildFailed", { env: env.name }) }
        : { color: "green", message: t("environmentsTab.rebuilt", { env: env.name }) };
    });

  const pull = (env: Environment) =>
    act(env, async () => {
      const answer = await pullEnvironment(orgId, env.name);
      // REQ-1504: a pull into a protected environment is proposed, and says so.
      if (answer.requires_approval) {
        return { color: "blue", message: t("environmentsTab.deployProposed", { env: env.name }) };
      }
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

  async function changeStage(env: Environment, stage: EnvironmentStage) {
    if (stage === env.stage) return;
    try {
      await patchEnvironment(orgId, env.name, { stage });
      reload();
    } catch (err) {
      fail(err as Error);
    }
  }

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
    // An environment merges back into the one it was created from; prod has none, so it opens with
    // nothing chosen and the merge is refused until somebody names a target — the server refuses
    // it too (REQ-1549).
    setMergeInto(env.parent);
    setRemovals(false);
    setSelection([]);
    setMessage("");
    setRetireSource(false);
    setRetireRemote(false);
    setPreview(null);
    mergeRequired.reset();
  }

  async function runPreview() {
    // A preview needs only the target; the message is the merge's own.
    if (!mergeSource || !mergeRequired.validate(["into"]) || mergeInto === null) return;
    try {
      const answer = await previewMerge(orgId, mergeInto, mergeSource.name, removals, selection);
      setPreview(answer.report);
    } catch (err) {
      fail(err as Error);
    }
  }

  async function runMerge() {
    if (!mergeSource || !mergeRequired.validate() || mergeInto === null) return;
    setMerging(true);
    try {
      const answer = await mergeEnvironment(orgId, mergeInto, {
        from_env: mergeSource.name,
        removals,
        selection: selection.length > 0 ? selection : null,
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
    if (!mergeSource || !mergeRequired.validate() || mergeInto === null) return;
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

  const createForm = (
    <FormLayout data-testid="env-new-form">
      <FormField size="medium">
        <TextInput
          label={t("environmentsTab.newName")}
          {...createRequired.field("name")}
          value={name}
          onChange={(e) => setName(e.currentTarget.value)}
          ref={(el: HTMLInputElement | null) => {
            newName.current = el;
            createRequired.field("name").ref(el);
          }}
          data-testid="env-new-name"
        />
      </FormField>
      <FormField size="medium">
        <Select
          label={t("environmentsTab.branchFrom")}
          data={names}
          value={from}
          onChange={(v) => setFrom(v ?? PROD)}
          data-testid="env-new-from"
        />
      </FormField>
      <FormField
        size="medium"
        help={t("environmentsTab.dataModeHelp")}
        hint={t(DATA_MODE_HINTS[mode])}
      >
        <Select
          label={t("environmentsTab.dataMode")}
          data={DATA_MODES.map((m) => ({ value: m, label: t(DATA_MODE_LABELS[m]) }))}
          value={mode}
          onChange={(v) => v && setDataMode(v as DataMode)}
          allowDeselect={false}
          disabled={!canAdminister}
          data-testid="env-new-data-mode"
        />
      </FormField>
      {/* The tick after the fields; the fields it opens follow. */}
      <FormChecks>
        <SyntheticSeedTick seed={synthetic} setSeed={setSynthetic} />
      </FormChecks>
      <SyntheticSeedFields envs={names} seed={synthetic} setSeed={setSynthetic} />
      {/* Create is not a save: it keeps its word, beside Cancel. */}
      <FormFooter
        panel
        onCancel={() => setAdding(false)}
        action={
          <Button
            leftSection={<GitBranch size={14} aria-hidden />}
            onClick={create}
            loading={creating}
            data-testid="env-create"
          >
            {t("environmentsTab.create")}
          </Button>
        }
      />
    </FormLayout>
  );

  const envView = (e: Environment) => {
    const badge = syncBadge(e.name);
    return (
      <>
        <EnvironmentActions
          e={e}
          busy={busy === e.name}
          manageable={canManage(e)}
          canAdminister={canAdminister}
          onRebuild={() => rebuild(e)}
          onStage={(stage) => changeStage(e, stage)}
          onToggleProtected={() => toggleProtected(e)}
          onMerge={() => openMerge(e)}
          onPush={() => push(e)}
          onPull={() => pull(e)}
          onDelete={() => openDelete(e)}
        />
        <DetailFields
          testId={`env-read-${e.name}`}
          fields={[
            { label: t("environmentsTab.colName"), value: e.name },
            {
              label: t("environmentsTab.colKind"),
              value: isBase(e)
                ? t("environmentsTab.ownConnections")
                : t("environmentsTab.branchOf", { env: e.parent }),
            },
            { label: t("environmentsTab.colStage"), value: t(`environmentStage.${e.stage}`) },
            { label: t("environmentsTab.colRepo"), value: badge === null ? null : badge.label },
            { label: t("environmentsTab.colCreatedBy"), value: e.created_by },
            { label: t("environmentsTab.colExpires"), value: e.expires_at },
            {
              label: t("environmentsTab.colProtected"),
              value: e.protected ? t("detailFields.yes") : t("detailFields.no"),
            },
          ]}
        />
      </>
    );
  };

  const onList = tab === "environments";
  const shownEnvs = shownBy(envs, narrowings);
  const openEnv = envs.find((e) => e.name === detail.id) ?? null;
  const openAdd = () => {
    detail.close();
    setAdding(true);
    // The reader lands in the form's first field.
    requestAnimationFrame(() => newName.current?.focus());
  };

  return (
    <>
      <div className="page-header">
        <PageTitle icon={GitBranch}>{title}</PageTitle>
        {onList && (
          <FilterInput
            value={search}
            onChange={setSearch}
            placeholder={t("environmentsTab.filterPlaceholder")}
            testId="environments-filter"
          />
        )}
        <PageActions>
          {onList && (
            <Button
              variant={adding ? "outline" : "filled"}
              aria-label={adding ? t("environmentsTab.closeForm") : undefined}
              onClick={() => (adding ? setAdding(false) : openAdd())}
              data-testid="env-create-open"
            >
              {adding ? <X size={14} /> : t("environmentsTab.newEnvironment")}
            </Button>
          )}
        </PageActions>
      </div>
      <Stack gap="md" data-testid="environments-tab">
        {/* REQ-1552: the state of the org's mirror is true of every tab here, not of the environment
          list alone, so it sits above them. Collapsed, because the prose behind it is read once --
          what has to stay visible is the single line saying nothing is mirrored anywhere. */}
        <Collapsibles id="admin.environments.about" testId="env-help">
          <CollapsibleItem
            value="about"
            defaultOpen={false}
            title={
              <Group gap="xs" wrap="nowrap" component="span">
                <span>{t("environmentsTab.aboutTitle")}</span>
                {remoteConfigured === false && (
                  <Badge color="orange" variant="light" size="xs" data-testid="env-no-remote">
                    {t("environmentsTab.noRemoteShort")}
                  </Badge>
                )}
              </Group>
            }
          >
            <Stack gap="xs">
              <Text size="sm">{t("environmentsTab.branchIsEnvironment")}</Text>
              {remoteConfigured === false && (
                <Text size="sm" data-testid="env-no-remote-detail">
                  {t("environmentsTab.noRemote")}
                </Text>
              )}
            </Stack>
          </CollapsibleItem>
        </Collapsibles>
        <Tabs value={tab} onChange={setTab}>
          <Tabs.List>
            <Tabs.Tab value="environments">{t("environmentsTab.tabEnvironments")}</Tabs.Tab>
            <Tabs.Tab value="requests">{t("environmentsTab.tabRequests")}</Tabs.Tab>
            <Tabs.Tab value="repository">{t("environmentsTab.tabRepository")}</Tabs.Tab>
            <Tabs.Tab value="integration">{t("environmentsTab.tabIntegration")}</Tabs.Tab>
            <Tabs.Tab value="synthetic" data-tour="synthetic-tab">
              {t("syntheticDatasets.tab")}
            </Tabs.Tab>
          </Tabs.List>

          <Tabs.Panel value="environments" pt="md">
            <Stack gap="md">
              <SplitPane
                outOfList={detail.outOfList}
                testId="environments-split"
                storageKey="provisa.environments.detailWidth"
                title={adding ? t("environmentsTab.newEnvironment") : (openEnv?.name ?? "")}
                onClose={() => {
                  setAdding(false);
                  detail.close();
                }}
                closeOnEscape={!adding}
                labels={{
                  panel: t("environmentsTab.panelLabel"),
                  resize: t("tablesPage.resizeDetail"),
                  expand: t("tablesPage.expandDetail"),
                  restore: t("tablesPage.restoreDetail"),
                  close: t("tablesPage.closeDetail"),
                  backToList: t("tablesPage.backToList"),
                }}
                list={
                  <SortGroupTable
                    testPrefix="environments"
                    testId="environments-list"
                    emptyTestId="environments-empty"
                    // A list a filter emptied says so, never that there are none.
                    empty={
                      envs.length === 0
                        ? t("environmentsTab.noEnvironments")
                        : t("environmentsTab.noMatch")
                    }
                    rows={shownEnvs}
                    columns={[
                      {
                        key: "name",
                        label: t("environmentsTab.colName"),
                        sortValue: (e) => e.name,
                      },
                      {
                        key: "kind",
                        label: t("environmentsTab.colKind"),
                        sortValue: (e) => e.parent ?? "",
                        groupValue: (e) => e.parent ?? "",
                      },
                      {
                        key: "stage",
                        label: t("environmentsTab.colStage"),
                        sortValue: (e) => ENVIRONMENT_STAGES.indexOf(e.stage),
                        groupValue: (e) => t(`environmentStage.${e.stage}`),
                      },
                      {
                        key: "createdBy",
                        label: t("environmentsTab.colCreatedBy"),
                        sortValue: (e) => e.created_by ?? "",
                        groupValue: (e) => e.created_by ?? "",
                      },
                      {
                        key: "expires",
                        label: t("environmentsTab.colExpires"),
                        sortValue: (e) => e.expires_at ?? "",
                      },
                    ]}
                    headers={[
                      { col: "name" },
                      { col: "kind" },
                      <Group gap={4} align="center" wrap="nowrap">
                        {t("environmentsTab.colStage")}
                        <HelpBubble
                          title={t("environmentsTab.stageTitle")}
                          paragraphs={[
                            t("environmentsTab.stageHelp"),
                            t("environmentsTab.stageHelp2"),
                          ]}
                          ariaLabel={t("environmentsTab.stageTitle")}
                          testId="env-stage-help"
                        />
                      </Group>,
                      t("environmentsTab.colRepo"),
                      { col: "createdBy" },
                      { col: "expires" },
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
                      </Group>,
                    ]}
                    colSpan={7}
                    rowKey={(e) => e.name}
                    render={(e) => (
                      <EnvironmentRow
                        key={e.name}
                        e={e}
                        selected={!adding && e.name === detail.id}
                        onOpen={() => {
                          setAdding(false);
                          if (detail.id === e.name) detail.close();
                          else detail.open(e.name);
                        }}
                        badge={syncBadge(e.name)}
                      />
                    )}
                  />
                }
              >
                {adding ? createForm : openEnv !== null ? envView(openEnv) : null}
              </SplitPane>
            </Stack>
          </Tabs.Panel>

          <Tabs.Panel value="synthetic" pt="md">
            <SyntheticDatasetsPanel
              envs={names}
              onCreateEnvironment={() => {
                setTab("environments");
                openAdd();
              }}
            />
          </Tabs.Panel>
          <Tabs.Panel value="requests" pt="md">
            <MergeRequestsPanel orgId={orgId} canDecide={canAdminister} />
          </Tabs.Panel>
          <Tabs.Panel value="repository" pt="md">
            <RepoBrowser orgId={orgId} />
          </Tabs.Panel>
          <Tabs.Panel value="integration" pt="md">
            {/* Storing a remote changes what the rest of this page can say -- the banner, the badge,
              and whether a push has anywhere to go -- so the page rereads rather than holding the
              answer it loaded with. */}
            <RepoIntegrationPanel orgId={orgId} onChanged={reload} />
          </Tabs.Panel>
        </Tabs>

        <ModalLayout
          opened={mergeSource !== null}
          onClose={() => setMergeSource(null)}
          title={t("environmentsTab.mergeTitle", { env: mergeSource?.name })}
        >
          <FormLayout>
            {/* REQ-1549: the target arrives already chosen — this picker is how the work is sent
              somewhere other than where the branch came from. */}
            <FormField size="medium" help={t("environmentsTab.mergeIntoHelp")}>
              <Select
                label={t("environmentsTab.mergeIntoLabel")}
                data={names.filter((n) => n !== mergeSource?.name)}
                {...mergeRequired.field("into")}
                value={mergeInto}
                onChange={setMergeInto}
                data-testid="env-merge-into"
              />
            </FormField>
            {mergeInto === null && (
              <FormField size="full">
                <Alert color="orange" data-testid="env-merge-no-target">
                  {t("environmentsTab.noMergeTarget")}
                </Alert>
              </FormField>
            )}
            {/* REQ-1550: required, and required of every merge rather than only of a proposed one —
              the merge lands as one squashed commit, so this sentence is the whole account of it. */}
            <FormField size="full" help={t("environmentsTab.mergeMessageHelp")}>
              <Textarea
                label={t("environmentsTab.mergeMessage")}
                value={message}
                {...mergeRequired.field("message")}
                onChange={(e) => setMessage(e.currentTarget.value)}
                data-testid="env-merge-message"
              />
            </FormField>
            {/* What each tick does is behind its (?), where a keyboard reaches it. */}
            <FormChecks>
              <FormField size="short" help={t("environmentsTab.removalsHelp")}>
                <Checkbox
                  label={t("environmentsTab.removals")}
                  checked={removals}
                  onChange={(e) => setRemovals(e.currentTarget.checked)}
                  data-testid="env-merge-removals"
                />
              </FormField>
              <FormField size="short" help={t("environmentsTab.retireSourceHelp")}>
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
              </FormField>
              {retireSource && (
                <FormField size="short" help={t("environmentsTab.retireRemoteHelp")}>
                  <Checkbox
                    label={t("environmentsTab.retireRemote")}
                    checked={retireRemote}
                    onChange={(e) => setRetireRemote(e.currentTarget.checked)}
                    data-testid="env-merge-retire-remote"
                  />
                </FormField>
              )}
            </FormChecks>
            <FormField size="full">
              <Stack gap="xs">
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
                {/* REQ-1506: the objects the preview named, each one selectable. Ticking any scopes
              the merge to those; what they require and the target lacks is carried with them, and
              the next preview names it. */}
                {preview?.paths && offered(preview, selection).length > 0 && (
                  <Stack gap={4} data-testid="env-merge-select">
                    <Text size="sm" fw={600}>
                      {t("environmentsTab.selectTitle")}
                    </Text>
                    <Text size="xs" c="dimmed">
                      {t("environmentsTab.selectHelp")}
                    </Text>
                    <FormChecks>
                      {offered(preview, selection).map((path) => (
                        <Checkbox
                          key={path}
                          size="xs"
                          label={path}
                          checked={selection.includes(path)}
                          onChange={(e) =>
                            setSelection((prev) =>
                              e.currentTarget.checked
                                ? [...prev, path]
                                : prev.filter((p) => p !== path),
                            )
                          }
                          data-testid={`env-merge-select-${path}`}
                        />
                      ))}
                    </FormChecks>
                  </Stack>
                )}
                {preview?.pulled_in && preview.pulled_in.length > 0 && (
                  <Alert color="blue" data-testid="env-merge-pulled-in">
                    <Text size="sm">{t("environmentsTab.pulledIn")}</Text>
                    <List size="sm" withPadding>
                      {preview.pulled_in.map((path) => (
                        <List.Item key={path}>{path}</List.Item>
                      ))}
                    </List>
                  </Alert>
                )}
              </Stack>
            </FormField>
            {/* The merge first, at the start; what looks before or asks someone else follows. */}
            <FormFooter
              action={
                <Button onClick={runMerge} loading={merging} data-testid="env-merge-run">
                  {intoEnv?.protected
                    ? t("environmentsTab.propose")
                    : t("environmentsTab.mergeConfirm")}
                </Button>
              }
            >
              <Button variant="default" onClick={runPreview} data-testid="env-merge-preview-run">
                {t("environmentsTab.preview")}
              </Button>
              {/* REQ-1551: for a target branch the git host governs with pull requests, the review
                happens there — this opens it, and the comment above becomes its description. */}
              <Tip text={t("environmentsTab.requestReviewHelp")}>
                <Button
                  variant="subtle"
                  onClick={runReview}
                  loading={reviewing}
                  data-testid="env-merge-review"
                >
                  {t("environmentsTab.requestReview")}
                </Button>
              </Tip>
            </FormFooter>
          </FormLayout>
        </ModalLayout>

        {inUse && (
          <DependentsDialog
            subject={inUse.env}
            dependents={inUse.dependents}
            onClose={() => setInUse(null)}
          />
        )}
        <ModalLayout
          opened={deleteTarget !== null}
          onClose={() => setDeleteTarget(null)}
          title={t("environmentsTab.deleteTitle", { env: deleteTarget?.name })}
        >
          <FormLayout>
            <FormField size="full">
              <Text size="sm">{t("environmentsTab.deleteHelp")}</Text>
            </FormField>
            <FormChecks>
              <FormField size="short" help={t("environmentsTab.deleteBranchHelp")}>
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
              </FormField>
              {deleteBranch && (
                <FormField size="short" help={t("environmentsTab.deleteRemoteBranchHelp")}>
                  <Checkbox
                    label={t("environmentsTab.deleteRemoteBranch")}
                    checked={deleteRemote}
                    onChange={(e) => setDeleteRemote(e.currentTarget.checked)}
                    data-testid="env-delete-remote-branch"
                  />
                </FormField>
              )}
            </FormChecks>
            {/* The action keeps its word, first; Cancel follows. */}
            <FormFooter
              onCancel={() => setDeleteTarget(null)}
              action={
                <DestructiveConfirmButton
                  onClick={runDelete}
                  loading={busy === deleteTarget?.name}
                  testId="env-delete-run"
                >
                  {t("environmentsTab.deleteConfirm")}
                </DestructiveConfirmButton>
              }
            />
          </FormLayout>
        </ModalLayout>
      </Stack>
    </>
  );
}
