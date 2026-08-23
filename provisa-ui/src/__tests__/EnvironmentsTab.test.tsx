// Copyright (c) 2026 Kenneth Stott
// Canary: 0e5c3947-b18d-42fa-9a76-2c81f0d64b73
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1487..REQ-1529: the environments page, and the rules it is expected not to let a person walk
// into — prod cannot be deleted, a merge into a protected environment is a proposal rather than an
// application, and a preview is a separate act from the merge it describes.

import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, waitFor, fireEvent } from "../test-utils/render";
import { EnvironmentsTab } from "../components/admin/EnvironmentsTab";
import type { BranchSync, Environment } from "../api/environments";
import { notifications } from "@mantine/notifications";

// The notification host is not mounted in these renders, so what a notification SAYS is read off
// the call rather than out of the DOM (REQ-1556).
vi.mock("@mantine/notifications", () => ({ notifications: { show: vi.fn() } }));

const auth = { activeOrgId: "acme" as string | null, capabilities: ["org_settings"] };
vi.mock("../context/AuthContext", () => ({ useAuth: () => auth }));
vi.mock("../api/environments", () => ({
  // isBase is a pure derivation over a row, not a call — the component reads it the way it reads
  // any other field, so the mock keeps the real one.
  isBase: (e: { branched_from: string | null }) => e.branched_from === null,
  fetchEnvironments: vi.fn(),
  createEnvironment: vi.fn(),
  deleteEnvironment: vi.fn(),
  patchEnvironment: vi.fn(),
  previewMerge: vi.fn(),
  mergeEnvironment: vi.fn(),
  requestReview: vi.fn(),
  fetchBranchSync: vi.fn(),
  pushEnvironment: vi.fn(),
  pullEnvironment: vi.fn(),
  // REQ-1556: the refusal is recognised by TYPE rather than by parsing its message, so the mock
  // has to hand back a real class for the component's instanceof to mean anything.
  DivergedError: class DivergedError extends Error {
    constructor(
      message: string,
      public base: string | null,
      public conflicts: unknown[],
    ) {
      super(message);
      this.name = "DivergedError";
    }
  },
  fetchMergeRequests: vi.fn(),
  decideMergeRequest: vi.fn(),
  fetchRepoIntegration: vi.fn(),
  saveRepoIntegration: vi.fn(),
  fetchRepoBranches: vi.fn(),
  listRemoteBranches: vi.fn(),
  fetchRemoteBranches: vi.fn(),
  fetchRepoHistory: vi.fn(),
  fetchRepoFiles: vi.fn(),
  fetchRepoFile: vi.fn(),
}));

import {
  createEnvironment,
  deleteEnvironment,
  fetchEnvironments,
  fetchMergeRequests,
  fetchRemoteBranches,
  fetchRepoBranches,
  fetchRepoFile,
  fetchRepoFiles,
  fetchRepoHistory,
  listRemoteBranches,
  fetchRepoIntegration,
  mergeEnvironment,
  previewMerge,
  requestReview,
  fetchBranchSync,
  pushEnvironment,
  pullEnvironment,
  DivergedError,
} from "../api/environments";

const mockList = vi.mocked(fetchEnvironments);
const mockCreate = vi.mocked(createEnvironment);
const mockDelete = vi.mocked(deleteEnvironment);
const mockMerge = vi.mocked(mergeEnvironment);
const mockPreview = vi.mocked(previewMerge);
const mockReview = vi.mocked(requestReview);
const mockSync = vi.mocked(fetchBranchSync);
const mockPush = vi.mocked(pushEnvironment);
const mockPull = vi.mocked(pullEnvironment);

/** A branch's standing against the remote, defaulting to the two lines agreeing. */
function syncOf(overrides: Partial<BranchSync> = {}): BranchSync {
  return {
    local: "abc1234",
    remote: "abc1234",
    ahead: 0,
    behind: 0,
    diverged: false,
    unsynced: false,
    ...overrides,
  };
}

function env(name: string, overrides: Partial<Environment> = {}): Environment {
  return {
    name,
    created_at: null,
    created_by: "someone",
    expires_at: null,
    protected: false,
    drifted: false,

    branched_from: name === "prod" ? null : "prod",
    can_undo: true,
    can_redo: false,
    deployed_sha: "aaaaaaa",
    ...overrides,
  };
}

beforeEach(() => {
  vi.clearAllMocks();
  auth.activeOrgId = "acme";
  auth.capabilities = ["org_settings"];
  mockList.mockResolvedValue([env("prod", { protected: true }), env("dev")]);
  // Mantine keeps every tab panel mounted, so the three sibling panels load with this one.
  vi.mocked(fetchMergeRequests).mockResolvedValue([]);
  vi.mocked(fetchRepoIntegration).mockResolvedValue({
    remote: null,
    status_webhook: null,
    configured: false,
  });
  vi.mocked(fetchRepoBranches).mockResolvedValue([]);
  vi.mocked(listRemoteBranches).mockResolvedValue({});
  vi.mocked(fetchRemoteBranches).mockResolvedValue({});
  vi.mocked(fetchRepoHistory).mockResolvedValue([]);
  vi.mocked(fetchRepoFiles).mockResolvedValue([]);
  vi.mocked(fetchRepoFile).mockResolvedValue("");
  mockSync.mockResolvedValue({
    remote_configured: true,
    branches: { prod: syncOf(), dev: syncOf() },
  });
});

describe("EnvironmentsTab", () => {
  it("withholds delete from prod, which the server refuses", async () => {
    render(<EnvironmentsTab />);
    expect(await screen.findByTestId("env-delete-dev")).toBeInTheDocument();
    expect(screen.queryByTestId("env-delete-prod")).toBeNull();
  });

  it("deletes an environment and leaves its branch unless the branch is named too", async () => {
    // REQ-1524/REQ-1550: the ref is the record of what the environment held, so it survives by
    // default — and the remote copy is not offered until the local branch is going with it.
    mockDelete.mockResolvedValue();
    render(<EnvironmentsTab />);
    fireEvent.click(await screen.findByTestId("env-delete-dev"));
    expect(await screen.findByTestId("env-delete-branch")).not.toBeChecked();
    expect(screen.queryByTestId("env-delete-remote-branch")).toBeNull();
    fireEvent.click(screen.getByTestId("env-delete-run"));
    await waitFor(() =>
      expect(mockDelete).toHaveBeenCalledWith("acme", "dev", {
        deleteBranch: false,
        deleteRemoteBranch: false,
      }),
    );
  });

  it("deletes the remote branch only alongside the local one", async () => {
    mockDelete.mockResolvedValue();
    render(<EnvironmentsTab />);
    fireEvent.click(await screen.findByTestId("env-delete-dev"));
    fireEvent.click(await screen.findByTestId("env-delete-branch"));
    fireEvent.click(await screen.findByTestId("env-delete-remote-branch"));
    fireEvent.click(screen.getByTestId("env-delete-run"));
    await waitFor(() =>
      expect(mockDelete).toHaveBeenCalledWith("acme", "dev", {
        deleteBranch: true,
        deleteRemoteBranch: true,
      }),
    );
  });

  it("says which branches hold work the remote does not", async () => {
    // REQ-1546: the counts are what a person acts on, and a branch the remote has never seen is
    // named outright rather than drawn as "0 to push".
    mockSync.mockResolvedValue({
      remote_configured: true,
      branches: {
        prod: syncOf(),
        dev: syncOf({ remote: null, ahead: null, behind: null, unsynced: true }),
      },
    });
    render(<EnvironmentsTab />);
    expect(await screen.findByTestId("env-sync-prod")).toHaveTextContent("In sync");
    expect(screen.getByTestId("env-sync-dev")).toHaveTextContent("Not on the remote");
  });

  it("counts a branch ahead of the remote and one behind it", async () => {
    mockSync.mockResolvedValue({
      remote_configured: true,
      branches: {
        prod: syncOf({ ahead: 0, behind: 3, unsynced: true }),
        dev: syncOf({ ahead: 2, behind: 0, unsynced: true }),
      },
    });
    render(<EnvironmentsTab />);
    expect(await screen.findByTestId("env-sync-dev")).toHaveTextContent("2 to push");
    expect(screen.getByTestId("env-sync-prod")).toHaveTextContent("3 to pull");
  });

  it("says so when the organization has no remote at all", async () => {
    // REQ-1552: "in sync" and "mirrored nowhere" look identical from the counts alone, so the
    // absence of a remote is stated rather than left to be inferred from empty badges.
    mockSync.mockResolvedValue({ remote_configured: false, branches: {} });
    render(<EnvironmentsTab />);
    expect(await screen.findByTestId("env-no-remote")).toHaveTextContent("no git remote");
    expect(screen.queryByTestId("env-sync-dev")).toBeNull();
  });

  it("keeps the explanations folded away until they are asked for", async () => {
    // REQ-1552: the prose is read once and the page is a working surface; what stays visible when
    // it is folded is the one line that is not prose -- that nothing is mirrored anywhere.
    mockSync.mockResolvedValue({ remote_configured: false, branches: {} });
    render(<EnvironmentsTab />);
    expect(await screen.findByTestId("env-no-remote")).toBeInTheDocument();
    expect(screen.getByTestId("env-no-remote-detail")).not.toBeVisible();
    fireEvent.click(screen.getByText("About environments"));
    expect(await screen.findByTestId("env-no-remote-detail")).toBeInTheDocument();
  });

  it("marks a diverged branch rather than counting it", async () => {
    mockSync.mockResolvedValue({
      remote_configured: true,
      branches: {
        prod: syncOf(),
        dev: syncOf({ ahead: null, behind: null, diverged: true, unsynced: true }),
      },
    });
    render(<EnvironmentsTab />);
    expect(await screen.findByTestId("env-sync-dev")).toHaveTextContent("Diverged");
  });

  it("pushes a branch the best-effort mirror could not send", async () => {
    // REQ-1546: the mirror after a commit does not fail the edit it could not send, so this is
    // the repair a person reaches for once the badge says the remote is behind.
    mockPush.mockResolvedValue({ pushed: "abc1234", sync: syncOf() });
    render(<EnvironmentsTab />);
    fireEvent.click(await screen.findByTestId("env-push-dev"));
    await waitFor(() => expect(mockPush).toHaveBeenCalledWith("acme", "dev"));
  });

  it("says a pull found nothing rather than reporting a change", async () => {
    // REQ-1547: a pull that took nothing is not a deploy, and the message does not claim one.
    mockPull.mockResolvedValue({ applied: false, sync: syncOf() });
    render(<EnvironmentsTab />);
    fireEvent.click(await screen.findByTestId("env-pull-dev"));
    await waitFor(() => expect(mockPull).toHaveBeenCalledWith("acme", "dev"));
  });

  it("names the objects a pull carried away from this environment", async () => {
    // REQ-1556: a fast-forward is not refused and looks like any other pull, and it can still
    // overwrite an edit sitting in this schema that no commit holds.
    mockPull.mockResolvedValue({
      applied: true,
      report: {
        added: 0,
        changed: 1,
        removed: 0,
        base: "abc1234",
        compared: true,
        conflicts: [{ path: "sales/domain.yaml", source: "changed", target: "changed" }],
      },
      sync: syncOf(),
    });
    render(<EnvironmentsTab />);
    fireEvent.click(await screen.findByTestId("env-pull-dev"));
    await waitFor(() => expect(notifications.show).toHaveBeenCalled());
    const said = vi.mocked(notifications.show).mock.calls.at(-1)![0];
    render(<>{said!.message}</>);
    expect(screen.getByTestId("env-pull-conflict")).toHaveTextContent("sales/domain.yaml");
    expect(screen.getByTestId("env-pull-conflicts")).toHaveTextContent("overwriting 1 object(s)");
  });

  it("names the objects a refused pull collided on", async () => {
    // REQ-1556: "the two lines diverged" is not a statement about any particular object, and the
    // objects are what whoever has to decide is deciding about.
    mockPull.mockRejectedValue(
      new DivergedError(
        "'dev' and its remote branch both hold commits the other does not.",
        "abc",
        [{ path: "sales/domain.yaml", source: "changed", target: "removed" }],
      ),
    );
    render(<EnvironmentsTab />);
    fireEvent.click(await screen.findByTestId("env-pull-dev"));
    await waitFor(() => expect(notifications.show).toHaveBeenCalled());
    const said = vi.mocked(notifications.show).mock.calls.at(-1)![0];
    render(<>{said!.message}</>);
    expect(screen.getByTestId("env-pull-conflict")).toHaveTextContent(
      "source changed it, target removed it",
    );
  });

  it("creates a branch of the environment that was chosen to branch from", async () => {
    mockCreate.mockResolvedValue({
      environment: env("feature"),
      copy: { added: 3, changed: 0, removed: 0 },
    });
    render(<EnvironmentsTab />);
    fireEvent.change(await screen.findByTestId("env-new-name"), {
      target: { value: "feature" },
    });
    fireEvent.click(screen.getByTestId("env-create"));
    await waitFor(() =>
      expect(mockCreate).toHaveBeenCalledWith("acme", {
        name: "feature",
        from_env: "prod",
        // REQ-1538: nobody touched the box, so the new environment does NOT resolve prod's
        // connections. The safe answer is the default.
        inherit_connections: false,
      }),
    );
  });

  it("badges the environment an inheriting environment took its connections from", async () => {
    // REQ-1538: the fact worth marking is the inheritance, and the badge names WHICH environment
    // supplies the connections — "dev" alone would not say where a query in it actually lands.
    mockList.mockResolvedValue([env("prod"), env("dev", { branched_from: "prod" })]);
    render(<EnvironmentsTab />);
    expect(await screen.findByTestId("env-inherits-dev")).toHaveTextContent("Inherited from prod");
    expect(screen.queryByTestId("env-inherits-prod")).toBeNull();
  });

  it("previews without merging", async () => {
    mockPreview.mockResolvedValue({
      report: { added: 2, changed: 1, removed: 0 },
      applied: false,
      requires_approval: false,
    });
    render(<EnvironmentsTab />);
    // REQ-1549: the button belongs to the environment being merged, and it goes back to the one it
    // was branched from — so dev's merge is previewed against prod with nothing else chosen.
    fireEvent.click(await screen.findByTestId("env-merge-dev"));
    fireEvent.click(await screen.findByTestId("env-merge-preview-run"));
    await waitFor(() => expect(mockPreview).toHaveBeenCalledWith("acme", "prod", "dev", false));
    expect(mockMerge).not.toHaveBeenCalled();
    expect(await screen.findByTestId("env-merge-preview")).toHaveTextContent("+2 / ~1 / -0");
  });

  // REQ-1555: which of the target's own work this merge would carry away. Named, never offered to
  // be resolved — the merge applies the source either way, and the value is the signal.
  it("names the objects both environments changed since they parted", async () => {
    mockPreview.mockResolvedValue({
      report: {
        added: 0,
        changed: 1,
        removed: 0,
        base: "abc1234",
        compared: true,
        conflicts: [{ path: "sales/domain.yaml", source: "changed", target: "changed" }],
      },
      applied: false,
      requires_approval: false,
    });
    render(<EnvironmentsTab />);
    fireEvent.click(await screen.findByTestId("env-merge-dev"));
    fireEvent.click(await screen.findByTestId("env-merge-preview-run"));
    const named = await screen.findByTestId("env-merge-conflicts");
    expect(named).toHaveTextContent("sales/domain.yaml");
    expect(named).toHaveTextContent("source changed it, target changed it");
    // Nothing to choose: reporting a conflict is not a resolution UI.
    expect(screen.queryByTestId("env-merge-conflict-resolve")).toBeNull();
  });

  it("says nothing about conflicts when the two lines collided nowhere", async () => {
    mockPreview.mockResolvedValue({
      report: { added: 2, changed: 0, removed: 0, base: "abc1234", compared: true, conflicts: [] },
      applied: false,
      requires_approval: false,
    });
    render(<EnvironmentsTab />);
    fireEvent.click(await screen.findByTestId("env-merge-dev"));
    fireEvent.click(await screen.findByTestId("env-merge-preview-run"));
    await screen.findByTestId("env-merge-preview");
    expect(screen.queryByTestId("env-merge-conflicts")).toBeNull();
    expect(screen.queryByTestId("env-merge-not-compared")).toBeNull();
  });

  it("distinguishes nothing compared from nothing conflicting", async () => {
    // Two environments each rooted by their own baseline share no ancestor, so the question could
    // not be asked. An empty list there is not a clean merge and must not read as one.
    mockPreview.mockResolvedValue({
      report: { added: 3, changed: 0, removed: 0, base: null, compared: false, conflicts: [] },
      applied: false,
      requires_approval: false,
    });
    render(<EnvironmentsTab />);
    fireEvent.click(await screen.findByTestId("env-merge-dev"));
    fireEvent.click(await screen.findByTestId("env-merge-preview-run"));
    expect(await screen.findByTestId("env-merge-not-compared")).toHaveTextContent(
      "share no common ancestor",
    );
    expect(screen.queryByTestId("env-merge-conflicts")).toBeNull();
  });

  it("proposes rather than merges into a protected environment", async () => {
    // The merge is not refused — it becomes a request somebody else decides, so the button says so
    // before it is pressed (REQ-1504).
    mockMerge.mockResolvedValue({ applied: false, requires_approval: true });
    render(<EnvironmentsTab />);
    fireEvent.click(await screen.findByTestId("env-merge-dev"));
    expect(await screen.findByTestId("env-merge-run")).toHaveTextContent("Propose");
    fireEvent.change(screen.getByTestId("env-merge-message"), {
      target: { value: "adds the tag domain" },
    });
    fireEvent.click(screen.getByTestId("env-merge-run"));
    await waitFor(() =>
      expect(mockMerge).toHaveBeenCalledWith("acme", "prod", {
        from_env: "dev",
        removals: false,
        message: "adds the tag domain",
        retire_source: false,
        retire_remote: false,
      }),
    );
  });

  it("will not merge until the comment is written", async () => {
    // REQ-1550: the merge lands as one squashed commit, so a blank comment would leave the range
    // of work with no account of itself. The server refuses it; the button does not offer it.
    render(<EnvironmentsTab />);
    fireEvent.click(await screen.findByTestId("env-merge-dev"));
    expect(await screen.findByTestId("env-merge-run")).toBeDisabled();
    fireEvent.change(screen.getByTestId("env-merge-message"), { target: { value: "   " } });
    expect(screen.getByTestId("env-merge-run")).toBeDisabled();
    fireEvent.change(screen.getByTestId("env-merge-message"), { target: { value: "ship it" } });
    expect(screen.getByTestId("env-merge-run")).toBeEnabled();
  });

  it("offers deleting the remote branch only underneath deleting the environment", async () => {
    // REQ-1550: the remote copy is what survives a lost volume, so deleting it while the
    // environment stands is never on screen — the option hangs off the one above it.
    mockMerge.mockResolvedValue({ applied: true, requires_approval: false });
    render(<EnvironmentsTab />);
    fireEvent.click(await screen.findByTestId("env-merge-dev"));
    expect(await screen.findByTestId("env-merge-retire")).toBeInTheDocument();
    expect(screen.queryByTestId("env-merge-retire-remote")).toBeNull();
    fireEvent.click(screen.getByTestId("env-merge-retire"));
    fireEvent.click(await screen.findByTestId("env-merge-retire-remote"));
    fireEvent.change(screen.getByTestId("env-merge-message"), { target: { value: "ship it" } });
    fireEvent.click(screen.getByTestId("env-merge-run"));
    await waitFor(() =>
      expect(mockMerge).toHaveBeenCalledWith("acme", "prod", {
        from_env: "dev",
        removals: false,
        message: "ship it",
        retire_source: true,
        retire_remote: true,
      }),
    );
  });

  it("unchecks the remote branch when the environment is left standing", async () => {
    render(<EnvironmentsTab />);
    fireEvent.click(await screen.findByTestId("env-merge-dev"));
    fireEvent.click(await screen.findByTestId("env-merge-retire"));
    fireEvent.click(await screen.findByTestId("env-merge-retire-remote"));
    fireEvent.click(screen.getByTestId("env-merge-retire"));
    fireEvent.click(screen.getByTestId("env-merge-retire"));
    expect(screen.getByTestId("env-merge-retire-remote")).not.toBeChecked();
  });

  it("asks the git host for a review of the same pair the merge would use", async () => {
    // REQ-1551: where the target branch is governed by pull requests, an approval recorded here
    // decides nothing — the review has to happen where the push will be refused.
    mockReview.mockResolvedValue({
      pull_request: { url: "https://github.com/acme/model/pull/4", number: 4, new: true },
      pushed: "abc123",
      sync: {
        local: "abc123",
        remote: "abc123",
        ahead: 0,
        behind: 0,
        diverged: false,
        unsynced: false,
      },
    });
    render(<EnvironmentsTab />);
    fireEvent.click(await screen.findByTestId("env-merge-dev"));
    fireEvent.change(await screen.findByTestId("env-merge-message"), {
      target: { value: "adds the tag domain" },
    });
    fireEvent.click(screen.getByTestId("env-merge-review"));
    await waitFor(() =>
      expect(mockReview).toHaveBeenCalledWith("acme", "dev", {
        message: "adds the tag domain",
        into: "prod",
      }),
    );
    expect(mockMerge).not.toHaveBeenCalled();
  });

  it("has nowhere to send an environment that was not branched from another", async () => {
    // REQ-1549: prod is where branches come from, so nothing is chosen for it and the merge waits
    // on a target rather than defaulting to one.
    render(<EnvironmentsTab />);
    fireEvent.click(await screen.findByTestId("env-merge-prod"));
    expect(await screen.findByTestId("env-merge-no-target")).toBeInTheDocument();
    expect(screen.getByTestId("env-merge-run")).toBeDisabled();
    expect(screen.getByTestId("env-merge-preview-run")).toBeDisabled();
  });

  it("explains the two switches that decide how an environment behaves", async () => {
    // "Inherit connections" and Protected each change what the platform will do later, and neither
    // says so from its label alone (REQ-1504, REQ-1538).
    mockList.mockResolvedValue([env("prod", { protected: true }), env("dev")]);
    render(<EnvironmentsTab />);
    expect(await screen.findByTestId("env-inherit-help")).toBeInTheDocument();
    expect(screen.getByTestId("env-protected-help")).toBeInTheDocument();
  });

  it("does not offer an administrator's controls to a member", async () => {
    auth.capabilities = [];
    render(<EnvironmentsTab />);
    expect(await screen.findByTestId("env-merge-dev")).toBeInTheDocument();
    expect(screen.queryByTestId("env-delete-dev")).toBeNull();
    // REQ-1538/REQ-1529: the box stays visible and is forced on — inheriting is the only kind of
    // environment a non-administering member may create, and hiding it would send a request the
    // server answers 403 with nothing on screen to explain it.
    const inherit = screen.getByTestId("env-new-inherit") as HTMLInputElement;
    expect(inherit.checked).toBe(true);
    expect(inherit.disabled).toBe(true);
  });

  it("sends inherit_connections when a member without org_settings creates one", async () => {
    auth.capabilities = [];
    mockCreate.mockResolvedValue({
      environment: env("feature"),
      copy: { added: 3, changed: 0, removed: 0 },
    });
    render(<EnvironmentsTab />);
    fireEvent.change(await screen.findByTestId("env-new-name"), { target: { value: "feature" } });
    fireEvent.click(screen.getByTestId("env-create"));
    await waitFor(() =>
      expect(mockCreate).toHaveBeenCalledWith("acme", {
        name: "feature",
        from_env: "prod",
        inherit_connections: true,
      }),
    );
  });
});
