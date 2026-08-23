// Copyright (c) 2026 Kenneth Stott
// Canary: 8f52b6ad-3c19-4e7a-b0d5-91c47e2a63be
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1487..REQ-1529: the environments an org holds, the merges between them, the approvals that
// hold a merge, and the repository the model is projected into. One module because they are one
// router — every path here hangs off /admin/orgs/{org}/environments.

import { serverMessage, requestFailed } from "../i18n/serverMessage";

const API_BASE = import.meta.env.VITE_API_BASE || "";

function base(orgId: string): string {
  return `${API_BASE}/admin/orgs/${encodeURIComponent(orgId)}/environments`;
}

async function ok<T>(res: Response, op: string): Promise<T> {
  if (!res.ok) {
    const data = await res.json().catch(() => ({ detail: res.statusText }));
    throw new Error(serverMessage(data, requestFailed(op, res.status)));
  }
  return res.json() as Promise<T>;
}

function json(method: string, body: unknown): RequestInit {
  return { method, headers: { "Content-Type": "application/json" }, body: JSON.stringify(body) };
}

export interface Environment {
  name: string;
  created_at: string | null;
  created_by: string | null;
  expires_at: string | null;
  protected: boolean;
  drifted: boolean;
  // REQ-1529/REQ-1538: an environment with no `branched_from` carries its OWN connections;
  // anything else resolves them from the environment it was created from. The registry stores that
  // one column and nothing else -- owning your connections IS holding no branched_from, so
  // `isBase` derives it rather than reading a field the server has never sent.
  branched_from: string | null;
  // REQ-1553: which way the history is open from where this environment stands. The cursor lives
  // in the control plane and the line lives in git, so neither end is derivable in the browser --
  // the server answers both with the row rather than leaving a button to be refused after it is
  // pressed.
  can_undo: boolean;
  can_redo: boolean;
  // REQ-1548: the commit this environment's model was ingested from. One control plane, one sha --
  // this is the version the switcher shows as the one being read.
  deployed_sha: string | null;
}

/** REQ-1529: an environment bound with its own source credentials, which is what members branch. */
export function isBase(env: Environment): boolean {
  return env.branched_from === null;
}

/**
 * One object both lines edited since the commit they last shared, and what each of them did to it
 * (REQ-1555). REPORTED, NEVER RESOLVED: there is no per-object choosing here and no three-way
 * editor — a merge into a target is the source winning, and this is the sentence naming whose work
 * that carried away.
 */
export interface Conflict {
  path: string;
  source: "added" | "changed" | "removed";
  target: "added" | "changed" | "removed";
}

export interface CopyReport {
  added: number;
  changed: number;
  removed: number;
  // REQ-1555: `base` is the commit both lines last held, and `compared` is false when they share no
  // ancestor at all. An empty `conflicts` under `compared: false` means NOTHING WAS COMPARED rather
  // than nothing collided, so the two are never rendered the same way.
  base?: string | null;
  compared?: boolean;
  conflicts?: Conflict[];
  [key: string]: unknown;
}

export interface MergeRequest {
  id: number;
  // REQ-1496: a request names EITHER a source environment or a source ref, never both -- a merge
  // comes from a sibling environment, a load from a commit. `source_sha` is what actually applies;
  // `source_ref` is kept beside it because a branch name is what a person recognises.
  source_env: string | null;
  source_ref: string | null;
  source_sha: string | null;
  // REQ-1539: whether the load applies the creation-only classes. Part of what is being approved.
  seed: boolean;
  target_env: string;
  state: string;
  requested_by: string | null;
  requested_at: string | null;
  decided_by: string | null;
  decided_at: string | null;
  decision_note: string | null;
  applied_at: string | null;
  report: CopyReport;
  message: string;
}

export interface RepoIntegration {
  remote: string | null;
  status_webhook: string | null;
  configured: boolean;
}

export interface RepoCommit {
  sha: string;
  author: string;
  message: string;
  committed_at: number;
}

/**
 * The list of environments changed on the server (REQ-1487).
 *
 * The switcher in the navbar and the admin page are separate trees that both read this list, so
 * creating a branch on one leaves the other showing the list it read when it mounted — a branch
 * that exists and cannot be selected. The mutations announce here rather than each page threading
 * a callback to a component it does not own.
 */
export const ENVIRONMENTS_CHANGED_EVENT = "provisa:environments-changed";

function announceEnvironmentsChanged(): void {
  window.dispatchEvent(new Event(ENVIRONMENTS_CHANGED_EVENT));
}

export async function fetchEnvironments(orgId: string): Promise<Environment[]> {
  const res = await fetch(base(orgId));
  const body = await ok<{ environments: Environment[] }>(res, "fetchEnvironments");
  return body.environments;
}

/**
 * Create an environment (REQ-1488, REQ-1528).
 *
 * `from_env` is the environment whose model the new one starts from, copied whole.
 * `inherit_connections` says the new environment resolves that one's connection coordinates —
 * host, port, database, username — by reference instead of carrying its own (REQ-1538). It
 * defaults to false on the server and is left out here unless the caller asks for it: a dev
 * environment made from prod should get prod's model and none of prod's databases.
 */
export async function createEnvironment(
  orgId: string,
  body: {
    name: string;
    from_env: string;
    inherit_connections?: boolean;
    expires_at?: string | null;
  },
): Promise<{ environment: Environment; copy: CopyReport }> {
  const res = await fetch(base(orgId), json("POST", body));
  const created = await ok<{ environment: Environment; copy: CopyReport }>(
    res,
    "createEnvironment",
  );
  announceEnvironmentsChanged();
  return created;
}

/**
 * End an environment (REQ-1487).
 *
 * `deleteBranch` also drops the local branch, and `deleteRemoteBranch` the copy on the org's git
 * host — a separate ask (REQ-1546), because the remote is what survives a lost volume, and only
 * offered underneath the local one (REQ-1550).
 */
export async function deleteEnvironment(
  orgId: string,
  name: string,
  opts: { deleteBranch?: boolean; deleteRemoteBranch?: boolean } = {},
): Promise<void> {
  const query = new URLSearchParams({
    delete_branch: String(Boolean(opts.deleteBranch)),
    delete_remote_branch: String(Boolean(opts.deleteRemoteBranch)),
  });
  const res = await fetch(`${base(orgId)}/${encodeURIComponent(name)}?${query}`, {
    method: "DELETE",
  });
  if (!res.ok) {
    const data = await res.json().catch(() => ({ detail: res.statusText }));
    throw new Error(serverMessage(data, requestFailed("deleteEnvironment", res.status)));
  }
  announceEnvironmentsChanged();
}

export async function patchEnvironment(
  orgId: string,
  name: string,
  body: { expires_at?: string | null; clear_expiry?: boolean; protected?: boolean | null },
): Promise<Environment> {
  const res = await fetch(`${base(orgId)}/${encodeURIComponent(name)}`, json("PATCH", body));
  const answer = await ok<{ environment: Environment }>(res, "patchEnvironment");
  announceEnvironmentsChanged();
  return answer.environment;
}

/**
 * What merging `fromEnv` into `name` WOULD do, applying none of it (REQ-1527).
 *
 * A GET, and not `POST /merge` with `dry_run`: a caller that got the flag wrong would apply the
 * merge it meant to inspect, and a method that cannot write cannot make that mistake.
 */
export async function previewMerge(
  orgId: string,
  name: string,
  fromEnv: string,
  removals = false,
): Promise<{ report: CopyReport; applied: boolean; requires_approval: boolean }> {
  const query = new URLSearchParams({ from_env: fromEnv, removals: String(removals) });
  const res = await fetch(`${base(orgId)}/${encodeURIComponent(name)}/merge-preview?${query}`);
  return ok(res, "previewMerge");
}

/**
 * Merge `from_env` into `name` (REQ-1490, REQ-1504).
 *
 * A merge into a PROTECTED target does not apply — it files a request for an org_admin to decide,
 * and the answer carries `request` instead of `report`. `removals` is a separate confirmation, so
 * a merge cannot silently empty an environment.
 */
export async function mergeEnvironment(
  orgId: string,
  name: string,
  // REQ-1550: `message` is REQUIRED by the server — the merge lands as one squashed commit, so
  // this sentence is the only account of the range of work it stands for. `retire_remote` is an
  // option ON `retire_source` (REQ-1549) and is refused without it.
  body: {
    from_env: string;
    removals?: boolean;
    message: string;
    retire_source?: boolean;
    retire_remote?: boolean;
  },
): Promise<MergeOutcome> {
  const res = await fetch(`${base(orgId)}/${encodeURIComponent(name)}/merge`, json("POST", body));
  return ok(res, "mergeEnvironment");
}

/**
 * How a branch stands against the remote (REQ-1546).
 *
 * Computed from refs alone and never from the network, so rendering a badge never dials the org's
 * git host: `behind` is therefore as of the last fetch. `ahead`/`behind` are null when one side of
 * the pair is missing — unknown, not zero. `diverged` is reported, never resolved.
 */
export interface BranchSync {
  local: string | null;
  remote: string | null;
  ahead: number | null;
  behind: number | null;
  diverged: boolean;
  unsynced: boolean;
}

/** Every branch's standing against the remote, and whether there is a remote (REQ-1546/REQ-1552). */
export interface RepoSync {
  // REQ-1552: false means nothing this organization holds is mirrored anywhere. Without it, a
  // branch with no remote counterpart is indistinguishable from one that is up to date.
  remote_configured: boolean;
  branches: Record<string, BranchSync>;
}

/** Every branch's standing against the remote, local and remote-only names alike (REQ-1546). */
export async function fetchBranchSync(orgId: string): Promise<RepoSync> {
  const res = await fetch(`${base(orgId)}/-/repo-integration/sync`);
  return ok<RepoSync>(res, "fetchBranchSync");
}

/**
 * Send this environment's branch to the remote (REQ-1546).
 *
 * The mirror after every commit is best effort (REQ-1527), so a branch can hold work the remote
 * does not. This is the repair for exactly that.
 */
export async function pushEnvironment(
  orgId: string,
  name: string,
): Promise<{ pushed: string | null; sync: BranchSync }> {
  const res = await fetch(`${base(orgId)}/${encodeURIComponent(name)}/push`, { method: "POST" });
  return ok(res, "pushEnvironment");
}

/**
 * Fetch the remote and apply what it holds for this environment (REQ-1547).
 *
 * A pull is an apply: the fetch writes only the remote-tracking refs, and the branch moves because
 * the applied model is written through. A divergence is refused (409), never merged.
 */
export async function pullEnvironment(
  orgId: string,
  name: string,
): Promise<{ applied: boolean; report?: CopyReport; sync: BranchSync }> {
  const res = await fetch(`${base(orgId)}/${encodeURIComponent(name)}/pull`, { method: "POST" });
  // REQ-1556: a divergence is refused, and the refusal names the objects both lines moved. That
  // list is the whole point of the refusal -- "the two lines diverged" is not a statement about any
  // particular object -- so it is carried on the error rather than flattened into its message.
  if (res.status === 409) {
    const data = await res.json().catch(() => ({ detail: res.statusText }));
    if (data?.code === "environments.diverged") {
      throw new DivergedError(
        serverMessage(data, requestFailed("pullEnvironment", res.status)),
        data.params?.base ?? null,
        data.params?.conflicts ?? [],
      );
    }
  }
  return ok(res, "pullEnvironment");
}

/**
 * A pull refused because both lines hold commits the other does not (REQ-1556).
 *
 * REPORTED, NEVER RESOLVED, and here not even applied: nothing changed, and what this carries is
 * the account of which objects whoever now has to decide is deciding about. ``base`` is null when
 * the two lines share no ancestor at all, and an empty ``conflicts`` under it means NOTHING WAS
 * COMPARED rather than nothing collided.
 */
export class DivergedError extends Error {
  constructor(
    message: string,
    readonly base: string | null,
    readonly conflicts: Conflict[],
  ) {
    super(message);
    this.name = "DivergedError";
  }
}

/** Where an environment stands in its own history after a step (REQ-1543). */
export interface Position {
  report: CopyReport;
  deployed_sha: string;
  redo_sha: string | null;
  refreshed: boolean;
}

/**
 * Put the environment back to the commit before the one it is at (REQ-1543).
 *
 * An undo is an apply like any other — the model at the earlier commit is applied, and the
 * position moves. `redo_sha` is where the undo departed from, which is what makes redo possible.
 */
export async function undoEnvironment(orgId: string, name: string): Promise<Position> {
  const res = await fetch(`${base(orgId)}/${encodeURIComponent(name)}/undo`, { method: "POST" });
  return ok(res, "undoEnvironment");
}

/** Step forward again toward the position an undo departed from (REQ-1543). */
export async function redoEnvironment(orgId: string, name: string): Promise<Position> {
  const res = await fetch(`${base(orgId)}/${encodeURIComponent(name)}/redo`, { method: "POST" });
  return ok(res, "redoEnvironment");
}

/** Where a review of a branch is happening, on the org's git host (REQ-1551). */
export interface PullRequest {
  url: string;
  number: number;
  new: boolean;
}

/**
 * Ask the git host to review this environment's branch (REQ-1551).
 *
 * For a target branch governed by pull requests, this is the merge: an approval inside Provisa
 * decides nothing the host would honour. The branch is pushed by the same call, and `into`
 * defaults to the environment this one was branched from (REQ-1549).
 */
export async function requestReview(
  orgId: string,
  name: string,
  body: { message: string; into?: string | null },
): Promise<{ pull_request: PullRequest; pushed: string | null; sync: BranchSync }> {
  const res = await fetch(`${base(orgId)}/${encodeURIComponent(name)}/review`, json("POST", body));
  return ok(res, "requestReview");
}

/**
 * What a merge call did. Exactly one of `report` and `request` is present: a merge that ran
 * reports what it changed, and one that was proposed carries the request an approver will read.
 */
export interface MergeOutcome {
  report?: CopyReport;
  request?: MergeRequest;
  applied: boolean;
  requires_approval: boolean;
}

export async function fetchMergeRequests(orgId: string, openOnly = false): Promise<MergeRequest[]> {
  const res = await fetch(`${base(orgId)}/-/merge-requests?open_only=${String(openOnly)}`);
  const body = await ok<{ requests: MergeRequest[] }>(res, "fetchMergeRequests");
  return body.requests;
}

export async function decideMergeRequest(
  orgId: string,
  requestId: number,
  approve: boolean,
  note?: string,
): Promise<MergeRequest> {
  const res = await fetch(
    `${base(orgId)}/-/merge-requests/${requestId}/decide`,
    json("POST", { approve, note: note ?? null }),
  );
  const answer = await ok<{ request: MergeRequest }>(res, "decideMergeRequest");
  return answer.request;
}

/**
 * What a load did, or would do (REQ-1496). Paths rather than counts, because a path is the thing an
 * approver recognises — the surrogate keys the rows carry belong to the schema they came from.
 */
export interface DeployReport {
  env: string;
  ref: string;
  seed: boolean;
  added: string[];
  changed: string[];
  removed: string[];
  unchanged: number;
}

/** Exactly one of `report` and `request` is present, as with a merge. */
export interface LoadOutcome {
  report?: DeployReport;
  request?: MergeRequest;
  applied: boolean;
  requires_approval: boolean;
}

/**
 * Make the tree at `ref` this environment's model (REQ-1496).
 *
 * THIS IS THE INTERACTIVE HALF of the load. A person signed in to the control plane that will hold
 * the result chooses a branch and applies it here; `provisa env deploy` is the same call for a
 * pipeline. Nothing else applies a tree — no watcher turns somebody's merge into a change here.
 *
 * `dry_run` reports without writing and is what the panel runs first, so the report is read before
 * it is agreed to. `seed` is off unless the load is what CREATES the environment: a tree carries
 * the roles of whatever control plane projected it, and a desktop's self-granted rights must not
 * arrive with it (REQ-1539).
 */
export async function deployEnvironment(
  orgId: string,
  name: string,
  body: { ref: string; dry_run?: boolean; seed?: boolean; message?: string },
): Promise<LoadOutcome> {
  const res = await fetch(`${base(orgId)}/${encodeURIComponent(name)}/deploy`, json("POST", body));
  return ok(res, "deployEnvironment");
}

export async function fetchRepoIntegration(orgId: string): Promise<RepoIntegration> {
  const res = await fetch(`${base(orgId)}/-/repo-integration`);
  return ok<RepoIntegration>(res, "fetchRepoIntegration");
}

/**
 * Set both halves; `null` clears one (REQ-1527).
 *
 * The remote is stored verbatim, so a secret reference — `https://${env:GIT_TOKEN}@host/repo.git`
 * — is what is kept and what is read back. It is resolved at push time and nowhere else, which is
 * why no token is ever typed into this form as a literal.
 */
export async function saveRepoIntegration(
  orgId: string,
  body: { remote: string | null; status_webhook: string | null },
): Promise<RepoIntegration> {
  const res = await fetch(`${base(orgId)}/-/repo-integration`, json("PUT", body));
  return ok<RepoIntegration>(res, "saveRepoIntegration");
}

/** REQ-1537: what was found at a remote, and whether Provisa could create it if it is missing. */
export interface RemoteProbe {
  exists: boolean;
  kind: string;
  creatable: boolean;
  target: string;
  detail: string;
}

/**
 * Ask whether the remote names a repository that exists (REQ-1537).
 *
 * Read-only, and probed BEFORE the field is saved: `remote` is the candidate being typed, so a
 * typo is caught while the field is still open. Omit it to re-check the stored one.
 */
export async function probeRepoRemote(orgId: string, remote?: string | null): Promise<RemoteProbe> {
  const res = await fetch(
    `${base(orgId)}/-/repo-integration/probe`,
    json("POST", { remote: remote ?? null }),
  );
  return ok<RemoteProbe>(res, "probeRepoRemote");
}

/**
 * Create the repository the remote names, because an operator asked for it (REQ-1537).
 *
 * Never called on Provisa's own initiative — a missing repository is as likely to be a typo as an
 * omission. The address created is the one the operator answered about, passed explicitly.
 */
export async function createRepoRemote(
  orgId: string,
  remote: string,
  isPrivate = true,
): Promise<RemoteProbe> {
  const res = await fetch(
    `${base(orgId)}/-/repo-integration/create-remote`,
    json("POST", { remote, private: isPrivate }),
  );
  return ok<RemoteProbe>(res, "createRepoRemote");
}

/**
 * Bring the org's remote branches back as `origin/*` (REQ-1541).
 *
 * The other half of the mirror: the projection is pushed to the org's own git host, the pull
 * request is reviewed and merged there, and this is what makes the merged branch nameable by a
 * deploy. An act, never a poll — Provisa dials another organization's host with that
 * organization's credential only when somebody asks it to.
 */
export async function fetchRemoteBranches(orgId: string): Promise<Record<string, string>> {
  const res = await fetch(`${base(orgId)}/-/repo-integration/fetch`, json("POST", {}));
  const body = await ok<{ branches: Record<string, string> }>(res, "fetchRemoteBranches");
  return body.branches;
}

/** What the last fetch found, read from refs — this dials nothing (REQ-1541). */
export async function listRemoteBranches(orgId: string): Promise<Record<string, string>> {
  const res = await fetch(`${base(orgId)}/-/repo-integration/remote-branches`);
  const body = await ok<{ branches: Record<string, string> }>(res, "listRemoteBranches");
  return body.branches;
}

/** Every branch in the org's repository — including refs whose environment has been deleted. */
export async function fetchRepoBranches(orgId: string): Promise<string[]> {
  const res = await fetch(`${base(orgId)}/-/repo/branches`);
  const body = await ok<{ branches: string[] }>(res, "fetchRepoBranches");
  return body.branches;
}

export async function fetchRepoHistory(
  orgId: string,
  ref: string,
  limit = 100,
): Promise<RepoCommit[]> {
  const query = new URLSearchParams({ ref, limit: String(limit) });
  const res = await fetch(`${base(orgId)}/-/repo/history?${query}`);
  const body = await ok<{ commits: RepoCommit[] }>(res, "fetchRepoHistory");
  return body.commits;
}

export async function fetchRepoFiles(orgId: string, ref: string): Promise<string[]> {
  const res = await fetch(`${base(orgId)}/-/repo/files?ref=${encodeURIComponent(ref)}`);
  const body = await ok<{ paths: string[] }>(res, "fetchRepoFiles");
  return body.paths;
}

export async function fetchRepoFile(orgId: string, ref: string, path: string): Promise<string> {
  const query = new URLSearchParams({ ref, path });
  const res = await fetch(`${base(orgId)}/-/repo/file?${query}`);
  const body = await ok<{ text: string }>(res, "fetchRepoFile");
  return body.text;
}
