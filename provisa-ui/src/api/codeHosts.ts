// Copyright (c) 2026 Kenneth Stott
// Canary: 81a82953-b7ce-4df5-8b91-31c32e4644ed
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2098: a code source's image and remote host -- building the image of a dev-mode source
// (pushed to a container registry, or downloaded), connecting the remote host running it (which
// switches the source to built mode), its host's status, re-pinning, and going back to dev.

import { requestFailed, serverMessage } from "../i18n/serverMessage";

const API_BASE = import.meta.env.VITE_API_BASE || "";

export type Arch = "amd64" | "arm64";

export interface Build {
  id: string;
  source_id: string;
  code_hash: string;
  arch: Arch;
  registry_id: string | null;
  status: "building" | "done" | "failed";
  image_ref: string | null;
  digest: string | null;
  node: string | null;
  error: string | null;
  started_at: string;
  finished_at: string | null;
  /** The compose service to run the image, once it is built. */
  compose: string | null;
}

export interface Connected {
  mode: "built";
  remote: Record<string, unknown>;
  content_hash: string;
}

export interface HostStatus {
  mode: "dev" | "built";
  host?: string;
  pinned_hash?: string;
  answering?: boolean;
  /** The code hash the host reports. */
  code_hash?: string;
  error?: string;
}

async function ok<T>(res: Response, op: string): Promise<T> {
  if (!res.ok) {
    const data = await res.json().catch(() => ({ detail: res.statusText }));
    throw new Error(serverMessage(data, requestFailed(op, res.status)));
  }
  return res.json() as Promise<T>;
}

const base = (orgId: string, sourceId: string) =>
  `${API_BASE}/admin/orgs/${encodeURIComponent(orgId)}/code-sources/${encodeURIComponent(sourceId)}`;

const post = (url: string, body?: unknown) =>
  fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: body === undefined ? undefined : JSON.stringify(body),
  });

export async function startBuild(
  orgId: string,
  sourceId: string,
  body: { arch: Arch; registry_id: string | null; download: boolean },
): Promise<Build> {
  return ok(await post(`${base(orgId, sourceId)}/builds`, body), "startBuild");
}

export async function fetchBuild(orgId: string, sourceId: string, buildId: string): Promise<Build> {
  return ok(
    await fetch(`${base(orgId, sourceId)}/builds/${encodeURIComponent(buildId)}`),
    "fetchBuild",
  );
}

export async function fetchBuilds(orgId: string, sourceId: string): Promise<Build[]> {
  return (
    await ok<{ builds: Build[] }>(await fetch(`${base(orgId, sourceId)}/builds`), "fetchBuilds")
  ).builds;
}

export const archiveUrl = (orgId: string, sourceId: string, buildId: string) =>
  `${base(orgId, sourceId)}/builds/${encodeURIComponent(buildId)}/archive`;

export async function connectHost(
  orgId: string,
  sourceId: string,
  body: { host: string; port: number; pairing_code: string },
): Promise<Connected> {
  return ok(await post(`${base(orgId, sourceId)}/connect`, body), "connectHost");
}

export async function backToDev(orgId: string, sourceId: string): Promise<void> {
  await ok(await post(`${base(orgId, sourceId)}/dev`), "backToDev");
}

export async function fetchHostStatus(orgId: string, sourceId: string): Promise<HostStatus> {
  return ok(await fetch(`${base(orgId, sourceId)}/host`), "fetchHostStatus");
}

export async function repin(
  orgId: string,
  sourceId: string,
  codeHash: string,
): Promise<{ content_hash: string }> {
  return ok(await post(`${base(orgId, sourceId)}/repin`, { code_hash: codeHash }), "repin");
}
