// Copyright (c) 2026 Kenneth Stott
// Canary: 6ee40908-d5d5-4f9c-b511-9853f1ac8da2
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2112: the two things the extension does on a Provisa instance.
//
// DEV MODE: a code source pointed at the workspace folder, a server directory (REQ-2121) the
// worker reads where it is, so an edit is read by the next query. The instance must be able to
// read the folder (a single-organization install on this machine, under the operator's allowed
// directories); it refuses by name otherwise, and the extension shows that refusal.
//
// BUILD IMAGE: the dev-mode source's image, through the routes the UI's Build image panel calls
// (REQ-2098), polled until it is done; its compose service is what the extension copies.

"use strict";

/** The source type and runtime of a workspace's language (provisa/codesource/languages.py). */
const LANGUAGE = {
  python: { type: "python", runtime: "python" },
  typescript: { type: "node", runtime: "node" },
};

/** The createSource input of a dev-mode source reading `folder` in place. */
function devSourceInput(sourceId, language, folder) {
  const lang = LANGUAGE[language];
  if (lang === undefined) throw new Error(`a code source is python or typescript, not ${language}`);
  return {
    id: sourceId,
    type: lang.type,
    mappingJson: JSON.stringify({ runtime: lang.runtime, code: { kind: "directory", path: folder } }),
  };
}

/** Points source `sourceId` at `folder` in dev mode on the client's instance. A source of that id
 * that already reads `folder` in dev mode is left as it is; one that reads other code is refused
 * by name, since a save replaces every setting of a source. */
async function pointAtWorkspace(client, sourceId, language, folder) {
  const input = devSourceInput(sourceId, language, folder);
  const existing = (await client.sources()).find((s) => s.id === sourceId);
  if (existing !== undefined) {
    const mapping = JSON.parse(existing.mappingJson);
    const code = mapping.code;
    const dev = mapping.mode === undefined || mapping.mode === "dev";
    if (existing.type === input.type && code && code.kind === "directory" && code.path === folder && dev) {
      return { created: false };
    }
    throw new Error(`the source ${sourceId} already exists and reads other code; choose another name`);
  }
  await client.createSource(input);
  return { created: true };
}

/** Builds `sourceId`'s image (`choice` is {arch, registry_id, download}) and waits for it; resolves
 * to the finished build, whose `compose` is the service that runs it. */
async function buildImage(client, sourceId, choice, sleep, pollMs = 2000) {
  let build = await client.startBuild(sourceId, choice);
  while (build.status === "building") {
    await sleep(pollMs);
    build = await client.build(sourceId, build.id);
  }
  if (build.status === "failed") throw new Error(`the image build failed: ${build.error}`);
  return build;
}

module.exports = { devSourceInput, pointAtWorkspace, buildImage };
