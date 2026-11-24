// Copyright (c) 2026 Kenneth Stott
// Canary: f649deba-70cd-48a3-a8a1-bd476ce73746
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2112 (amended 2026-11-22), REQ-2106: the code-source services shim reached locally. Outside
// Provisa a signed-in developer reaches the services through a server route keyed to their
// personal access token, scoped to their dev-mode source. The extension forwards that address as
// $PROVISA_SERVICES into every VS Code terminal and every debug session, so provisa_services and
// @provisa/services run from the editor as they do in a worker.

"use strict";

/** The one variable code is given to reach Provisa's services (provisa/codesource/services.py
 * SERVICES_ENV). */
const SERVICES_ENV = "PROVISA_SERVICES";

/** The services address of dev-mode source `sourceId` on `instance` ({url, org}) for the developer
 * holding personal access token `token`. The shim appends /<service>/<call> to it and sends no
 * header of its own, so the token is part of the address, as a worker's is. */
function servicesAddress(instance, sourceId, token) {
  if (!instance.url) throw new Error("the Provisa instance has no url");
  if (!instance.org) throw new Error(`the Provisa instance ${instance.url} names no org`);
  if (!sourceId) throw new Error("run Provisa: Run in Dev Mode first; the services are a dev-mode source's");
  if (!token) throw new Error(`not signed in to ${instance.url}: run Provisa: Sign In`);
  const base = instance.url.replace(/\/+$/, "");
  const org = encodeURIComponent(instance.org);
  const source = encodeURIComponent(sourceId);
  return `${base}/admin/orgs/${org}/code-sources/${source}/services/${encodeURIComponent(token)}`;
}

/** The variables forwarded into terminals and debug sessions. */
function servicesEnv(instance, sourceId, token) {
  return { [SERVICES_ENV]: servicesAddress(instance, sourceId, token) };
}

/** Sets `env` on a VS Code EnvironmentVariableCollection, so each new terminal has it. The
 * collection is not persisted: it holds a personal access token, which lives only in the
 * extension's secret storage and is set again on each activation. */
function forwardToTerminals(collection, env, describedAs) {
  collection.persistent = false;
  collection.clear();
  for (const [name, value] of Object.entries(env)) collection.replace(name, value);
  collection.description = describedAs;
}

/** `config`, a debug configuration being launched, with `env` added to its environment. A value
 * the configuration sets itself is kept: a launch.json entry is the developer's own choice. */
function withServices(config, env) {
  return { ...config, env: { ...env, ...(config.env || {}) } };
}

module.exports = { SERVICES_ENV, servicesAddress, servicesEnv, forwardToTerminals, withServices };
