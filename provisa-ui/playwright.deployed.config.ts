// Copyright (c) 2026 Kenneth Stott
// Canary: e8ca7d71-62fb-431e-95ae-6d162c22e680
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.

import { defineConfig } from "@playwright/test";

/**
 * The deployed target: specs run against a native-tier deployment that is already up and that this
 * run did not start — `provisa run --demo` as a service (.github/workflows/nixos.yml runs it on
 * NixOS). One origin serves the SPA and the API, and the demo config has no sign-in, so there is
 * nothing to provision and no global setup. coverage.ts calls a deployment it does not own the
 * `cloud` target; this is that target with no IdP in front of it.
 *
 * What runs is what a host owes a live AskAmerica source: added with the free key, its bundled
 * server is listening within a minute (askamerica-server-listening.spec.ts). Registering a table
 * and querying it wait on that server's catalog, which the UI lanes' full case judges.
 */
const DEPLOYED_URL = process.env.PROVISA_E2E_DEPLOYED_URL;
if (!DEPLOYED_URL) {
  throw new Error("PROVISA_E2E_DEPLOYED_URL is required: the origin of the running deployment");
}
process.env.PROVISA_E2E_TARGET = "cloud";
process.env.PROVISA_E2E_CLOUD_URL = DEPLOYED_URL;

export default defineConfig({
  testDir: "./e2e",
  // One deployment and one control plane: there is no backend per worker to isolate a second one.
  workers: 1,
  timeout: 180000,
  use: {
    baseURL: DEPLOYED_URL,
    headless: true,
  },
  projects: [
    {
      name: "deployed",
      testMatch: ["**/askamerica-server-listening.spec.ts"],
    },
  ],
});
