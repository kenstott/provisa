// Copyright (c) 2026 Kenneth Stott
// Canary: 13bf0b0e-7062-4f22-b44b-db8483a0c866
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.

import fs from "node:fs";
import path from "node:path";

import { SUPERUSER_TOKEN_KEY } from "../src/lib/sessionToken";
import { TOUR_SEEN_KEY } from "../src/tour/tourKeys";

/**
 * REQ-1472: sign the cloud run in, once, before any spec opens a page.
 *
 * The local harness's global-setup provisions a server it owns — an unauthenticated PUT
 * /admin/config followed by POST /setup. Neither is available here and neither should be: this
 * target is a running deployment fronted by an external IdP, and the run holds no IdP identity.
 * The break-glass exchange is the sign-in that exists for every provider, so the run posts the
 * configured credentials once and hands every worker the resulting session through storageState —
 * the same localStorage key (`provisa_su_token`) the SPA writes on an operator sign-in.
 */
export default async function globalSetup() {
  const baseURL = process.env.PROVISA_E2E_CLOUD_URL;
  if (!baseURL) throw new Error("PROVISA_E2E_CLOUD_URL is required for the cloud target");
  const username = process.env.PROVISA_SUPERUSER_USERNAME;
  const password = process.env.PROVISA_SUPERUSER_PASSWORD;
  if (!username || !password) {
    throw new Error(
      "PROVISA_SUPERUSER_USERNAME and PROVISA_SUPERUSER_PASSWORD are required for the cloud " +
        "target — they are the deployment's break-glass credentials, and there is no other " +
        "sign-in this run can perform against an IdP-backed deployment.",
    );
  }

  const resp = await fetch(`${baseURL}/auth/superuser-login`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ username, password }),
  });
  if (!resp.ok) {
    throw new Error(
      `superuser sign-in failed against ${baseURL}: ${resp.status} ${await resp.text()}`,
    );
  }
  const { access_token: token } = (await resp.json()) as { access_token: string };

  const statePath = process.env.PROVISA_E2E_STORAGE_STATE;
  if (!statePath) throw new Error("PROVISA_E2E_STORAGE_STATE is required for the cloud target");
  fs.mkdirSync(path.dirname(statePath), { recursive: true });
  fs.writeFileSync(
    statePath,
    JSON.stringify({
      cookies: [],
      origins: [
        {
          origin: new URL(baseURL).origin,
          localStorage: [
            { name: SUPERUSER_TOKEN_KEY, value: token },
            // The cloud deployment runs in demo mode, where a browser profile that has not seen
            // the guided tour auto-starts it on first navigation (App.tsx TourAutoStart) and the
            // tour then drives the page away from wherever the spec navigated. The specs that
            // already meet this locally set the flag themselves; here every spec meets it, so the
            // run's storage state carries it. tour-page-preload.spec.ts opts back in with ?tour=1,
            // which starts the tour regardless of the flag.
            { name: TOUR_SEEN_KEY, value: "true" },
          ],
        },
      ],
    }),
  );
}
