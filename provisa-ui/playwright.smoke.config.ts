// Copyright (c) 2026 Kenneth Stott
// Canary: 66af752e-5e7f-49ad-8686-6a3e460b7296
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.

// The smoke check's browser step (REQ-1989). tests/smoke/test_main_is_startable.py has already
// started the one backend and runs this; only the page server is started here. No global setup:
// the backend already holds the demo.
import { defineConfig } from "@playwright/test";

const UI_PORT = Number(process.env.PROVISA_E2E_UI_PORT);
const API_PORT = Number(process.env.PROVISA_API_PORT);
if (!UI_PORT || !API_PORT) {
  throw new Error("PROVISA_E2E_UI_PORT and PROVISA_API_PORT name the running smoke instance");
}

export default defineConfig({
  testDir: "./e2e",
  testMatch: /smoke-explore\.spec\.ts$/,
  workers: 1,
  retries: 0,
  forbidOnly: true,
  reporter: "line",
  timeout: 120_000,
  use: { baseURL: `http://localhost:${UI_PORT}`, reducedMotion: "reduce" },
  webServer: {
    command: "npm run dev",
    url: `http://localhost:${UI_PORT}`,
    reuseExistingServer: false,
    timeout: 120_000,
    env: {
      PROVISA_UI_PORT: String(UI_PORT),
      PROVISA_API_PORT: String(API_PORT),
      // One backend: the page server's per-worker routing stays off.
      PROVISA_E2E_BACKEND_PORTS: "",
    },
  },
});
