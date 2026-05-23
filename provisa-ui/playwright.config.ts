// Copyright (c) 2026 Kenneth Stott
// Canary: 28ec90e3-f56d-47df-81ca-eed6b47465a2
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { defineConfig } from "@playwright/test";
import * as fs from "fs";
import * as path from "path";
import { fileURLToPath } from "url";

// Load root .env so live-backend tests receive AWS credentials
const __dirname = path.dirname(fileURLToPath(import.meta.url));
const rootEnv = path.resolve(__dirname, "../.env");
if (fs.existsSync(rootEnv)) {
  for (const line of fs.readFileSync(rootEnv, "utf8").split("\n")) {
    const trimmed = line.trim();
    if (!trimmed || trimmed.startsWith("#") || !trimmed.includes("=")) continue;
    const [k, ...rest] = trimmed.split("=");
    const key = k.trim();
    if (!process.env[key]) process.env[key] = rest.join("=").trim();
  }
}

export default defineConfig({
  testDir: "./e2e",
  testMatch: /[/\\][^.][^/\\]*\.spec\.ts$/,
  timeout: 30000,
  retries: 1,
  use: {
    baseURL: "http://localhost:3000",
    headless: true,
  },
  webServer: [
    {
      command: "npm run dev",
      port: 3000,
      reuseExistingServer: !process.env.CI,
      timeout: 15000,
    },
    {
      command: "bash -c 'cd .. && .venv/bin/uvicorn main:app --host 0.0.0.0 --port 8000'",
      url: "http://localhost:8000/health",
      reuseExistingServer: !process.env.CI,
      timeout: 30000,
    },
  ],
});
