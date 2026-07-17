// Copyright (c) 2026 Kenneth Stott
// Canary: 61979c73-3313-47a6-aa3c-1f8849ed151e
// Canary: placeholder
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.

import * as fs from "fs";
import * as path from "path";
import { fileURLToPath } from "url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const CONFIG_PATH = path.resolve(__dirname, "../../config/provisa-install.yaml");
const BACKUP_PATH = CONFIG_PATH.replace(".yaml", ".yaml.bak");
const SNAPSHOT_PATH = CONFIG_PATH + ".snapshot";

export default async function globalTeardown() {
  if (fs.existsSync(SNAPSHOT_PATH)) {
    fs.copyFileSync(SNAPSHOT_PATH, CONFIG_PATH);
    fs.rmSync(SNAPSHOT_PATH);
  }
  if (fs.existsSync(BACKUP_PATH)) {
    fs.rmSync(BACKUP_PATH);
  }
}
