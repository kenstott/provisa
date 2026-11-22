#!/usr/bin/env node
// Copyright (c) 2026 Kenneth Stott
// Canary: e18b5f02-6a37-4c9d-84e1-b3d70f29a5c6
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// `npx @provisa/sdk check [args]` runs `uvx provisa==<this SDK's version> code check [args]`.
// No logic of its own: the check is the server's own reader at the matching version (REQ-1984).
import { spawnSync } from "node:child_process";
import { readFileSync } from "node:fs";

export function command(args, version) {
  if (args[0] !== "check") {
    throw new Error("usage: provisa-sdk check [DIR] [--json] ...");
  }
  return ["uvx", `provisa==${version}`, "code", "check", ...args.slice(1)];
}

const own = JSON.parse(readFileSync(new URL("../package.json", import.meta.url), "utf8"));

if (import.meta.url === `file://${process.argv[1]}`) {
  let argv;
  try {
    argv = command(process.argv.slice(2), own.version);
  } catch (e) {
    console.error(e.message);
    process.exit(2);
  }
  const done = spawnSync(argv[0], argv.slice(1), { stdio: "inherit" });
  if (done.error) {
    console.error(`provisa-sdk check: ${done.error.message}`);
    process.exit(2);
  }
  process.exit(done.status);
}
