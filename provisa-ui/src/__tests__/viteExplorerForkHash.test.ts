// @vitest-environment node
// Copyright (c) 2026 Kenneth Stott
// Canary: 0b8e61c4-7d52-4f0a-b7a4-5c2d9e31f0a8
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2163: Vite pre-bundles the aliased explorer fork, and its optimizer hash covers the lockfile
// and the config but not the fork's content. Without the content in the config, an edit to the fork
// left the old pre-bundle in service (ontology and ops root fields rendered loose after the fix
// landed). The config's optimizer options must carry the fork's content.
import { describe, it, expect } from "vitest";
import { createHash } from "crypto";
import fs from "fs";
import path from "path";
import config from "../../vite.config";

describe("vite optimizer follows the explorer fork's content (REQ-2163)", () => {
  it("puts a hash of the fork's file in the optimizer options Vite hashes", () => {
    type Resolved = {
      optimizeDeps: { rolldownOptions: { transform: { define: Record<string, string> } } };
    };
    const resolved = (config as unknown as (e: { mode: string; command: string }) => Resolved)({
      mode: "development",
      command: "serve",
    });
    const want = createHash("sha256")
      .update(fs.readFileSync(path.resolve(__dirname, "../plugins/graphiql-explorer-fork.cjs")))
      .digest("hex")
      .slice(0, 16);
    const define = resolved.optimizeDeps.rolldownOptions.transform.define;
    expect(define.__GRAPHIQL_EXPLORER_FORK_HASH__).toBe(JSON.stringify(want));
  });
});
