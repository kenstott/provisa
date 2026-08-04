// Copyright (c) 2026 Kenneth Stott
// Canary: 8d1c74a0-5b2e-4f36-9c81-6a0f2e5d3b47
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// Lifecycle for the community Neo4j instance that neo4j-docker-export.spec.ts exports into.
//
// It lives here rather than in that spec's beforeAll/afterAll because starting or removing a
// container attaches and detaches a veth pair on the host's docker bridge, and Chromium's
// NetworkChangeNotifier watches netlink for exactly that. Every browser in every other worker
// reacts by tearing down its in-flight connections, which surfaces as a burst of
// net::ERR_NETWORK_CHANGED and whichever specs were mid-navigation at that instant failing.
// Driven from globalSetup/globalTeardown, the churn happens before the first browser launches
// and after the last one closes, where nothing is listening for it.

import { execSync, spawnSync } from "child_process";

export const NEO4J_HTTP_PORT = 17474;
export const NEO4J_BOLT_PORT = 17687;
export const NEO4J_URL = `http://localhost:${NEO4J_HTTP_PORT}`;
export const CONTAINER_NAME = "e2e-neo4j-community-export";

export function startNeo4jContainer(): void {
  spawnSync("docker", ["rm", "-f", CONTAINER_NAME], { stdio: "pipe" });
  execSync(
    [
      "docker", "run", "-d",
      "--name", CONTAINER_NAME,
      "-p", `${NEO4J_HTTP_PORT}:7474`,
      "-p", `${NEO4J_BOLT_PORT}:7687`,
      "-e", "NEO4J_AUTH=none",
      "neo4j:5.19-community",
    ].join(" "),
    { stdio: "pipe" },
  );
}

export function removeNeo4jContainer(): void {
  spawnSync("docker", ["rm", "-f", CONTAINER_NAME], { stdio: "pipe" });
}
