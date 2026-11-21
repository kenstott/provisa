// Copyright (c) 2026 Kenneth Stott
// Canary: e4a91c37-2b6d-4f50-8d1e-6c03b9f7a2d8
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { afterEach, beforeEach } from "vitest";

/**
 * Declares the deployment's base domain as ui_server's /deployment-config.js does, or withdraws the
 * declaration with null. Org-subdomain addressing (REQ-1276) exists only under a declaration.
 */
export function declareBaseDomain(base: string | null): void {
  window.__PROVISA_ORG_BASE_DOMAIN__ = base;
}

/** Runs each test of the enclosing file or block on a deployment that declares `base`. */
export function onDeclaredBaseDomain(base: string): void {
  beforeEach(() => declareBaseDomain(base));
  afterEach(() => declareBaseDomain(null));
}
