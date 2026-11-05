// Copyright (c) 2026 Kenneth Stott
// Canary: 6c1c66f2-a3d8-4017-90f5-be9e172f1c1a
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

/** The deployment's own org. On a single-tenant deployment it is the one org, data and all; with
 * multi-tenancy it is the Platform org (the control plane's label), and the data is the tenants'. */
export function isPlatformOrg(orgId: string | null, multitenancy: boolean): boolean {
  return multitenancy && orgId === "default";
}
