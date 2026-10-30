// Copyright (c) 2026 Kenneth Stott
// Canary: 4b7e0d93-1a26-4f58-8c3e-6d9f2a0c5e17
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import type { RuleRight } from "../../types/admin";

/** The label of each right a row rule may have (REQ-2188). */
export const RULE_RIGHT_LABEL: Record<RuleRight, string> = {
  R: "securityPage.ruleRightRead",
  U: "securityPage.ruleRightUpdate",
  D: "securityPage.ruleRightDelete",
};
