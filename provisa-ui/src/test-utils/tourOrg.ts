// Copyright (c) 2026 Kenneth Stott
// Canary: 7736615b-52bf-4c18-85b7-365c854d085c
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import type { OrgHas } from "../tour/tourPrerequisites";

/** REQ-2013: an org that has every object the product tour's steps show -- the standard demo's. */
export const STANDARD_DEMO_ORG: OrgHas = {
  sources: new Set(["pet-store-sqlite", "inquiries-sqlite", "dq-checker", "demo-profiler"]),
  tables: new Set([
    "pet-store-sqlite.pets",
    "inquiries-sqlite.inquiries",
    "inquiries-sqlite.users",
    "dq-checker.pets_quality_dq_checker_dq",
  ]),
  dataProducts: 2,
  pollyConfigured: true,
};
