// Copyright (c) 2026 Kenneth Stott
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// The detail panel's lifecycle props for a test that does not look at the lifecycle: no other
// products, no members, no findings, and status actions that answer success.

import { vi } from "vitest";

const ok = () => Promise.resolve({ success: true, message: "" });

export function lifecycleProps() {
  return {
    products: [],
    members: [],
    findings: [],
    onStatusChanged: vi.fn(),
    onStatusFailed: vi.fn(),
    openProduct: vi.fn(),
    statusActions: { publish: vi.fn(ok), deprecate: vi.fn(ok), retire: vi.fn(ok), restore: vi.fn(ok) },
  };
}
