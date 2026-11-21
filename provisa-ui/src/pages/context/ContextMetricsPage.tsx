// Copyright (c) 2026 Kenneth Stott
// Canary: 2b7c4f0e-6a1d-4e3b-9c55-81f0d2a7e9b4
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { MetricsPage } from "../MetricsPage";

/** REQ-2259: the Metrics page of the Context group -- the metrics as the Derived page shows them,
 * read-only. Metrics are edited under Derived. */
export function ContextMetricsPage() {
  return <MetricsPage readOnly />;
}
