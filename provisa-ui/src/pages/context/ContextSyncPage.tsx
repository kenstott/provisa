// Copyright (c) 2026 Kenneth Stott
// Canary: da5da7f4-8ba6-4058-8ca7-af7e647b2b68
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { SyncingTab } from "../../components/admin/syncing/SyncingTab";

/** REQ-2259, REQ-2247: Context Sync -- every outside system kept in step with Provisa, moved out of
 * Admin into the Context group with its rights unchanged. SyncingTab draws the page's header and list. */
export function ContextSyncPage() {
  return (
    <div className="page page-sticky-head">
      <SyncingTab />
    </div>
  );
}
