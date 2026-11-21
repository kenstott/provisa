// Copyright (c) 2026 Kenneth Stott
// Canary: 6c1f8e3a-2d47-4b90-a5e2-8f3b7d0c4e19
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import type { ColumnTagChange, TagLossResolution } from "../../api/glossary";
import type { MutationResult } from "../../types/admin";

/** A refusal over the tags a re-homed key ref would lose (REQ-2145): each column's change. */
export function tagLossOf(result: MutationResult): ColumnTagChange[] | null {
  return result.code === "tag.move_loses"
    ? (result.params as { columns: ColumnTagChange[] }).columns
    : null;
}

/** How the save goes ahead over those tags, as RelationshipInput takes it. */
export function tagLossInput(resolve?: TagLossResolution) {
  return resolve
    ? { confirmTagLoss: resolve.confirm_tag_loss ?? [], carryTags: resolve.carry_tags === true }
    : {};
}
