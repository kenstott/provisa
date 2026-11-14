// Copyright (c) 2026 Kenneth Stott
// Canary: 9b2e4c7a-1d58-4f36-a0e9-6c3b8f1d2e47
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import type { Tag, TagTarget } from "../types/admin";

/** useOfferedTags as a test stands in for the server's offeredTags: the tags that apply to the
 *  target's kind and are not derived, less those `notOffered` names (a tag outside the object's
 *  domains, which only the server can tell). */
export function offeredTagsFor(tags: Tag[], target: TagTarget, notOffered: ReadonlySet<string>) {
  return {
    offered: new Set(
      tags
        .filter((tag) => tag.appliesTo.includes(target.objectType) && !tag.derived)
        .filter((tag) => !notOffered.has(tag.id))
        .map((tag) => tag.id),
    ),
    loading: false,
  };
}
