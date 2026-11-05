// Copyright (c) 2026 Kenneth Stott
// Canary: 5b8e2c17-9d04-4a63-8f1e-7c3a60d4b925
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

/**
 * REQ-1945: the Deep Dives menu's topics and the per-viewer record of which were completed.
 *
 * Completion is a per-viewer convenience (a mark on a card), so it lives in localStorage and a
 * browser that refuses storage simply shows no marks -- that is the design, not a masked failure.
 */

import { TOPIC_IDS, type TopicId } from "./tourSteps";
import { TOUR_TOPICS_DONE_KEY } from "./tourKeys";
import { getStoredJson, setStored } from "../lib/browserStorage";

export { TOPIC_IDS };
export type { TopicId };

/** Topics the viewer has completed (Done on a topic's last step), in the order they were finished. */
export function completedTopics(): TopicId[] {
  const parsed = getStoredJson<unknown>(TOUR_TOPICS_DONE_KEY, { global: true });
  if (!Array.isArray(parsed)) return [];
  return parsed.filter((id): id is TopicId => (TOPIC_IDS as readonly unknown[]).includes(id));
}

export function markTopicCompleted(id: TopicId): void {
  const done = completedTopics();
  if (done.includes(id)) return;
  try {
    setStored(TOUR_TOPICS_DONE_KEY, JSON.stringify([...done, id]), { global: true });
  } catch {
    // REQ-1945: as above -- a browser that refuses storage just does not remember the mark.
  }
}
