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

export { TOPIC_IDS };
export type { TopicId };

/** Topics the viewer has completed (Done on a topic's last step), in the order they were finished. */
export function completedTopics(): TopicId[] {
  try {
    const raw = localStorage.getItem(TOUR_TOPICS_DONE_KEY);
    if (raw === null) return [];
    const parsed: unknown = JSON.parse(raw);
    if (!Array.isArray(parsed)) return [];
    return parsed.filter((id): id is TopicId => (TOPIC_IDS as readonly unknown[]).includes(id));
  } catch {
    // REQ-1945: completion marks are a per-viewer convenience; unreadable storage means no marks.
    return [];
  }
}

export function markTopicCompleted(id: TopicId): void {
  const done = completedTopics();
  if (done.includes(id)) return;
  try {
    localStorage.setItem(TOUR_TOPICS_DONE_KEY, JSON.stringify([...done, id]));
  } catch {
    // REQ-1945: as above -- a browser that refuses storage just does not remember the mark.
  }
}
