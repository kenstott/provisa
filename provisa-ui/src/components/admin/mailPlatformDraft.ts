// Copyright (c) 2026 Kenneth Stott
// Canary: bde39cef-30e2-417f-abf4-e896d71ec058
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1923: what an administrator is typing for one mail platform, and what is still missing.

import type { MailPlatform } from "../../api/mailPlatforms";

export interface Draft {
  client_id: string;
  client_secret: string;
  settings: Record<string, string>;
  organisation_mailboxes: boolean;
}

export const draftOf = (p: MailPlatform): Draft => ({
  client_id: p.client_id ?? "",
  client_secret: "",
  settings: Object.fromEntries(p.settings_fields.map((f) => [f, p.settings[f] ?? ""])),
  organisation_mailboxes: p.organisation_mailboxes,
});

/** What is still needed before a platform's client can be saved. */
export function platformMissing(p: MailPlatform, draft: Draft): string[] {
  const missing: string[] = [];
  if (!draft.client_id.trim()) missing.push("client_id");
  if (!p.configured && !draft.client_secret) missing.push("client_secret");
  for (const field of p.settings_fields) {
    if (!draft.settings[field]?.trim()) missing.push(field);
  }
  return missing;
}
