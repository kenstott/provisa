// Copyright (c) 2026 Kenneth Stott
// Canary: 470de4d2-6347-4dd2-8fca-835ad3b036db
// Canary: placeholder
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { serverMessage, requestFailed } from "../i18n/serverMessage";

export interface EmailPreference {
  email_opt_in: boolean;
}

const base = () => `/auth/email`;

export async function getEmailPreferences(): Promise<EmailPreference> {
  const res = await fetch(`${base()}/preferences`);
  if (!res.ok) {
    const data = await res.json().catch(() => ({ detail: res.statusText }));
    throw new Error(serverMessage(data, requestFailed("getEmailPreferences", res.status)));
  }
  return res.json();
}

export async function updateEmailPreferences(pref: EmailPreference): Promise<EmailPreference> {
  const res = await fetch(`${base()}/preferences`, {
    method: "PATCH",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(pref),
  });
  if (!res.ok) {
    const data = await res.json().catch(() => ({ detail: res.statusText }));
    throw new Error(serverMessage(data, requestFailed("updateEmailPreferences", res.status)));
  }
  return res.json();
}
