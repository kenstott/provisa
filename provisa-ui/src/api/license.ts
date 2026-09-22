// Copyright (c) 2026 Kenneth Stott
// Canary: 6a1d9c34-8f52-4b0e-9c7a-1e5d3f8b0a62
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { requestFailed } from "../i18n/serverMessage";

/**
 * The offline trial/license state (REQ-1137 companion). `licensed` is exposed regardless of trial
 * status, unlike `should_nag`/`nag_text`, which only populate once the 30-day trial has elapsed —
 * the UI's persistent "Unregistered" indicator reads `licensed` alone so it shows from day one.
 */
export interface LicenseStatus {
  licensed: boolean;
  should_nag: boolean;
  nag_text: string | null;
}

export async function fetchLicenseStatus(): Promise<LicenseStatus> {
  const res = await fetch("/auth/license-status");
  if (!res.ok) throw new Error(requestFailed("License status", res.status));
  return res.json();
}
