// Copyright (c) 2026 Kenneth Stott
// Canary: 9c2e5f8a-1d4b-4a7c-9e6f-3b8d0c1a2f5e
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { serverMessage } from "../i18n/serverMessage";

/**
 * Global kill switch for a --demo/--native desktop launch: kills the uvicorn backend and the
 * Vite UI dev server on this host. The server refuses (409) outside that runtime — there is no
 * lone host-process pair to kill under Docker or in a hosted deployment.
 */
export async function shutdownAllServices(): Promise<void> {
  const res = await fetch("/admin/lifecycle/shutdown", { method: "POST" });
  if (!res.ok) {
    throw new Error(serverMessage(await res.json().catch(() => null), "Stop all services"));
  }
}
