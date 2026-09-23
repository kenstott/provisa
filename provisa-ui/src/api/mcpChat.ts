// Copyright (c) 2026 Kenneth Stott
// Canary: 9a4c7e21-5b3a-4f8e-a1c6-2d9f5e3b7a04
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { requestFailed } from "../i18n/serverMessage";

const API_BASE = import.meta.env.VITE_API_BASE || "";

/** REQ-1804: whether Polly (the chat assistant) has a usable vendor/model/credential. */
export interface McpChatStatus {
  configured: boolean;
  reason: string;
}

export async function fetchMcpChatStatus(): Promise<McpChatStatus> {
  const resp = await fetch(`${API_BASE}/admin/mcp/chat/status`);
  if (!resp.ok) throw new Error(requestFailed("chat status", resp.status));
  return resp.json();
}
