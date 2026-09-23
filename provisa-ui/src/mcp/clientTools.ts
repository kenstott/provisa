// Copyright (c) 2026 Kenneth Stott
// Canary: 3e8f1a52-9c6d-4b70-8a1f-5d2e7c9b0a34
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import type { NavigateFunction } from "react-router-dom";

/**
 * Client-executed MCP chat tools (REQ-1795) — the browser-only half of the tool set
 * provisa/api/mcp/chat.py declares. The server never runs these; when the model calls one, the
 * chat loop pauses (`awaiting_client_tools`) and the frontend (useMcpChat.ts) calls
 * `executeClientTool` here, then resumes the loop with the result.
 *
 * This module never imports the Apollo client directly (repo lint rule: only App.tsx may — every
 * other caller goes through a hook). `runMutation` is supplied by ChatPanel.tsx, which owns the
 * actual `useMutation` calls, so every mutation dispatched here rides the user's own Apollo
 * session — the SAME authorization (capability checks, domain gates, RLS, the REQ-434
 * creation-request queue for under-privileged callers) a manual click would hit applies
 * unchanged, no new privilege surface. `confirm` similarly defers the actual dialog to ChatPanel.
 */

export interface ClientToolContext {
  navigate: NavigateFunction;
  confirm: (opts: { title: string; message: string }) => Promise<boolean>;
  runMutation: (name: string, variables: Record<string, unknown>) => Promise<ClientToolResult>;
}

export interface ClientToolResult {
  success: boolean;
  message: string;
}

export const CLIENT_TOOL_NAMES = new Set(["navigate", "refresh_mv"]);

export async function executeClientTool(
  name: string,
  input: Record<string, unknown>,
  ctx: ClientToolContext,
): Promise<ClientToolResult> {
  if (name === "navigate") {
    const route = String(input.route ?? "");
    if (!route) return { success: false, message: "navigate: 'route' is required" };
    ctx.navigate(route);
    return { success: true, message: `Navigated to ${route}` };
  }

  if (name === "refresh_mv") {
    const mvId = String(input.mv_id ?? "");
    if (!mvId) return { success: false, message: "refresh_mv: 'mv_id' is required" };
    const ok = await ctx.confirm({
      title: "Refresh materialized view?",
      message: `Refresh "${mvId}" now?`,
    });
    if (!ok) return { success: false, message: "Cancelled by the user." };
    return ctx.runMutation("refresh_mv", { mvId });
  }

  throw new Error(`unknown client tool ${JSON.stringify(name)}`);
}
