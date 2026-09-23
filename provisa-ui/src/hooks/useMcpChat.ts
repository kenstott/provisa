// Copyright (c) 2026 Kenneth Stott
// Canary: 7b4d2c81-3e9f-4a06-8c15-9d2b6e4f8a71
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useState } from "react";
import { executeClientTool, type ClientToolContext } from "../mcp/clientTools";

const API_BASE = import.meta.env.VITE_API_BASE || "";

export interface ChatMsg {
  role: "user" | "assistant";
  text: string;
}

export interface ToolEvent {
  name: string;
  input?: unknown;
  running?: boolean;
  error?: boolean;
}

export interface ChatError {
  message: string;
  action?: { label: string; route: string };
}

// Anthropic-shaped raw content (SDK JSON block form, not the display-only ChatMsg above) — the
// history POSTed to /admin/mcp/chat, extended across a client-tool pause/resume (REQ-1795).
type RawTurn = { role: "user" | "assistant"; content: unknown };

/**
 * Drives the MCP chat SSE endpoint, including REQ-1795's client-tool pause/resume: when the
 * server yields `awaiting_client_tools`, this hook executes the pending tool(s) itself (via
 * `clientTools.ts`), appends the results, and re-POSTs to resume — all inside one `send()` call,
 * so callers just see the exchange complete once the model is done, whether or not it paused
 * along the way.
 */
export function useMcpChat(roleId: string, toolCtx: ClientToolContext, currentRoute?: string) {
  const [messages, setMessages] = useState<ChatMsg[]>([]);
  const [tools, setTools] = useState<ToolEvent[]>([]);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<ChatError | null>(null);

  const markToolResolved = (name: string, isError: boolean) => {
    setTools((prev) => {
      const next = [...prev];
      for (let i = next.length - 1; i >= 0; i--) {
        if (next[i].name === name && next[i].running) {
          next[i] = { ...next[i], running: false, error: isError };
          break;
        }
      }
      return next;
    });
  };

  /** POST one turn of `convo` and consume its SSE stream. Returns the pending
   *  `awaiting_client_tools` event if the server paused, or null if the turn truly finished. */
  const streamOnce = async (
    convo: RawTurn[],
    onText: (chunk: string) => void,
  ): Promise<Record<string, unknown> | null> => {
    const resp = await fetch(`${API_BASE}/admin/mcp/chat`, {
      method: "POST",
      headers: { "Content-Type": "application/json", "x-provisa-role": roleId },
      body: JSON.stringify({ messages: convo, current_route: currentRoute }),
    });
    if (!resp.ok || !resp.body) throw new Error(`chat failed: ${resp.status}`);

    const reader = resp.body.getReader();
    const decoder = new TextDecoder();
    let buffer = "";
    let awaiting: Record<string, unknown> | null = null;
    for (;;) {
      const { done, value } = await reader.read();
      if (done) break;
      buffer += decoder.decode(value, { stream: true });
      const blocks = buffer.split("\n\n");
      buffer = blocks.pop() ?? "";
      for (const block of blocks) {
        const line = block.trim();
        if (!line.startsWith("data:")) continue;
        const ev = JSON.parse(line.slice(5).trim());
        if (ev.type === "text") onText(ev.text);
        else if (ev.type === "tool_use")
          setTools((p) => [...p, { name: ev.name, input: ev.input, running: true }]);
        else if (ev.type === "tool_result") markToolResolved(ev.name, ev.is_error);
        else if (ev.type === "awaiting_client_tools") awaiting = ev;
        else if (ev.type === "error") setError({ message: ev.error, action: ev.action });
      }
    }
    return awaiting;
  };

  const send = async (raw: string) => {
    const text = raw.trim();
    if (!text || busy) return;
    setError(null);
    setTools([]);
    const displayHistory = [...messages, { role: "user" as const, text }];
    setMessages([...displayHistory, { role: "assistant" as const, text: "" }]);
    setBusy(true);

    let assistantText = "";
    const appendAssistant = (chunk: string) => {
      assistantText += chunk;
      const current = assistantText;
      setMessages((prev) => {
        const next = [...prev];
        next[next.length - 1] = { role: "assistant", text: current };
        return next;
      });
    };

    let convo: RawTurn[] = [
      ...messages.map((m) => ({ role: m.role, content: m.text })),
      { role: "user", content: text },
    ];

    try {
      for (;;) {
        const awaiting = await streamOnce(convo, appendAssistant);
        if (!awaiting) break; // the model finished for real — no pending client tools

        const pending = awaiting.pending as { id: string; name: string; input: unknown }[];
        const clientResults = [];
        for (const p of pending) {
          let result;
          try {
            result = await executeClientTool(
              p.name,
              (p.input as Record<string, unknown>) ?? {},
              toolCtx,
            );
          } catch (e) {
            result = { success: false, message: e instanceof Error ? e.message : String(e) };
          }
          markToolResolved(p.name, !result.success);
          clientResults.push({
            type: "tool_result",
            tool_use_id: p.id,
            content: JSON.stringify(result),
            is_error: !result.success,
          });
        }

        convo = [
          ...convo,
          { role: "assistant", content: awaiting.assistant_content },
          {
            role: "user",
            content: [...(awaiting.server_tool_results as unknown[]), ...clientResults],
          },
        ];
        // Loop continues: re-POSTs `convo` to resume the model's turn.
      }
    } catch (e) {
      setError({ message: e instanceof Error ? e.message : String(e) });
    } finally {
      setBusy(false);
    }
  };

  return { messages, tools, busy, error, send, clear: () => setMessages([]) };
}
