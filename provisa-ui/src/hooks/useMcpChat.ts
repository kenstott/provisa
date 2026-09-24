// Copyright (c) 2026 Kenneth Stott
// Canary: 7b4d2c81-3e9f-4a06-8c15-9d2b6e4f8a71
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useRef, useState } from "react";
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
export function useMcpChat(
  roleId: string,
  toolCtx: ClientToolContext,
  currentRoute?: string,
  // REQ-1820: server tools (create_source_now, register_table_now, ...) run entirely on the
  // backend — this is the ONLY signal the frontend gets that one succeeded, since the actual
  // mutation never went through Apollo. Callers use it to refetch whatever page is showing the
  // thing that just changed (e.g. the Sources admin page's own query), which nothing did before.
  onServerToolResult?: (name: string, isError: boolean) => void,
) {
  const [messages, setMessages] = useState<ChatMsg[]>([]);
  const [tools, setTools] = useState<ToolEvent[]>([]);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<ChatError | null>(null);
  // REQ-1815: the in-flight fetch a "Stop" click aborts.
  const abortRef = useRef<AbortController | null>(null);

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
    signal: AbortSignal,
  ): Promise<Record<string, unknown> | null> => {
    const resp = await fetch(`${API_BASE}/admin/mcp/chat`, {
      method: "POST",
      headers: { "Content-Type": "application/json", "x-provisa-role": roleId },
      body: JSON.stringify({ messages: convo, current_route: currentRoute }),
      signal,
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
        let ev;
        try {
          ev = JSON.parse(line.slice(5).trim());
        } catch (parseErr) {
          // A malformed line should never take down the whole turn's remaining events.
          console.error("Malformed SSE event, skipped:", parseErr, line);
          continue;
        }
        if (ev.type === "text") onText(ev.text);
        else if (ev.type === "tool_use")
          setTools((p) => [...p, { name: ev.name, input: ev.input, running: true }]);
        else if (ev.type === "tool_result") {
          markToolResolved(ev.name, ev.is_error);
          onServerToolResult?.(ev.name, ev.is_error);
        }
        else if (ev.type === "awaiting_client_tools") awaiting = ev;
        else if (ev.type === "error") setError({ message: ev.error, action: ev.action });
      }
    }
    return awaiting;
  };

  // REQ-1823: each round of the tool-call loop is its OWN assistant turn — whatever Polly says
  // after a tool round trip is a new response, not a continuation of what she said before it — so
  // every round gets a fresh message bubble instead of one bubble growing across the whole
  // multi-step exchange. Tracked by a stable object REFERENCE, not an index: a present_choice
  // question/answer recorded mid-turn (REQ-1818/1813) inserts its own messages via pushMessage
  // while a round is still streaming, which would silently invalidate any precomputed index: the
  // placeholder's actual position is found fresh on every update via indexOf, immune to whatever
  // else got inserted around it. If the placeholder is gone (e.g. the conversation was cleared
  // mid-turn), the update is simply dropped rather than resurrecting a stale message.
  const beginAssistantTurn = (): ((chunk: string) => void) => {
    const placeholder: ChatMsg = { role: "assistant", text: "" };
    setMessages((prev) => [...prev, placeholder]);
    let assistantText = "";
    return (chunk: string) => {
      assistantText += chunk;
      const current = assistantText;
      setMessages((prev) => {
        const idx = prev.indexOf(placeholder);
        if (idx === -1) return prev;
        // REQ-1842: mutate `placeholder` in place — the previous version wrote `next[idx] = {
        // role: "assistant", text: current }`, a NEW object, replacing the very reference this
        // closure was tracking. Every chunk after the first then searched for an object no
        // longer in the array (indexOf always -1) and silently dropped, so the message froze on
        // the FIRST chunk forever while the server kept streaming the whole real reply
        // underneath — confirmed live via console logging of every onText call. A new outer
        // array (still built below) is what tells React to re-render either way.
        placeholder.text = current;
        return [...prev];
      });
    };
  };

  const send = async (raw: string) => {
    const text = raw.trim();
    if (!text || busy) return;
    setError(null);
    setTools([]);
    setMessages((prev) => [...prev, { role: "user" as const, text }]);
    setBusy(true);
    const controller = new AbortController();
    abortRef.current = controller;

    let convo: RawTurn[] = [
      ...messages.map((m) => ({ role: m.role, content: m.text })),
      { role: "user", content: text },
    ];

    try {
      for (;;) {
        const appendAssistant = beginAssistantTurn();
        const awaiting = await streamOnce(convo, appendAssistant, controller.signal);
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
      // A user-initiated Stop (REQ-1815) is not a failure — don't surface an error banner for it.
      if (!(e instanceof DOMException && e.name === "AbortError")) {
        setError({ message: e instanceof Error ? e.message : String(e) });
      }
    } finally {
      abortRef.current = null;
      setBusy(false);
    }
  };

  return {
    messages,
    tools,
    busy,
    error,
    send,
    cancel: () => abortRef.current?.abort(),
    clear: () => {
      setMessages([]);
      setTools([]);
    },
    pushMessage: (msg: ChatMsg) => setMessages((prev) => [...prev, msg]),
  };
}
